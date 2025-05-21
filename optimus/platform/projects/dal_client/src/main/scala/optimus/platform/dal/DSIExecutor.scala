/*
 * Morgan Stanley makes this available to you under the Apache License, Version 2.0 (the "License").
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package optimus.platform.dal

import msjava.slf4jutils.scalalog.getLogger
import optimus.breadcrumbs.crumbs.RequestsStallInfo
import optimus.breadcrumbs.crumbs.StallPlugin
import optimus.core.CoreAPI.asyncResult
import optimus.graph._
import optimus.graph.diagnostics.gridprofiler.GridProfiler
import optimus.platform._
import optimus.platform.annotations.nodeSync
import optimus.platform.dal.pubsub.HandlePubSub
import optimus.platform.dal.pubsub.PubSubClientRequest
import optimus.platform.dal.pubsub.PubSubClientResponse
import optimus.platform.dsi.bitemporal.CmdLogLevelHelper
import optimus.platform.dsi.bitemporal.DSI
import optimus.platform.dsi.bitemporal.ExpressionQueryCommand
import optimus.platform.dsi.bitemporal.QueryPlan
import optimus.platform.dsi.bitemporal.QueryResult
import optimus.platform.dsi.bitemporal.ReadOnlyCommand
import optimus.platform.dsi.bitemporal.Result
import optimus.platform.dsi.bitemporal.SelResult
import optimus.platform.dsi.bitemporal.LeadWriterCommand
import optimus.platform.util.PrettyStringBuilder

import scala.collection.mutable

// TODO (OPTIMUS-13031): Needs changing to an @transient @entity, but @async not supported on entities
trait DSIExecutor {
  @async def executeLeadWriterCommands(dsi: DSI, cmd: Seq[LeadWriterCommand]): Seq[Result]
  @async def executeQuery(dsi: DSI, cmd: ReadOnlyCommand): Result
  def executePubSubRequest(dsi: DSI, request: PubSubClientRequest): PubSubClientResponse
}

@entity object DevNull {

  @node(exposeArgTypes = true) @scenarioIndependent def sendToDevNull: Result =
    throw new UnsupportedOperationException("plugin expected here")

  val stuffInDevNull: mutable.ArrayBuffer[(sendToDevNull$node, Scheduler)] = mutable.ArrayBuffer()

  val devNullPlugin: SchedulerPlugin = new SchedulerPlugin {
    override def adapt(v: NodeTask, ec: OGSchedulerContext): Boolean = {
      v match {
        case node: sendToDevNull$node =>
          stuffInDevNull += (
            (
              node,
              ec.scheduler
            )
          ) // for verification purposes only, consider these nodes forgotten otherwise
          true
        case _ =>
          log.warn(s"Received unsupported task type $v")
          false
      }
    }
  }

  sendToDevNull_info.setCacheable(false)
  sendToDevNull_info.setPlugin(devNullPlugin)
}

//@entity
object DALDSIExecutor extends DSIExecutor {
  private val log = getLogger(this)

  def executePubSubRequest(dsi: DSI, request: PubSubClientRequest): PubSubClientResponse = {
    dsi match {
      case pubsubDsi: HandlePubSub =>
        pubsubDsi.executePubSubRequest(request)
      case otherDsi =>
        throw new IllegalStateException(s"Pubsub client request should not have arrived this dsi! $otherDsi")
    }
  }

  private def markNodeAccessingDAL(): Unit = {
    val currNode: NodeTask = EvaluationContext.currentNode
    /* we need to mark access DAL even if we don't have the plugin since if this runs outside a withoutDAL block and we
    have a cache hit on this inside of the withoutDAL block we still want to crash
     */
    currNode.markAccessedDAL()
    currNode.scenarioStack
      .findPluginTag(AdvancedUtils.WithoutDALViolationCollector)
      .foreach(_.addLocation(EvaluationContext.currentNode.enqueuerChain()))
  }
  @async def executeQuery(dsi: DSI, cmd: ReadOnlyCommand): Result = {
    markNodeAccessingDAL()
    val res = asyncResult(EvaluationContext.cancelScope) { doExecuteQuery(dsi, cmd) }

    if (res.hasException) {
      val e = res.exception
      log.error(s"DAL read caught exception: ", e)
      dsi.close(false) // false; only close the dal connection and not the full dsi

      if (DALEventMulticaster.getListeners.isEmpty) {
        throw e // Let client handle it..
      } else {
        DALEventMulticaster.notifyListeners(_.fatalDALErrorCallback(DALFatalErrorEvent(e)))
        DevNull.sendToDevNull
      }
    } else res.value
  }

  @async override def executeLeadWriterCommands(dsi: DSI, cmds: Seq[LeadWriterCommand]): Seq[Result] = {
    markNodeAccessingDAL()
    dsi match {
      case clientDsi: ClientSideDSI => clientDsi.executeLeadWriterCommands(cmds)
      case serverDsi                => serverDsi.executeLeadWriterCommands(cmds)
    }
  }

  @async private def doExecuteQuery(dsi: DSI, cmd: ReadOnlyCommand): Result = {
    val results = dsi match {
      case clientSideDsi: ClientSideDSI =>
        // ClientSideDSI's method is @async but DSI's isn't, downcasting to retain async stack
        cmd match {
          case ExpressionQueryCommand(_, QueryPlan.Accelerated, _) => clientSideDsi.executeAccCommands(List(cmd))
          case _                                                   => clientSideDsi.executeReadOnlyCommands(List(cmd))
        }

      case serverDsi =>
        /*
         * If it's not a client then it must be InMemory or a ServerDSI wrapped into a DSI for the localDALResolver on the broker.
         * We use the node batching plugin here to ensure that the local DAL resolver can batch queries properly.
         */
        val result = executeServerDsiQuery(cmd, serverDsi)
        Seq(result)
    }

    require(
      results.length == 1,
      s"expected one result for one command, but got ${results.length}, the command was: $cmd")
    if (GridProfiler.DALProfilingOn) GridProfiler.recordDALResults(results.head match {
      case m: SelResult   => m.value.size
      case q: QueryResult => q.value.size
      case _              => 0
    })
    results.head
  }

  private final case class ReadOnlyCommandNode(cmd: ReadOnlyCommand, dsi: DSI) extends CompletableNode[Result] {
    override def executionInfo: NodeTaskInfo = DALDSIExecutor.executionInfo

    override def writePrettyString(sb: PrettyStringBuilder): PrettyStringBuilder = {
      sb ++= super.writePrettyString(sb) ++= s" cmd: ${cmd.logDisplay(CmdLogLevelHelper.InfoLevel)}"
    }
  }

  private[this] def runNode[T](n: Node[T]): Node[T] = {
    EvaluationContext.ensureStarted(n)
  }

  @nodeSync private def executeServerDsiQuery(cmd: ReadOnlyCommand, dsi: DSI): Result =
    executeServerDsiQuery$queued(cmd, dsi).get
  private def executeServerDsiQuery$queued(cmd: ReadOnlyCommand, dsi: DSI) = runNode(ReadOnlyCommandNode(cmd, dsi))

  private object ReadOnlyCommandBatcher extends GroupingNodeBatcherSchedulerPlugin[Result, ReadOnlyCommandNode] {
    self =>
    maxBatchSize = Integer.getInteger("optimus.platform.dal.serverDsiReadCommandBatchSize", 20000)

    case object PerScenarioInfoT
    /*
     * Grouping by DSI here as this code is only executed with DSIs created by wrapping a ServerDSI, i.e. PartitionAwareServerDsi#WrappedServerDSI,
     * or by test DSI implementations where this should also be safe.
     */
    override type GroupKeyT = DSI
    override val pluginType: PluginType = PluginType.DAL
    override val stallReason = "waiting for DAL read-only command batcher"
    override def stallRequests(nodeTaskInfo: NodeTaskInfo): Option[RequestsStallInfo] = {
      val requests = FlightTracker.nodesInFlight().map(_.toString)
      Some(RequestsStallInfo(StallPlugin.DAL, requests.length, requests))
    }

    private object FlightTracker {
      private var counter = 0L
      private val nodes = mutable.Map.empty[Long, Seq[ReadOnlyCommandNode]]
      private val lock = new Object

      def add(nodes: Seq[ReadOnlyCommandNode]): Long = lock.synchronized {
        counter = counter + 1L
        this.nodes += counter -> nodes
        counter
      }

      def remove(id: Long): Unit = lock.synchronized {
        nodes -= id
      }

      def nodesInFlight(): Seq[Seq[ReadOnlyCommandNode]] = lock.synchronized {
        nodes.values.toSeq
      }

      def clear(): Unit = lock.synchronized {
        nodes.clear()
      }
    }

    @node override def run(dsi: GroupKeyT, nodes: Seq[ReadOnlyCommandNode]): Seq[Result] = {
      val cmds = nodes.map(_.cmd)
      val id = FlightTracker.add(nodes)
      try {
        val res = dsi.executeReadOnlyCommands(cmds)
        require(
          res.size == cmds.size,
          s"expected as many results as commands, but got ${res.size} results for ${cmds.size} commands")
        res
      } finally {
        FlightTracker.remove(id)
      }
    }

    override protected def prioritiseConcurrency: Boolean = false

    /*
     * Disabling async assertions because it would fail within the InMemory implementation which it's not a valid client.
     * Also, clients use InMemorySuite in combination with assertAsync to test the correctness of their own client code
     * so we don't have to be part of that anyway
     * NB: this code path is also executed when a serverDsi is wrapped as a normal DSI for the local resolver running on
     * the broker. We use the node batching plugin here to ensure that the local DAL resolver can batch queries properly.
     */
    override protected def scenarioStackForRun(inputScenarioStack: ScenarioStack): ScenarioStack =
      inputScenarioStack.ssShared.siStackWithIgnoreSyncStack

    override def getGroupBy(psinfo: PerScenarioInfoT, v: ReadOnlyCommandNode): DSI = v.dsi
  }

  private val executionInfo: NodeTaskInfo =
    NodeBatcherSchedulerPluginBase.getExecutionInfo("ReadOnlyCommandBatcher", ReadOnlyCommandBatcher)

  /*
   * These used to be async and not nodes, but we can't have @async on @entity; to preserve the behaviour, turning caching off
   */
//  executeQuery_info.setCacheable(false)
//  doExecuteQuery_info.setCacheable(false)

}
