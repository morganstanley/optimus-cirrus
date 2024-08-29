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
package optimus.platform.temporalSurface.impl

import java.io.FileWriter
import java.io.PrintWriter
import java.io.StringWriter
import java.util.concurrent.atomic.AtomicBoolean
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.BreadcrumbsSendLimit.OnceByCrumbEquality
import optimus.breadcrumbs.crumbs.Events
import optimus.breadcrumbs.crumbs.Properties
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.core.ChainedNodeID
import optimus.core.CoreAPI
import optimus.graph.NodeTask
import optimus.graph.Settings
import optimus.graph.diagnostics.gridprofiler.GridProfiler
import optimus.platform.dal.TemporalContextAPI
import optimus.platform.internal.KnownClassInfo
import optimus.platform.internal.UnknownClassInfo
import optimus.platform.util.PrettyStringBuilder
import optimus.platform._
import optimus.platform.storable.PersistentEntity
import optimus.platform.temporalSurface.TemporalSurface
import optimus.platform.temporalSurface.operations.DataQueryByEntityReference
import optimus.platform.temporalSurface.operations.EntityClassBasedQuery
import optimus.platform.temporalSurface.operations.TemporalSurfaceQuery
import optimus.utils.SeqExplore
import org.slf4j.LoggerFactory

import java.io.Writer
import scala.compat.Platform._
import scala.util.matching.Regex

final case class EntityTraceMsg(
    vt: Option[String],
    tt: Option[String],
    tag: Option[String],
    cmd: String,
    baseEntity: String,
    nodeCaller: String,
    ref2Entity: Map[String, String],
    isFinal: Boolean) {
  override def toString: String = {
    val strFinal = if (isFinal) "Y" else "N"
    val strResult = ref2Entity.map { case (k, v) => s"$k->$v" }.mkString(",")
    s"vt:${vt.getOrElse("")}--tt:${tt.getOrElse("")}--tag:${tag.getOrElse(
        "")}--cmd:$cmd--baseEntity:$baseEntity--node:$nodeCaller--result:$strResult--$strFinal"
  }
}

// TODO (OPTIMUS-56981): code refactoring for tracing feature
object TemporalContextTrace {
  def traceQuery(
      callingScope: List[TemporalSurface],
      operation: TemporalSurfaceQuery,
      temporalContextChain: List[TemporalContext],
      tag: String): Unit = {
    traceReporter.traceQuery(callingScope, operation, temporalContextChain, tag)
  }

  private val log = LoggerFactory.getLogger(TemporalContextTrace.getClass)

  final val TraceFailedLegacyTurnoffKey = "traceFailedLegacyTurnoff"
  final val TraceEntityKey = "traceEntity"
  final val TraceDalAccessKey = "traceDALAccess"
  final val TraceTweakedVtTtKey = "traceTweakedVtTt"
  final val TraceTweakedLoadContext = "traceTweakedLoadContext"
  final val TraceCreatedTemporalContext = "createTemporalContext"
  final val CauseNotFound = "<unknown cause>"

  def traceCreated(tc: TemporalContextImpl): Unit = {
    val tag = EvaluationContext.scenarioStack.findPluginTag(TemporalContextPermittedListTag)
    if (tag.isEmpty) {
      // not permitted
      traceReporter.traceCreated(tc)
    }
  }
  def traceTweakedVtTt(vt: Tweak, tt: Tweak): Unit = {
    val tag = EvaluationContext.scenarioStack.findPluginTag(TemporalContextPermittedListTag)
    if (tag.isEmpty) {
      // not permitted
      traceReporter.traceTweakedVtTt(vt, tt)
    }
  }
  def traceTweakedLoadContext(lc: Tweak): Unit = {
    val tag = EvaluationContext.scenarioStack.findPluginTag(TemporalContextPermittedListTag)
    if (tag.isEmpty) {
      // not permitted
      traceReporter.traceTweakedLoadContext(lc)
    }
  }
  def traceMigrationGivenTweakedVtTt(vt: Tweak, tt: Tweak, tag: Option[String]): Unit = {
    val tag = EvaluationContext.scenarioStack.findPluginTag(TemporalContextPermittedListTag)
    if (tag.isEmpty) {
      // not permitted
      traceReporter.traceMigrationGivenTweakedVtTt(vt, tt, tag)
    }
  }
  def traceReadVt(): Unit = {
    val tag = EvaluationContext.scenarioStack.findPluginTag(TemporalContextPermittedListTag)
    if (tag.isEmpty) {
      // not permitted
      traceReporter.traceReadVt()
    }
  }
  def traceReadTt(): Unit = {
    val tag = EvaluationContext.scenarioStack.findPluginTag(TemporalContextPermittedListTag)
    if (tag.isEmpty) {
      // not permitted
      traceReporter.traceReadTt()
    }
  }
  def traceDalAccess(causeScope: QueryRecorderTag, node: NodeTask): Unit = {
    traceReporter.traceDalAccess(causeScope, node)
  }
  def traceEntity(rootQuery: TemporalSurfaceQuery, node: NodeTask, temporalSurface: TemporalSurface): Unit = {
    traceReporter.traceEntity(rootQuery, node, temporalSurface)
  }

  // this is to collect those cases that we cannot turn off legacy tweaks
  def traceFailedLegacyTurnoff(node: NodeTask): Unit = {
    traceReporter.traceFailedLegacyTurnoff(node)
  }

  private def defaultTraceReporter = {
    if (Settings.profileTemporalContext) {
      // TemporalContextWatchtowerPublisher is sync publish, which will be used when shutdown,
      // to ensure all the information collected by the GridProfiler is published
      log.info(s"creating profiler for tracing temporal context")
      GridProfilerTraceReporter
    } else {
      val filename = System.getProperty("optimus.platform.temporalSurface.TemporalContextTrace-file")
      if (filename == null) {
        if (Settings.enableTraceSurface) {
          log.info(s"creating TraceSurfaceReporter")
          TraceSurfaceReporter
        } else {
          log.info(s"creating StdErrTraceReporter")
          StdErrTraceReporter
        }
      } else {
        log.info(s"creating FileTraceReporter")
        new FileTraceReporter(filename)
      }
    }
  }

  // this is a flexibility point to allow users to customise the output
  var traceReporter: TraceReporter = defaultTraceReporter

  // called when ending a DistTask in a GSF engine, if the node arrived with a grid profiling tag
  private[optimus] def restoreTraceReporter(): Unit = {
    traceReporter.dataCleanup()
    traceReporter = defaultTraceReporter
  }
}

object EntityTraceMsg {
  val pattern: Regex = "vt:(.*?)--tt:(.*?)--tag:(.*?)--cmd:(.*?)--baseEntity:(.*?)--node:(.*?)--result:(.*?)--(.*?)".r
  def buildEntityTraceMsgFromString(traceString: String): EntityTraceMsg = {
    val pattern(matchVT, matchTT, matchTag, matchCmd, matchBaseEntity, matchCaller, matchResult, matchFinal) =
      traceString
    val isFinal = matchFinal.equals("Y")
    val ref2Entity =
      if (matchResult.isEmpty) Map.empty[String, String]
      else
        matchResult
          .split(",")
          .map { item =>
            val kv = item.split("->")
            if (kv.size <= 2) {
              val v = if (kv.size <= 1) "" else kv.last
              val k = if (kv.isEmpty) "" else kv.head
              k -> v
            } else throw new IllegalArgumentException(s"unexpected result $item in trace message $traceString")
          }
          .toMap
    val tag = if (matchTag.isEmpty) None else Some(matchTag)
    val vt = if (matchVT.isEmpty) None else Some(matchVT)
    val tt = if (matchTT.isEmpty) None else Some(matchTT)
    EntityTraceMsg(vt, tt, tag, matchCmd, matchBaseEntity, matchCaller, ref2Entity, isFinal)
  }
}

trait TraceReporter {
  def traceQuery(
      callingScope: List[TemporalSurface],
      operation: TemporalSurfaceQuery,
      temporalContextChain: List[TemporalContext],
      tag: String): Unit = {
    showText("Trace query", s"Traced query at '$tag' $operation")
  }

  def traceCreated(tc: TemporalContextImpl): Unit =
    showText(TemporalContextTrace.TraceCreatedTemporalContext, findCreateTCCause)

  protected def getTweakedVtTtMsg(vt: Tweak, tt: Tweak): String = {
    (vt ne null, tt ne null) match {
      case (true, true)  => s"validTime and transactionTime tweaks(${vt.prettyString}, ${tt.prettyString}) detected"
      case (true, false) => s"validTime tweak(${vt.prettyString}) detected"
      case (false, true) => s"transactionTime tweak(${tt.prettyString}) detected"
      case _             => ???
    }
  }

  def traceMigrationGivenTweakedVtTt(vt: Tweak, tt: Tweak, tag: Option[String]): Unit = {
    val msg = getTweakedVtTtMsg(vt, tt) + s" with tag: $tag"

    // for temporal context migration, the message is good enough, don't need the stacktrace
    showText("traceMigrationGiven", s"$msg $findTweakCause")
  }

  def traceTweakedVtTt(vt: Tweak, tt: Tweak): Unit = {
    val msg = getTweakedVtTtMsg(vt, tt)

    // for temporal context migration, the message is good enough, don't need the stacktrace
    showText(TemporalContextTrace.TraceTweakedVtTtKey, findTweakCause)
  }

  def traceTweakedLoadContext(lc: Tweak): Unit = {
    showText(TemporalContextTrace.TraceTweakedLoadContext, findTweakCause)
  }

  def traceReadVt(): Unit = {
    showText("accessVt", getNodeStack("validTime"))
  }
  def traceReadTt(): Unit = {
    showText("accessTt", getNodeStack("transactionTime"))
  }
  def traceDalAccess(causeScope: QueryRecorderTag, node: NodeTask): Unit = {
    val command = node.args()(1)
    val surface = causeScope.temporalSurface
    val time = surface match {
      case ts: LeafTemporalSurfaceImpl =>
        ts.currentTemporality
      case _ => None
    }
    val tag = surface match {
      case ts: TemporalSurfaceImpl if ts.tag != null => "tag:" + ts.tag
      case _                                         => ""
    }
    showText(TemporalContextTrace.TraceDalAccessKey, s"time:$time--$tag--$command")
  }
  def traceEntity(rootQuery: TemporalSurfaceQuery, node: NodeTask, temporalSurface: TemporalSurface): Unit = {

    val query = s"$rootQuery"
    val baseEntity = rootQuery match {
      case classQuery: EntityClassBasedQuery[_] =>
        classQuery.targetClass.getName
      case refQuery: DataQueryByEntityReference[_] =>
        refQuery.targetClass.getName
      case _ => throw new IllegalArgumentException(s"unexpected query")
    }

    // parse to get the time and tag info
    val (vt, tt, tag) = temporalSurface match {
      case ts: LeafTemporalSurfaceImpl =>
        (
          Some(ts.currentTemporality.validTime.toString),
          Some(ts.currentTemporality.txTime.toString),
          temporalSurface.tag)
      case _ => (None, None, temporalSurface.tag)
    }

    // parse to get the entity and eref info
    val parseResult =
      if (node.isDoneWithException) None
      else
        node.executionInfo() match {
          case pluginHackDataAccess.getSingleItemDataInSurface_info =>
            val isFinalResult = true
            val eref = node.args()(3) // index 3 is the eref that used to get the single item data
            node.resultObject() match {
              case persistEntity: PersistentEntity =>
                val entity = persistEntity.className
                Some(Map(eref -> entity) -> isFinalResult)
              case null =>
                Some(
                  Map(eref -> null) -> isFinalResult
                ) // TODO (OPTIMUS-13108): We shouldn't pass
              case _ =>
                throw new IllegalArgumentException(
                  s"result of $rootQuery must be a persistEntity when calling pluginHackDataAccess.getSingleItemDataInSurface")
            }
          case pluginHackDataAccess.getItemDataInSurface_info =>
            val isFinalResult = true
            node.resultObject() match {
              case entityMap: Map[_, _] =>
                // if the result is empty, then we need to manually check the sending query to get the entity info
                if (entityMap.isEmpty) {
                  rootQuery match {
                    case classQuery: EntityClassBasedQuery[_] =>
                      val entity = classQuery.targetClass.getCanonicalName
                      Some((Map("" -> entity), isFinalResult)) // return empty ref when the result is empty
                    case refQuery: DataQueryByEntityReference[
                          _
                        ] => // normally, it should not hit this because getItemDataInSurface should not accept ref query
                      throw new IllegalArgumentException(
                        s"getItemDataInSurface should not accept ref query: $rootQuery")
                    case _ =>
                      throw new IllegalArgumentException(
                        s"root query $rootQuery should be either EntityClassBasedQuery or DataQueryByEntityReference")
                  }
                } else {
                  val result = entityMap.map { case (ref, e) =>
                    e match {
                      case persistEntity: PersistentEntity =>
                        val entity = persistEntity.className
                        ref -> entity
                      case null =>
                        ref -> null // TODO (OPTIMUS-13108): We shouldn't pass
                      case _ =>
                        throw new IllegalArgumentException(s"result $e of $rootQuery must be a persistEntity")
                    }
                  }
                  Some(result -> isFinalResult)
                }
            }
          case pluginHackDataAccess.getClassInfoInSurface_info =>
            val isFinalResult = false
            val eref = node.args()(3) // index 3 is the eref
            // if the reason is needed for logging
            // val reason = node.args()(4) // index 4 is the reason
            node.resultObject() match {
              case classInfo: KnownClassInfo[_] => Some(Map(eref -> classInfo.clazz.getName) -> isFinalResult)
              case classInfo: UnknownClassInfo  => Some(Map(eref -> classInfo.description) -> isFinalResult)
              case _ =>
                throw new IllegalArgumentException(
                  s"empty result of $rootQuery while calling pluginHackDataAccess.getClassInfoInSurface")
            }

          case pluginHackDataAccess.getItemKeysInSurface_info =>
            val isFinalResult = false
            // we don't collect these data now because it will be large
            Some(Map.empty -> isFinalResult)

          case pluginHack.start_info => None // ignore all other DAL calls, including getClassInfo or getItemKey
          case _ =>
            throw new IllegalArgumentException(
              s"invalid traced node $node. The flag optimus.temporalSurface.traceEntity is only used for specific nodes now")
        }
    parseResult match {
      case Some(result) =>
        val ref2Entity = result._1
        val isFinal = result._2

        val strRef2Entity = ref2Entity.map { pair: (Any, Any) =>
          s"${pair._1}" -> s"${pair._2}"
        }
        val entityTraceResult =
          EntityTraceMsg(vt, tt, tag, query, baseEntity, node.executionInfo().name(), strRef2Entity, isFinal)
        showText(TemporalContextTrace.TraceEntityKey, entityTraceResult.toString)
      case _ => // ignore all none result
    }

  }

  def traceFailedLegacyTurnoff(node: NodeTask): Unit = {
    val psb = new PrettyStringBuilder
    node.waitersToFullMultilineNodeStack(false, psb)
    showText(TemporalContextTrace.TraceFailedLegacyTurnoffKey, psb.toString.replace("\n", "###"))
  }

  protected def currentTemporality(causeScope: QueryRecorderTag) = causeScope.temporalSurface match {
    case ts: LeafTemporalSurfaceImpl =>
      ts.currentTemporality
    case _ => None
  }

  protected def temporalSurfaceTag(causeScope: QueryRecorderTag): String = causeScope.temporalSurface match {
    case ts: TemporalSurfaceImpl if ts.tag != null => "tag:" + ts.tag
    case _                                         => ""
  }

  protected def getNodeStack(msg: String): String = {
    import optimus.platform.util.PrettyStringBuilder

    val n = EvaluationContext.currentNode

    val psb = new PrettyStringBuilder
    psb.append(s"Node Stack for access to $msg\n")
    n.waitersToFullMultilineNodeStack(true, psb)
    psb.toString
  }

  protected def findCreateTCCause: String = {
    val trace = Thread.currentThread.getStackTrace
    findCreateTCCall(trace).getOrElse(findDefaultCause)
  }

  // find the original place where temporal context is created by looking for "TemporalContext" constructor
  // of trait "optimus.platform.dal.TemporalContextAPI" in the stack trace
  protected def findCreateTCCall(trace: Array[StackTraceElement]): Option[String] = {
    val se = SeqExplore(trace.toSeq)
    se.past { ste =>
      (ste.getClassName.startsWith(classOf[TemporalContextAPI].getName) || ste.getClassName.startsWith(
        "optimus.platform.package")) &&
      ste.getMethodName.contains("TemporalContext")
    }.get
      .map(_.toString)
      .orElse {
        findCreateBranchOrLeafTCCall(trace)
      }
      .orElse {
        findCreateFlatTCCall(trace)
      }
  }

  // find the original place where flat temporal context is created by looking for "apply" method of
  // object "FlatTemporalContext" in the stack trace
  protected def findCreateFlatTCCall(trace: Array[StackTraceElement]): Option[String] = {
    val CLASSNAME = classOf[FlatTemporalContext].getName
    val METHODNAME = "apply"
    trace.toSeq.lastIndexWhere { ste =>
      ste.getClassName.startsWith(CLASSNAME) && ste.getMethodName.contains(METHODNAME)
    } match {
      case -1 => None
      case found =>
        if (found + 1 < trace.length)
          Some(s"${trace(found + 1)}")
        else
          None
    }
  }

  // find the original place where branch temporal context is created by looking for "apply" method of
  // object "FixedBranchContext" and "FixedLeafContext" in the stack trace
  private def findCreateBranchOrLeafTCCall(trace: Array[StackTraceElement]): Option[String] = {
    // TODO (OPTIMUS-18562): Fix className in TemporalContextTrace when enabling scala 2.12
    val BRANCHTCCLASSNAME = "optimus.platform.temporalSurface.TemporalSurfaceDefinition$FixedBranchContext$"
    val LEAFTCCLASSNAME = "optimus.platform.temporalSurface.TemporalSurfaceDefinition$FixedLeafContext$"
    val METHODNAME = "apply"
    SeqExplore(trace.toSeq)
      .until { ste =>
        (ste.getClassName == BRANCHTCCLASSNAME || ste.getClassName == LEAFTCCLASSNAME) && ste.getMethodName.contains(
          METHODNAME)
      }
      .next
      .get
      .map(_.toString)
  }

  // Since we cannot handle the case where cause is undetermined, and Tracing features do have some edge cases where
  // cannot determine the cause place, we won't report this issue any more.
  // And in future, we will use OPTIMUS-20656 to mark all permitted cases, and the violations will be reported directly
  // at Compile time.
  // TODO (OPTIMUS-20656): add new syntax block for permitting temporal context creation
  protected def findDefaultCause: String = TemporalContextTrace.CauseNotFound

  protected def findTweakCause: String = {
    val trace = Thread.currentThread.getStackTrace
    findGivenCall(trace).getOrElse(findDefaultCause)
  }

  // find the original place where vt/tt is tweaked by looking for "given" method of
  // trait "optimus.core.CoreAPI$class" in the stack trace
  protected def findGivenCall(trace: Array[StackTraceElement]): Option[String] = {
    val CLASSNAMES = Seq(classOf[CoreAPI].getName, AdvancedUtils.getClass.getName, "optimus.platform.package")
    SeqExplore(trace.toSeq)
      .past { ste =>
        val name = CLASSNAMES.exists(n => ste.getClassName.startsWith(n))
        val method = ste.getMethodName.contains("given") || ste.getMethodName.contains("migrationGiven")
        name && method
      }
      .get
      .map(_.toString)
  }

  protected def showStack(cause: String, msg: String): Unit
  protected def showText(cause: String, msg: String): Unit

  def dataCleanup(): Unit = {}

}

object StdErrTraceReporter extends TraceReporter {
  def showStack(cause: String, msg: String): Unit = new Exception(msg).printStackTrace()
  def showText(cause: String, msg: String): Unit = System.err.println(s"cause: $cause, msg: $msg")
}

class FileTraceReporter(name: String) extends TraceReporter with Runnable {

  val fos = new FileWriter(name)

  def showStack(cause: String, msg: String): Unit = {
    val sw = new StringWriter
    new Exception(msg).printStackTrace(new PrintWriter(sw))
    sw.append("\n")
    write(sw.toString)
  }
  def showText(cause: String, msg: String): Unit = write(s"$cause ---> $msg\n")

  protected def write(txt: CharSequence): Writer = synchronized(fos.append(txt))

  def run(): Unit = {
    try {
      synchronized(fos.flush())
    } catch {
      case t: Throwable =>
        t.printStackTrace()
    }
  }
}

object GridProfilerTraceReporter extends TraceReporter {
  override def showStack(cause: String, msg: String): Unit =
    if (Settings.profileTemporalContext)
      GridProfiler.recordVTTTStack(Map(new Exception(cause).getStackTrace().mkString("", EOL, EOL) -> Set(msg)))

  override def showText(cause: String, msg: String): Unit = {
    if (Settings.profileTemporalContext && (msg != TemporalContextTrace.CauseNotFound))
      GridProfiler.recordVTTTStack(Map(cause -> Set(msg)))
  }
}

/**
 * A special class used to report tracing default temporal surface. It only reports once to reduce log size, but
 * (whether it has reported or not) can be reset by distribution
 */
object TraceSurfaceReporter extends TraceReporter {
  private val hasReported = new AtomicBoolean(false)
  private val log = LoggerFactory.getLogger(TraceSurfaceReporter.getClass)
  val classToIgnore = ".app.common.FakedDefaultEntity"

  override protected def showStack(cause: String, msg: String): Unit = {}

  override protected def showText(cause: String, msg: String): Unit = {}

  override def traceQuery(
      callingScope: List[TemporalSurface],
      operation: TemporalSurfaceQuery,
      temporalContextChain: List[TemporalContext],
      tag: String): Unit = {
    logOnce(operation, tag)
    if (tag == "CrossAssetPositionSetMilestoneTrace") {
      val className = operation match {
        case classBasedQuery: EntityClassBasedQuery[_]                 => classBasedQuery.targetClass.getName
        case dataQueryByEntityReference: DataQueryByEntityReference[_] => dataQueryByEntityReference.targetClass.getName
        case _                                                         => operation.toString
      }
      if (!className.contains(classToIgnore)) {
        Breadcrumbs.info(
          OnceByCrumbEquality,
          ChainedNodeID.nodeID,
          PropertiesCrumb(
            _,
            Properties.event -> Events.TemporalSurfaceTrace.name,
            Properties.tsQueryClassName -> className,
            Properties.tsTag -> tag)
        )
      }
    }
  }

  private def logOnce(operation: TemporalSurfaceQuery, tag: String): Unit = {
    // TODO (OPTIMUS-18562): Fix className in TemporalContextTrace when enabling scala 2.12
    if (!operation.toString.contains(classToIgnore) && !hasReported.getAndSet(true)) {
      log.warn(s"Trace query at '$tag': $operation")
    }
  }

  override def dataCleanup(): Unit = {
    hasReported.set(false)
  }

}
