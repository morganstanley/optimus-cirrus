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

import msjava.slf4jutils.scalalog.getLogger
import optimus.graph.NodeAwaiter
import optimus.graph.NodeTask
import optimus.graph.OGSchedulerContext
import optimus.graph.PluginType
import optimus.graph.SchedulerPlugin
import optimus.graph.Settings
import optimus.graph.SimplePluginNodeTraceInfo
import optimus.platform.EvaluationQueue
import optimus.platform.NonForwardingPluginTagKey
import optimus.platform.temporalSurface.TemporalSurface
import optimus.platform.temporalSurface.operations.CommandRecorder
import optimus.platform.temporalSurface.operations.TemporalSurfaceQuery
import optimus.platform.util.PrettyStringBuilder

import scala.jdk.CollectionConverters._
import scala.collection.mutable

abstract class TemporalSurfaceTracePlugin extends SchedulerPlugin {
  def tag(n: NodeTask): Option[QueryRecorderTag] =
    n.scenarioStack().findPluginTag(HasRecorderTag).flatMap(_.recorderTag)

  def parseArgsFailure(n: NodeTask, ec: EvaluationQueue) = {
    n.completeWithException(
      new IllegalStateException(
        "cannot handle a node - expected args of a TemporalSurfaceQuery, but found " + n.args().toList),
      ec)
    true
  }
}

/**
 * handles a TemporalContext.dataAccess call provides trace, causality, and stats by shared plugin tag on the scenario
 * stack
 */
object TemporalSurfaceDataAccessTracePlugin extends TemporalSurfaceTracePlugin {
  override val pluginType = PluginType("TSTrace")
  def adapt(n: NodeTask, ec: OGSchedulerContext): Boolean = {
    val args = n.args()
    if (args.length >= 2)
      (args(0), args(1)) match {
        case (tc: TemporalContextImpl, query: TemporalSurfaceQuery) =>
          val tag =
            if (Settings.traceEntityForTCMigration) new QueryPluginTagForTCMigration(query, tc, n)
            else new QueryPluginTag(query, tc, n)
          n.replace(n.scenarioStack.withPluginTag(HasRecorderTag, new QueryAndTraceTag(n, query.toString, tag)))
          n.continueWith(tag, ec)
          false
        case _ => parseArgsFailure(n, ec)
      }
    else parseArgsFailure(n, ec)
  }
}

/**
 * handles a TemporalSurfaceImpl queryTemporalSurface call, or CachedDataAccess provides trace, causality, and stats by
 * shared plugin tag on the scenario stack, within the scope of a tag of QueryRecorderTag, provided by a
 * TemporalSurfaceDataAccessTracePlugin or a TemporalSurface_SurfaceSpecificTracePlugin
 */
class TemporalSurface_SurfaceSpecificTracePlugin(argIndex: Int) extends TemporalSurfaceTracePlugin {
  def adapt(n: NodeTask, ec: OGSchedulerContext): Boolean = {
    val args = n.args()
    if (args.length > argIndex)
      args(argIndex) match {
        case ts: TemporalSurface =>
          val tagOption = tag(n)
          tagOption foreach { parent =>
            val newTag = parent.root.getAnyChild(ts)
            n.replace(n.scenarioStack.withPluginTag(HasRecorderTag, new QueryAndTraceTag(n, newTag.name, newTag)))
            n.continueWith(newTag, ec)
          }

        case _ =>
          n.completeWithException(
            new IllegalStateException(
              "cannot handle a node - expected args of a TemporalSurfaceQuery, but found " + n.args().toList),
            ec)
      }
    false
  }
}
object TemporalSurfaceDALAccessTracePlugin {
  private val log = getLogger(getClass)
}
class TemporalSurfaceDALAccessTracePlugin(next: SchedulerPlugin) extends TemporalSurfaceTracePlugin {

  def adapt(n: NodeTask, ec: OGSchedulerContext): Boolean = {
    val tagOption = tag(n)
    tagOption foreach { scope =>
      scope.recorder.recordDalAccess(n, ec)
      TemporalContextTrace.traceDalAccess(scope, n)
    }
    next.adapt(n, ec)
  }
}
object DataAccessTrace {
  val log = getLogger(getClass)

}

sealed trait QueryAndTraceBaseTag {
  def recorderTag: Option[QueryRecorderTag]
}

object NoopQueryPluginTag extends QueryAndTraceBaseTag {
  val recorderTag: Option[QueryRecorderTag] = None
}

class QueryAndTraceTag(node: NodeTask, info: String, val recorderTag: Option[QueryRecorderTag])
    extends SimplePluginNodeTraceInfo(node, info)
    with QueryAndTraceBaseTag {
  def this(node: NodeTask, info: String, recorderTag: QueryRecorderTag) = this(node, info, Some(recorderTag))
}

object HasRecorderTag extends NonForwardingPluginTagKey[QueryAndTraceBaseTag] {}

abstract class QueryRecorderTag extends NodeAwaiter {

  def recorder: CommandRecorder
  def temporalSurface: TemporalSurface
  def name: String

  def recorderTag = this
  protected val children: mutable.Map[AnyRef, QueryPluginChild] =
    new java.util.IdentityHashMap[AnyRef, QueryPluginChild]().asScala
  def recorderFor(id: AnyRef, temporalSurface: TemporalSurface, name: String) = synchronized {
    children.getOrElseUpdate(
      id, {
        val created = root.getOrCreateChild(this, id, temporalSurface, name)
        created
      })
  }
  def root: QueryPluginTag

  def localStats: Seq[String]
  def hierarchyStats(psb: PrettyStringBuilder): Unit = {
    localStats foreach psb.appendln
    val snap = synchronized((children filter (!_._2.used)).values.toList.sortBy(_.recorder.startTime))
    psb.indent()
    snap foreach { _.hierarchyStats(psb) }
    psb.unIndent()
  }
  def expand: Unit = {
    temporalSurface.children foreach { c =>
      val tag = c.tag.map(tag => s" tag:`$tag`").getOrElse("")
      recorderFor(c, c, "temporal surface " + c.id + tag)
    }
  }
  def used = recorder.used

}
class QueryPluginTag(val command: TemporalSurfaceQuery, val temporalContext: TemporalContextImpl, n: NodeTask)
    extends QueryRecorderTag {

  def temporalSurface = temporalContext
  def name = {
    val tag = temporalContext.tag.map(tag => s" tag:`$tag`").getOrElse("")
    "root context " + temporalContext.id + " " + tag
  }

  override val recorder = CommandRecorder.apply

  override def localStats =
    Seq(
      s" RECORD ACCESS command = $command",
      s" on context ${temporalContext.id} stats = $recorder :: result=${n.resultAsString} ")
  override def onChildCompleted(eq: EvaluationQueue, n: NodeTask): Unit = {
    recorder.complete
    val psb = new PrettyStringBuilder
    hierarchyStats(psb)
    DataAccessTrace.log.info(psb.toString)
  }
  override def root = this
  protected val allChildren: mutable.Map[AnyRef, QueryPluginChild] =
    new java.util.IdentityHashMap[AnyRef, QueryPluginChild]().asScala

  def getOrCreateChild(parent: QueryRecorderTag, id: AnyRef, temporalSurface: TemporalSurface, name: String) =
    synchronized {
      allChildren.getOrElseUpdate(id, new QueryPluginChild(parent, id, temporalSurface, name))
    }

  def getAnyChild(id: AnyRef) = if (id == temporalContext) this else synchronized { allChildren(id) }
  expand
}

class QueryPluginChild(
    val parent: QueryRecorderTag,
    val id: AnyRef,
    val temporalSurface: TemporalSurface,
    val name: String)
    extends QueryRecorderTag {
  override val recorder = parent.recorder.childRecorder(false)
  override def root = parent.root
  override def onChildCompleted(eq: EvaluationQueue, n: NodeTask): Unit = {
    recorder.complete
  }
  override def localStats = Seq(s" on $name stats = $recorder")
  expand
}

class QueryPluginTagForTCMigration(
    override val command: TemporalSurfaceQuery,
    override val temporalContext: TemporalContextImpl,
    n: NodeTask)
    extends QueryPluginTag(command, temporalContext, n) {

  override def onChildCompleted(eq: EvaluationQueue, node: NodeTask): Unit = {
    TemporalContextTrace.traceEntity(root.command, node, temporalSurface)
  }

  override def getOrCreateChild(parent: QueryRecorderTag, id: AnyRef, temporalSurface: TemporalSurface, name: String) =
    synchronized {
      allChildren.getOrElseUpdate(id, new QueryPluginChildTagForTCMigration(parent, id, temporalSurface, name))
    }

}

class QueryPluginChildTagForTCMigration(
    override val parent: QueryRecorderTag,
    override val id: AnyRef,
    override val temporalSurface: TemporalSurface,
    override val name: String)
    extends QueryPluginChild(parent, id, temporalSurface, name) {

  override def onChildCompleted(eq: EvaluationQueue, node: NodeTask): Unit = {
    TemporalContextTrace.traceEntity(root.command, node, temporalSurface)
  }
}
