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
package optimus.graph

import optimus.platform._
import optimus.platform.annotations.nodeSync

import scala.util._

trait JobFriendlySchedulerPlugin extends SchedulerPlugin

/** Marks plugin tags that are transparent to @job. They will not be stored in the job identity. */
trait JobForwardingPluginTagKey[T] extends ForwardingPluginTagKey[T]
trait JobNonForwardingPluginTagKey[T] extends NonForwardingPluginTagKey[T]

/** Used to control how to publish from @job-nodes */
private[optimus] sealed trait JobDecoratorOptions extends Serializable {
  private[graph] def implOpt: Option[JobDecorator]
}

/** Turn off all publishing when executing @job-nodes */
case object JobDecoratorOff extends JobDecoratorOptions {
  private[graph] override def implOpt: Option[JobDecorator] = None
}

/** Override the default implementation of the [[JobDecorator]] used when executing @job-nodes */
final case class JobDecoratorFromClass(className: String) extends JobDecoratorOptions {
  @transient private[graph] override lazy val implOpt = JobDecorator.fromClassName(className)
}

/** Plugin tag key to hold the decorator options. Can be used for tests or simply to turn off publishing */
private[optimus] case object JobDecoratorPluginKey extends JobForwardingPluginTagKey[JobDecoratorOptions]

private[graph] class JobPlugin extends SchedulerPlugin {

  private[this] var subPlugin: SchedulerPlugin = _

  private[graph] def hasSubPlugin: Boolean = subPlugin ne null
  private[graph] def setSubPlugin(sp: SchedulerPlugin): Unit =
    if (!hasSubPlugin && (sp eq null)) throw new GraphInInvalidState("Cannot remove non existing sub-plugin from @job")
    else subPlugin = sp

  override val pluginType: PluginType = PluginType.JOB

  def adapt(n: NodeTask, ec: OGSchedulerContext): Boolean =
    subPlugin match {
      case _: JobFriendlySchedulerPlugin if subPlugin.adapt(n, ec) =>
        // do nothing because it is the responsibility of the plugin to later call back
        // into @job tracking code
        true
      case _ =>
        val cn = n.asInstanceOf[CompletableNode[Any]]
        JobDecorator
          .ifPresent(cn.scenarioStack, Seq(cn)) { jobDecorator =>
            val rn = jobDecorator.decorateSingleNode$queued(cn) { n: Node[Any] =>
              // this method may be called from a different graph thread than the original adapt above, so cannot pass references
              // to OGSchedulerContext, and need to fetch our current one
              val ec = OGSchedulerContext.current()
              // handing back to the scheduler a previously adapted node is the only legit use case of `markAsRunnableAndEnqueue`
              if (!hasSubPlugin || !subPlugin.adapt(n, ec)) ec.scheduler.markAsRunnableAndEnqueue(n)
            }
            // deal explicitly with failures at the @job decorator level,
            rn.continueWith(
              (eq: EvaluationQueue, _: NodeTask) => {
                // no need to manually complete node on success, as that will be done by
                // at the decorator level
                if (rn.isDoneWithException && !cn.isDoneEx) {
                  cn.combineInfo(rn, eq)
                  cn.completeFromNode(rn, eq)
                }
              },
              EvaluationContext.scheduler
            )
          }
          .isDefined
    }
}

private[graph] trait JobDecorator {

  /**
   * The entry point into @job decoration for single nodes.
   * One node will map to one job execution entity.
   * @see [[JobPlugin]]
   */
  @nodeSync
  def decorateSingleNode[T](node: CompletableNode[T])(enqueue: CompletableNode[T] => Unit): Unit
  def decorateSingleNode$queued[T](node: CompletableNode[T])(enqueue: CompletableNode[T] => Unit): Node[Unit]

  /**
   * The entry point into @job decoration for batchers inheriting from [[GroupingNodeBatcherSchedulerPlugin]].
   * One group of nodes will map to one job exection entity.
   * @see [[GroupingNodeBatcherSchedulerPlugin]]
   */
  @nodeSync
  def decorateBatch[T, NodeT <: CompletableNode[T]](nodes: Seq[NodeT])(enqueue: Seq[NodeT] => Node[Seq[T]]): Seq[T]
  def decorateBatch$queued[T, NodeT <: CompletableNode[T]](nodes: Seq[NodeT])(
      enqueue: Seq[NodeT] => Node[Seq[T]]): Node[Seq[T]]

  /**
   * The entry point into @job decoration for batchers inheriting from [[GroupingNodeBatcherWithTrySchedulerPlugin]].
   * One group of nodes will map to one job exection entity.
   * @see [[GroupingNodeBatcherWithTrySchedulerPlugin]]
   */
  @nodeSync
  def decorateTryBatch[T, NodeT <: CompletableNode[T]](nodes: Seq[NodeT])(
      enqueue: Seq[NodeT] => Node[Seq[Try[T]]]): Seq[Try[T]]
  def decorateTryBatch$queued[T, NodeT <: CompletableNode[T]](nodes: Seq[NodeT])(
      enqueue: Seq[NodeT] => Node[Seq[Try[T]]]): Node[Seq[Try[T]]]

}

private object JobDecorator {

  // `JobDecoratorImpl` requires `dal_client`. Construct the instance reflectively to break
  // the dependency cycle `dal_client` depends-on `core` depends-on `dal_client`.
  // Added bonus is that it keeps `core` small and nimble, and as such quicker to compile.
  private[this] lazy val implOpt = fromClassName("optimus.graph.JobDecoratorImpl")

  private[graph] def fromClassName(className: String): Option[JobDecorator] = {
    val classOpt = Try(Class.forName(className)).toOption
    classOpt.map(_.getDeclaredConstructor().newInstance().asInstanceOf[JobDecorator])
  }

  /** Invoke the callback if the nodes are @job, and there is a configured [[JobDecorator]] on the scenario stack. */
  def ifPresent[T, R](ss: ScenarioStack, nodes: Seq[Node[T]])(f: JobDecorator => R): Option[R] = {
    if (!areJobs(nodes)) None
    else {
      val jobDecorator = ss.findPluginTag(JobDecoratorPluginKey) match {
        case Some(jobDecoratorOptions) => jobDecoratorOptions.implOpt
        case None                      => implOpt
      }
      jobDecorator.map(f)
    }
  }

  /** @return `true` iff *all* nodes are annotated with @job. */
  def areJobs(nodes: Seq[Node[_]]): Boolean =
    if (!nodes.exists(_.executionInfo.isJob)) false
    else {
      require(nodes.forall(_.executionInfo.isJob), "Cannot mix @job nodes with regular nodes.")
      true
    }

}
