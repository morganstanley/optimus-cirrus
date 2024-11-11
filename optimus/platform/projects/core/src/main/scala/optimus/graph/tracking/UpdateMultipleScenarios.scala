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
package optimus.graph.tracking

import optimus.graph.CompletableNode
import optimus.graph.Node
import optimus.graph.NodeTask
import optimus.graph.OGSchedulerContext
import optimus.graph.Settings
import optimus.graph.tracking.DependencyTrackerRoot.TweakLambda
import optimus.platform.EvaluationContext
import optimus.platform.EvaluationQueue
import optimus.platform.Tweak

// used to evaluate the lambdas returned from MultiScenarioHandlerResult, obtaining a lock on the root first for the state of the world
// this will take a lock on the topmost common parent scenario
private[tracking] object UpdateMultipleScenarios {
  def evaluateAndApplyTweakLambdas(
      tweaks: Map[DependencyTracker, collection.Seq[Tweak]],
      tweakLambdas: Map[DependencyTracker, Iterable[TweakLambda]],
      commonParent: DependencyTracker,
      cause: EventCause
  ): Unit = {
    val trackers = tweaks.keys ++ tweakLambdas.keys
    val toResetBatchers: Set[DependencyTracker] = trackers.toSet.filter {
      case tracker if tracker.queue.currentBatcher.cause.isEmpty =>
        tracker.queue.setCurrentBatcher(commonParent.queue.currentBatcher)
        true
      case tracker if tracker.queue.currentBatcher != commonParent.queue.currentBatcher =>
        DependencyTracker.log.warn(
          s"Expected batcher on child scenario $tracker to match batcher of parent scenario $commonParent")
        true
      case _ => false
    }

    def nodifyLambdas(scenario: DependencyTracker, lambdas: Iterable[TweakLambda]): Node[collection.Seq[Tweak]] = {
      if (scenario.isDisposed) throw new DependencyTrackerDisposedException(scenario.name)
      val ss = scenario.nc_scenarioStack.withCancellationScope(commonParent.queue.currentCancellationScope)
      // need to write out apar flatmap invocation manually since we are in core (so don't have entityplugin)
      import optimus.platform.AsyncImplicits._
      val flatmap = lambdas.toSeq.apar.flatMap$newNode[Tweak, collection.Seq[Tweak]](lambda =>
        new CompletableNode[collection.Seq[Tweak]] {
          attach(ss)
          override def run(ec: OGSchedulerContext): Unit = {
            lambda.apply$queued().continueWith(this, ec)
          }
          override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
            completeFromNode(child.asInstanceOf[Node[collection.Seq[Tweak]]], eq)
          }
        })
      flatmap.attach(ss)
      flatmap
    }

    val nodes = tweakLambdas.map { case (scenario, exprs) => scenario -> nodifyLambdas(scenario, exprs) }
    EvaluationContext.enqueueAndWaitForAll(nodes.values.toSeq)
    // before we start applying tweaks we need to wait for any orphaned evaluations to complete
    commonParent.queue.evaluationBarrier(commonParent.queue.currentBatcher.cause)
    val cs = Some(commonParent.queue.currentCancellationScope)

    val results = nodes map { case (scenario, node) => scenario -> node.result }
    // deterministic order - sort by depth first (bottom up) then name
    val sorted = results.toSeq.sortBy { case (scenario, _) => (-scenario.level, scenario.name) }

    tweaks.toSeq ++ sorted foreach { case (scenario, tweaks) =>
      scenario.tweakContainer.doAddTweaks(tweaks, Settings.throwOnDuplicateInstanceTweaks, cause, cs)
    }

    toResetBatchers foreach { _.queue.resetCurrentBatcher() }
  }
}
