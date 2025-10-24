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

import optimus.graph.ConvertByNameToByValueNode.isTweakRedundant
import optimus.graph.loom.TrivialNode
import optimus.platform.AdvancedUtils
import optimus.platform.EvaluationContext
import optimus.platform.EvaluationQueue
import optimus.platform.Scenario
import optimus.platform.ScenarioStack
import optimus.platform.Tweak
import optimus.platform.inputs.loaders.FrozenNodeInputMap

import java.util
import scala.collection.mutable.ArrayBuffer

object ConvertByNameToByValueNode {
  private def isTweakRedundant(t: Tweak, ss: ScenarioStack): Boolean = if (
    Settings.removeRedundantTweaks && t.shouldCheckForRedundancy
  ) {
    // t.isPossiblyRedundant currently only returns true for InstancePropertyTarget
    val target = t.target.asInstanceOf[InstancePropertyTarget[_, _]]
    if (target.propertyInfo.wasTweakedByProperty()) false // Don't support propertyTweaks for now
    else {
      val tRes = t.tweakTemplate.immediateResult
      // isPossiblyRedundant == true only TrivialNode
      val key = target.key
      val alreadyTweak = TwkResolver.findInstanceTweak(key, ss)
      // key.result here is the underlying untweaked value of the node
      val tMethod = key.asInstanceOf[TrivialNode]
      val compareTo = if (alreadyTweak eq null) tMethod.trivialResult() else alreadyTweak.tweakTemplate.immediateResult
      tRes == compareTo
    }
  } else false

  def removeRedundantTweaks(s: Scenario, ss: ScenarioStack): Scenario =
    if (s.hasPossiblyRedundantTweaks && !s.keepRedundantTweaks && !ss.isSelfOrAncestorScenarioIndependent)
      s.filterConserve(!isTweakRedundant(_, ss), s.flagsWithoutHasRedundant)
    else s
}

/**
 * This node/class translate whatever byName tweaks it can into byValue tweak There is a potential loss of performance
 * here: it does this one scenario level at at time Note: lots of optimization opportunities
 */
class ConvertByNameToByValueNode[T](
    scenarioStack: ScenarioStack,
    originalScenario: Scenario,
    node: Node[T],
    isPermittedGiven: Boolean = false)
    extends CompletableNode[T] {
  private var curSS = scenarioStack
  private var nextScenario = originalScenario.nestedScenarios
  private val rtweaks = new ArrayBuffer[AnyRef /*Tweak|RTweak*/ ]()
  // Need to maintain original order of tweaks
  private var rtweakAwaiting: RTweak = _
  private var currentScenario: Scenario = _

  private class RTweak(var twk: Tweak, var twkNode: Node[_], val next: RTweak)

  attach(scenarioStack)

  private def parseAndEnqueue(s: Scenario, eq: EvaluationQueue): Unit = {
    currentScenario = s
    rtweaks.clear()
    rtweakAwaiting = null

    val it = s.topLevelTweaks.iterator
    while (it.hasNext) {
      val twk = it.next()
      if (twk.isReducibleToByValue) {
        val twkNode = twk.computableValueNode(curSS)
        eq.enqueue(this, twkNode) // Enqueue all tweaks, but wait for one a time...
        rtweakAwaiting = new RTweak(twk, twkNode, rtweakAwaiting)
        rtweaks += rtweakAwaiting
      } else rtweaks += twk
    }
    rtweakAwaiting.twkNode.continueWith(this, eq) // Start waiting for one tweak at a time
  }

  private def workOnScenario(curScenario: Scenario, eq: EvaluationQueue): Unit = {
    var s = curScenario
    var inWaiting = false

    def setCurSS(): Unit = {
      curSS = curSS.createChildInternal(s.withoutNested, FrozenNodeInputMap.empty, 0, node) // tweak expansion can throw
      if (nextScenario.isEmpty) s = null
      else {
        s = nextScenario.head
        nextScenario = nextScenario.tail
      }
    }

    do {
      // Quick check that the 'next' scenario contains reducible tweaks
      if (Settings.convertByNameToByValue && s.hasReducibleToByValueTweaks) {
        parseAndEnqueue(s, eq)
        inWaiting = true
      } else {
        if (Settings.removeRedundantTweaks)
          s = ConvertByNameToByValueNode.removeRedundantTweaks(s, curSS)
        try setCurSS()
        catch {
          case e: Exception =>
            completeWithException(e, eq)
            inWaiting = true // break loop
        }
      }
    } while ((s ne null) && !inWaiting)

    if (!inWaiting) {
      if (isPermittedGiven)
        node.attach(AdvancedUtils.withoutTemporalContextPermitted(curSS))
      else
        node.attach(curSS)
      eq.enqueue(this, node)
      node.continueWith(this, eq)
    }
  }

  override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
    if (child eq node)
      // Original node finished and we take its value (i.e. given() { thisIsTheNode }
      completeFromNode(node, eq)
    else {
      if (rtweakAwaiting.twkNode ne child) throw new GraphInInvalidState()
      // Callbacks onChildCompleted are one at a time (because we continueWith to one a time)

      // Note: we use the result below, but OOB info is forwarded to this ConvertByNameToByValueNode node!
      combineInfo(child, eq)
      // copy as a stable node so that TweakNode won't clone and re-execute it later on
      val asStable = AlreadyCompletedOrFailedNode.withResultOf(rtweakAwaiting.twkNode)
      rtweakAwaiting.twk = rtweakAwaiting.twk.reduceToByValue(asStable)
      rtweakAwaiting.twkNode = null // Eager cleanup
      rtweakAwaiting = rtweakAwaiting.next
      if (rtweakAwaiting eq null) {
        var newTweaks = new Array[Tweak](rtweaks.length)
        var count = 0
        def add(t: Tweak): Unit = if (originalScenario.keepRedundantTweaks || !isTweakRedundant(t, curSS)) {
          newTweaks(count) = t
          count += 1
        }

        var i = 0
        while (i < rtweaks.length) {
          rtweaks(i) match {
            case t: Tweak  => add(t)
            case r: RTweak => add(r.twk)
          }
          i += 1
        }
        if (count != newTweaks.length) newTweaks = util.Arrays.copyOf(newTweaks, count)

        EvaluationContext.asIfCalledFrom(this, eq) {
          workOnScenario(new Scenario(newTweaks, Nil, currentScenario.flagsWithoutHasRedundantOrRBValue), eq)
        }
      } else {
        rtweakAwaiting.twkNode.continueWith(this, eq)
      }
    }
  }

  override def run(ec: OGSchedulerContext): Unit = workOnScenario(originalScenario, ec)

  override def executionInfo(): NodeTaskInfo = NodeTaskInfo.ConvertByNameToByValue
}
