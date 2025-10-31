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

import optimus.core.SparseBitSet
import optimus.graph.TweakTemplate.TTModifyOriginal
import optimus.graph.diagnostics.NodeName
import optimus.graph.{PropertyNode => PN}
import optimus.graph.{TweakNode => TN}
import optimus.platform._
import optimus.platform.util.PrettyStringBuilder
import optimus.platform.{RecordingScenarioStack => RSS}
import optimus.platform.{ScenarioStack => SS}

/**
 * Node implementation to compute the effect of a Tweak and reflect it as a Node computeGenerator is one of the
 * following (@see optimus.graph.TweakNode.getComputeNode)
 *
 *   1. Node that isStable() most often AlreadyCompetedNode as in (e.g. e.x := 3 )
 *   1. Node that needs to be executed, but doesn't depend on input key (e.g. e.x := { a + b })
 *   1. Node that is TweakValueProviderNode
 *   1. A FunctionN that will be passed to tweakable input key PropertyNode.argsCopy() to generate a node that will be
 *      executed (e.g. e.x: = {(e, x) => { e.a + x }})
 *
 * Note: e.x := { e.x + 1 } is NOT key dependent is the sense that key (e.x) is known apriori e.x :+= 2 is key dependent
 * when target is not fixed
 *
 * Notes:
 *   1. TweakNode is already an XSFT node for evaluateInParentOfGiven and evaluateInGiven (NOT evaluateInCurrent)
 *      Consider (only interesting for byName tweaks: see above)
 *   1. {{{
 *        given(e.x := e => some_code) {
 *          e.x // Used this in scenario....
 *          given(e.some_non_X := whatever) {
 *            e.x // Re-uses  RHS because effectively e.x tweak freezes the value of e.x (only in these 2 modes)
 *          }
 *        }
 *     }}}
 *   1. scenarioStack() on the tweak node itself is for caching identity (e.g. XSFT)
 */
class TweakNode[T](
    private[optimus] val tweak: Tweak, // The original source tweak (this should not cause increased memory)
    private val givenSS: SS, // ScenarioStack where the tweak was found
    _key: PN[T] // Underscore is just to remember not to capture
) extends ProxyPropertyNode[T] {
  srcNodeTemplate = _key

  private var computeSS: SS = _ // ScenarioStack for the right hand side
  private var computeNode: Node[T] = _
  // If scenarioStack.isRecordingTweakUsage = true on complete will compute TTN as part of the result
  private var tweakTreeNode: TweakTreeNode = _

  {
    if (DiagnosticSettings.traceTweaksEnabled && OGTrace.observer.traceTweaks && tweak != null)
      setTweakInfection(new SparseBitSet(tweak.id))

    val info = propertyInfo
    if (info.isDirectlyTweakable) setTweakPropertyDependency(info.tweakMask)

  }
  override def run(ec: OGSchedulerContext): Unit = {
    initComputeScenarioStackAndNode()
    ec.enqueue(computeNode)
    computeNode.continueWith(this, ec)
  }

  override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
    finalizeTweakTreeNode()
    if (
      tweak != null && tweak.readsTweakables != null && !child.isTweakPropertyDependencySubsetOf(
        tweak.readsTweakables.mask)
    ) {
      completeWithException(new GraphInInvalidState("Tweak RHS read unexpected tweakables"), eq)
    } else
      completeFromNode(computeNode, eq)
  }

  /** Execution should not see it as the original node */
  override def executionInfo: NodeTaskInfo = srcNodeTemplate.propertyInfo.tweakInfo()

  /**
   * When reporting back the computeSS's recordings to the TweakNode's SS we need to wrap them in a TTN. This captures
   * the fact that this particular tweak, to this particular tweakable, had these dependencies. During XS matching, if
   * we find the same tweak to the same tweakable in the key ScenarioStack we'll then check (recursively) that these
   * dependencies *as seen from that new tweak* match those that we saw here. See [XS_BY_NAME_TWEAKS]
   */
  private final def finalizeTweakTreeNode(): Unit = {
    if (scenarioStack.isTrackingOrRecordingTweakUsage) {
      val nestedTweakables = if (computeSS eq null) null else computeSS.tweakableListener.recordedTweakables
      tweakTreeNode = toTweakTreeNode(nestedTweakables)
      if (isXScenarioOwner)
        replace(scenarioStack().withRecorded(new RecordedTweakables(tweakTreeNode)))
    }
  }

  final def toTweakTreeNode(recordedTweakables: RecordedTweakables): TweakTreeNode =
    new TweakTreeNode(tweak, srcNodeTemplate.tidyKey, givenSS.trackingDepth, recordedTweakables)

  final def trackingDepth: Int = givenSS.trackingDepth

  // This should only be called on completed TweakNodes running inside a RecordingScenarioStack (either from their own
  // XS Proxy or from some transitive caller's XS proxy)
  override protected def reportTweakableUseBy(node: NodeTask): Unit = {
    if (tweakTreeNode ne null) // If result is stable
      node.scenarioStack.tweakableListener.onTweakUsedBy(tweakTreeNode, node)
    else if (Settings.schedulerAsserts)
      throw new GraphInInvalidState("Asking to report not-recorded tweaks")
  }

  private def initComputeScenarioStackAndNode(): Unit = {
    // If we did get given a RecordingScenarioStack then we need to replace computeSS with an RS too so that we
    // capture the dependencies of this tweak. See [XS_BY_NAME_TWEAKS].
    if (scenarioStack.isRecordingTweakUsage) {
      val trackingProxy = scenarioStack.tweakableListener.trackingProxy
      if (scenarioStack.isRecordingWhenDependencies) {
        val listener = new WhenNodeRecordingTweakableListener(computeSS, trackingProxy, scenarioStack.tweakableListener)
        computeSS = RSS.withExistingListener(computeSS, listener)
      } else if (scenarioStack.tweakableListener ne computeSS.tweakableListener) {
        // ss.tweakableListener is the same as computeSS.tweakableListener in Tweak.bind case, in which case no need
        // to create a new listener for the computeSS (and doing so will result in a hang rather than a CircularReferenceException
        // in the case of Tweak.bind cycles [SEE_TWEAK_BIND_XS_CYCLE])
        // need to use the tweakable listener from `scenarioStack` rather than from computeSS
        computeSS = RSS.withNewListener(computeSS, trackingProxy, callingTl = scenarioStack.tweakableListener)
      }
    }
    computeNode = getComputeNode(computeSS, srcNodeTemplate)
  }

  /** Create or return RHS of the tweak, the caller is responsible for making a sensible use of it! */
  private[optimus] final def getComputeNode(evaluateInScenarioStack: ScenarioStack, key: PN[T]): Node[T] = {
    val computeNode = tweak.tweakTemplate.getComputeNode(evaluateInScenarioStack, key)
    // at this point, computeNode0 is attached to a scenario, but not necessarily the scenario we do the modify lookup in.
    withModifyOriginal(computeNode.asInstanceOf[Node[T]], evaluateInScenarioStack, key)
  }

  protected def withModifyOriginal(computedValue: Node[T], stack: ScenarioStack, original: PN[T]): Node[T] =
    computedValue

  /** Completes the TweakNode immidiately, in loom will be just removed/inlined */
  final def initWithValue(v: Any, requestingSS: SS): Unit = {
    _result = v.asInstanceOf[T]
    initAsCompleted(requestingSS)
    finalizeTweakTreeNode() // Normally would have to go before completion, but this is initialization
  }

  /** Tweak evaluation resulted in exception */
  final def initWithException(ex: Throwable, requestingSS: SS): Unit = {
    initAsFailed(ex, requestingSS)
    finalizeTweakTreeNode() // Normally would have to go before completion, but this is initialization
  }

  /**
   * givenSS (where the tweak was found/setup) requestingSS (scenario stack that requested the value and is getting this
   * tweak node instead)
   */
  final def initialize(requestingSS: SS): TN[T] = {
    val tweakTemplate = tweak.tweakTemplate

    if (tweakTemplate.resultIsStable) {
      throw new GraphInInvalidState("Use initAsCompleted for stable tweak results")
    }
    var scenarioStack: ScenarioStack = null // scenarioStack where TweakNode is visible and .cacheID used for caching
    val evaluateIn = tweak.evaluateIn

    // The most common case
    if (evaluateIn == Tweak.evaluateInParentOfGiven) { // by name
      scenarioStack = givenSS // [SEE_TWEAK_IS_XS_FT]
      computeSS = givenSS.parent.withSIParamsFrom(requestingSS)
    } else if (evaluateIn == Tweak.evaluateInGiven) { // bindOnce
      scenarioStack = givenSS // [SEE_TWEAK_IS_XS_FT]
      computeSS = givenSS.withSIParamsFrom(requestingSS)
    } else if (evaluateIn == Tweak.evaluateInCurrent || evaluateIn == Tweak.evaluateInGivenWhenExpanding) { // bind
      scenarioStack = requestingSS
      computeSS = requestingSS
    } else throw new GraphInInvalidState("Unknown evaluateIn enum")

    // if the requesting SS has a different tweakable listener to the given block we can't re-use cache from there
    // Note: this also takes care of some recording/tracking scenario requesting this node!
    if (givenSS.tweakableListener ne requestingSS.tweakableListener)
      scenarioStack = requestingSS

    attach(scenarioStack)

    this
  }

  /** Hide name suffix because it will be added in the custom pretty printer */
  override def name_suffix = ""

  /** Display values when they are trivial */
  override def writePrettyString(sb: PrettyStringBuilder): PrettyStringBuilder = {
    super.writePrettyString(sb)
    sb ++= tweak.tweakTemplate
  }

  private[optimus] final def tweakLocation: String = {
    if (tweak != null && tweak.initSite != null) tweak.initSite.toString
    else NodeName.nameAndSource(tweak.tweakTemplate.computeGenerator)
  }
}

/**
 * Class for all tweaks of the form e.x :op= { y } (which is eq to e.x := op(e.x, y))
 *
 * Note: assumes that modify itself is a non-node function.
 *
 * srcNode is the tweakable node we are modifying (i.e. "e.x" in the above) and computeNode is the right hand side of
 * the operator (i.e. "y" in the above)
 */
final class TweakNodeModifyOriginal[T](tweak: Tweak, givenSS: SS, ttmo: TTModifyOriginal[T], _key: PN[T])
    extends TN[T](tweak, givenSS, _key) {
  // Here is where we modify our compute generator so that it applies its result (using `modify`) to the original value
  // of the node that we look up in `stack`.
  override protected def withModifyOriginal(computedValue: Node[T], stack: ScenarioStack, original: PN[T]): Node[T] = {
    val out = new CompletableNode[T] {
      override def executionInfo(): NodeTaskInfo = NodeTaskInfo.TweakModifyOriginal
      private val key = original

      private var state: Int = _
      private var srcNode: Node[T] = _
      private val computeNode: Node[T] = computedValue

      override def run(ec: OGSchedulerContext): Unit = {
        state match {
          case 0 =>
            // early enqueue of the compute node so that it runs in parallel
            ec.enqueue(computeNode)

            // resolve tweak
            srcNode = scenarioStack().getNode(key)
            ec.enqueue(srcNode)
            state = 1
            srcNode.continueWith(this, ec)

          case 1 =>
            // our compute node was already enqueued so we don't need to enqueue it again
            state = 2
            computeNode.continueWith(this, ec)

          case 2 =>
            state = 3

            combineInfo(computeNode, ec)
            val computeR = computeNode.result

            combineInfo(srcNode, ec)
            val srcR = srcNode.result

            val modifiedResult = ttmo.op(srcR, computeR)
            completeWithResult(modifiedResult, ec)

          case otherwise =>
            throw new GraphInInvalidState(s"impossible state $otherwise")
        }
      }
    }

    out.attach(stack.withCacheableTransitively)
    out
  }
}
