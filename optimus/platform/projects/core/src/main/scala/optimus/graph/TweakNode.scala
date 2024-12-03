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
import optimus.graph.diagnostics.NodeName
import optimus.graph.loom.LNodeFunction
import optimus.graph.loom.LNodeFunction0
import optimus.graph.{PropertyNode => PN}
import optimus.graph.{TweakNode => TN}
import optimus.platform.util.PrettyStringBuilder
import optimus.platform._
import optimus.platform.{RecordingScenarioStack => RSS}
import optimus.platform.{ScenarioStack => SS}

object TweakNode {
  private object UnknownValue
}

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
 *      Consider (only interesting for byName tweaks: see above
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
class TweakNode[T](private[optimus] val computeGenerator: AnyRef) extends ProxyPropertyNode[T] {
  protected var computeNode: Node[T] = _
  private var tweak: Tweak = _ // The original source tweak (this should not cause increased memory)
  private var givenSS: ScenarioStack = _ // ScenarioStack where the tweak was found
  protected var computeSS: ScenarioStack = _ // ScenarioStack for the right hand side
  // If scenarioStack.isRecordingTweakUsage = true on complete will compute TTN as part of the result
  private var tweakTreeNode: TweakTreeNode = _

  override def run(ec: OGSchedulerContext): Unit = {
    initComputeScenarioStackAndNode()
    ec.enqueue(computeNode)
    computeNode.continueWith(this, ec)
  }

  override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
    finalizeTweakTreeNode()
    completeFromNode(computeNode, eq)
  }

  /**
   * This method is used in the context of ScenarioStack.equals need to check class due to the overrides for :*= :+=
   * etc.
   */
  def equalsAsTemplates(other: TweakNode[_]): Boolean = {
    def generatorsAreCompatible: Boolean =
      if (computeGenerator.getClass ne other.computeGenerator.getClass) {
        (computeGenerator, other.computeGenerator) match {
          case (nA: NodeTask, nB: NodeTask) if nA.isDoneWithResult && nB.isDoneWithResult =>
            nA.resultObject() == nB.resultObject()
          case (nA: NodeTask, nB: NodeTask) if nA.isDoneWithException && nB.isDoneWithException =>
            nA.exception() == nB.exception()
          case _ => false
        }
      } else NodeClsIDSupport.equals(other.computeGenerator, computeGenerator)
    getClass == other.getClass && generatorsAreCompatible
  }

  def hashCodeAsTemplate: Int = NodeClsIDSupport.hashCode(computeGenerator)

  /** Execution should not see it as the original node */
  override def executionInfo: NodeTaskInfo = srcNodeTemplate.propertyInfo.tweakInfo()

  /**
   * When reporting back the computeSS's recordings to the TweakNode's SS we need to wrap them in a TTN. This captures
   * the fact that this particular tweak, to this particular tweakable, had these dependencies. During XS matching, if
   * we find the same tweak to the same tweakable in the key ScenarioStack we'll then check (recursively) that these
   * dependencies *as seen from that new tweak* match those that we saw here. See [XS_BY_NAME_TWEAKS]
   */
  protected final def finalizeTweakTreeNode(): Unit = {
    if (scenarioStack.isTrackingOrRecordingTweakUsage) {
      val nestedTweakables = if (computeSS eq null) null else computeSS.tweakableListener.recordedTweakables
      tweakTreeNode = toTweakTreeNode(nestedTweakables)
      if (isXScenarioOwner)
        scenarioStack.onXSOriginalCompleted(new RecordedTweakables(tweakTreeNode))
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

  final protected def initComputeScenarioStackAndNode(): Unit = if (!isDone) {
    // If we did get given a RecordingScenarioStack then we need to replace computeSS with an RS too so that we
    // capture the dependencies of this tweak. See [XS_BY_NAME_TWEAKS].
    // Also set flag on SS to avoid waiting for XS node that might cause circular reference exception, because if the
    // tweak's computeNode refers (in)directly back to the tweakable then XS will wait for the currently running
    // tweakable to complete. This flag must be propagated transitively to all child XS nodes of the compute node.
    // See [XS_NO_WAIT]
    if (scenarioStack.isRecordingTweakUsage) {
      val trackingProxy = scenarioStack.tweakableListener.trackingProxy
      if (scenarioStack.isRecordingWhenDependencies) {
        val listener = new WhenNodeRecordingTweakableListener(computeSS, trackingProxy)
        computeSS = RSS.withExistingListener(computeSS, listener, noWaitForXs = true)
      } else if (scenarioStack.tweakableListener ne computeSS.tweakableListener) {
        // ss.tweakableListener is the same as computeSS.tweakableListener in Tweak.bind case, in which case no need
        // to create a new listener for the computeSS (and doing so will result in a hang rather than a CircularReferenceException
        // in the case of Tweak.bind cycles [SEE_TWEAK_BIND_XS_CYCLE])
        computeSS = RSS.withNewListener(computeSS, trackingProxy, noWaitForXs = true, earlyUpReport = true)
      }
    }
    computeNode = getComputeNode(computeSS, srcNodeTemplate)
  }

  /** Create or return RHS of the tweak, the caller is responsible for making a sensible use of it! */
  private[optimus] final def getComputeNode(evaluateInScenarioStack: ScenarioStack, key: PN[T]): Node[T] = {
    val computeNode0 =
      computeGenerator match {
        case tweakValueProviderNode: TweakValueProviderNode[T @unchecked] =>
          val cnode = if (tweakValueProviderNode.isKeyDependent) {
            tweakValueProviderNode.copyWith(key, evaluateInScenarioStack)
          } else tweakValueProviderNode
          cnode.replace(evaluateInScenarioStack.siRoot)
          cnode
        case lnodeDef: LNodeFunction[T @unchecked] =>
          lnodeDef.toNodeWith(key)
        case newComputeNode: Node[T @unchecked] =>
          if (newComputeNode.isStable) newComputeNode
          else newComputeNode.cloneTask().asInstanceOf[Node[T]]
        case _ =>
          if (key eq null) throw new GraphInInvalidState()
          key.argsCopy(computeGenerator).asInstanceOf[Node[T]]
      }

    if (computeNode0.scenarioStack() == null)
      computeNode0.replace(evaluateInScenarioStack.withCacheableTransitively)

    // at this point, computeNode0 is attached to a scenario, but not necessarily the scenario we do the modify lookup in.
    withModifyOriginal(computeNode0, evaluateInScenarioStack, key)
  }

  protected def withModifyOriginal(computedValue: Node[T], stack: ScenarioStack, original: PN[T]): Node[T] =
    computedValue

  /**
   * Original tweaks are created with current TweakNode as template. In different scenario stacks the same template can
   * result in different nodes with different results, hence cloning
   *
   * givenSS (where the tweak was found/setup) requestingSS (scenario stack that requested the value and is getting this
   * tweak node instead)
   */
  final def cloneWith(tweak: Tweak, evaluateIn: Int, requestingSS: SS, givenSS: SS, srcNodeTemplate: PN[T]): TN[T] = {
    val newTweakNode = cloneWithClean // NOTE: All derived classes must maintain proper clone!
    newTweakNode.givenSS = givenSS // Store the ScenarioStack where the tweak was found....
    newTweakNode.tweak = tweak
    newTweakNode.srcNodeTemplate = srcNodeTemplate

    if (DiagnosticSettings.traceTweaksEnabled && OGTrace.observer.traceTweaks && tweak != null)
      newTweakNode.setTweakInfection(new SparseBitSet(tweak.id))

    val info = srcNodeTemplate.propertyInfo
    if (info.isDirectlyTweakable) newTweakNode.setTweakPropertyDependency(info.tweakMask)

    if (resultIsStable()) {
      // Optimize for a common case of a byValue tweak (i.e. := const)
      newTweakNode.attach(requestingSS)
      newTweakNode.finalizeTweakTreeNode() // needs to be called before the node is marked DONE for visibility reasons
      newTweakNode.initAsCompleted(computeGenerator.asInstanceOf[Node[T]], requestingSS)
    } else {
      var scenarioStack: ScenarioStack = null // scenarioStack where TweakNode is visible and .cacheID used for caching
      var computeSS: ScenarioStack = null // scenarioStack where the RHS of := will be executing

      // The most common case
      if (evaluateIn == Tweak.evaluateInParentOfGiven) { // by name
        scenarioStack = givenSS // [SEE_TWEAK_IS_XS_FT]
        computeSS = givenSS.parent.withSIParamsFrom(requestingSS)
      } else if (evaluateIn == Tweak.evaluateInGiven) { // bindOnce
        scenarioStack = givenSS // [SEE_TWEAK_IS_XS_FT]
        computeSS = givenSS.withSIParamsFrom(requestingSS)
      } else if (evaluateIn == Tweak.evaluateInCurrent) { // bind
        scenarioStack = requestingSS
        computeSS = requestingSS
      } else throw new GraphInInvalidState("Unknown evaluateIn enum")

      // if the requesting SS has a different tweakable listener to the given block we can't re-use cache from there
      // Note: this also takes care of some recording/tracking scenario requesting this node!
      if (givenSS.tweakableListener ne requestingSS.tweakableListener)
        scenarioStack = requestingSS

      newTweakNode.attach(scenarioStack)
      newTweakNode.computeSS = computeSS
    }
    newTweakNode
  }

  protected def cloneWithClean = new TweakNode[T](computeGenerator)

  /** Returns true if the result of evaluation of this tweak is a stable value */
  def resultIsStable(): Boolean = computeGenerator match {
    case node: NodeTask => node.isStable
    case _              => false
  }

  /** Returns RHS value if available and marker object otherwise */
  private[graph] def immediateResult: Any = computeGenerator match { // really returns T
    case acn: AlreadyCompletedNode[_] => acn.result
    case _                            => TweakNode.UnknownValue
  }

  /** Returns true if the result is stable or SI */
  def resultIsScenarioIndependent(): Boolean = computeGenerator match {
    case tweakValProv: TweakValueProviderNode[_] => !tweakValProv.modify
    case node: NodeTask                          => node.isStable
    case _                                       => false
  }

  /** Returns true if the tweak is byName and does NOT depend on the input key */
  def isReducibleToByValue: Boolean = computeGenerator match {
    case _: TweakValueProviderNode[_] => false
    case _: LNodeFunction0[_]         => true // 0 means it doesn't take any additional input, hence reducible
    case node: NodeTask               => !node.isStable
    case _                            => false
  }

  /**
   * Added to support property nodes with tweak-handlers, see: `AdjustASTComponent` in Optimus scalac plugin.
   */
  final def cloneWithDirectAttach(scenarioStack: ScenarioStack, srcNodeTemplate: PropertyNode[T]): TweakNode[T] = {
    cloneWith(null, Tweak.evaluateInGiven, scenarioStack, scenarioStack, srcNodeTemplate)
  }

  // must override this because srcNodeTemplate can be null for AlreadyCompletedNode, but PropertyNode.equals relies
  // on comparing propertyInfo, which is defined as srcNodeTemplate.propertyInfo for ProxyPropertyNode
  override def equals(that: Any): Boolean = that match {
    case tn: TweakNode[_] =>
      if ((srcNodeTemplate eq null) && (tn.srcNodeTemplate eq null))
        equalsAsTemplates(tn) // compares computeGenerators and our class
      else
        getClass == tn.getClass && super.equals(that)
    case _ => false
  }

  override def hashCode: Int = if (srcNodeTemplate eq null) hashCodeAsTemplate else super.hashCode

  override def name_suffix = ":="

  override def writePrettyString(sb: PrettyStringBuilder): PrettyStringBuilder = {
    writePrettyString(sb, null)
  }

  /**
   * tweak is supplied when TweakNode is printed as part of printing a Tweak otherwise tweak is null and we look at this
   * as a plain node
   */
  final def writePrettyString(sb: PrettyStringBuilder, tweak: Tweak): PrettyStringBuilder = {
    if (srcNodeTemplate ne null) super.writePrettyString(sb) // (super includes name_suffix)
    else sb ++= name_suffix ++= " "

    def resultOrException(n: Node[_]): String = {
      if (n.isDoneWithResult) n.resultObject.toString
      else if (n.isDoneWithException) n.exception.toString
      else "[not complete]"
    }

    computeGenerator match {
      case n: Node[_] if n.isStable => sb ++= resultOrException(n)
      case _ if tweak != null =>
        if (tweak.initSite != null) sb ++= "{ code... }"
        else sb ++= NodeName.nameAndSource(computeGenerator)
      case _ => sb ++= NodeName.cleanNodeClassName(computeGenerator.getClass)
    }
  }

  private[optimus] final def tweakLocation: String = {
    if (tweak != null && tweak.initSite != null) tweak.initSite.toString
    else NodeName.nameAndSource(computeGenerator)
  }
}

/**
 * Base class for all tweaks of the form e.x :op= { y } (which is eq to e.x := op(e.x, y))
 *
 * Note: assumes that modify itself is a non-node function.
 *
 * srcNode is the tweakable node we are modifying (i.e. "e.x" in the above) and computeNode is the right hand side of
 * the operator (i.e. "y" in the above)
 */
sealed abstract class TweakNodeModifyOriginal[T, Self <: TweakNodeModifyOriginal[T, Self]](computeGen: AnyRef)
    extends TweakNode[T](computeGen) { self: Self =>
  def copy(computeGen: AnyRef): Self
  def opName: String
  def op(src: T, mod: T): T

  protected override def cloneWithClean: TweakNode[T] = copy(computeGen)

  // These flags need to be set because our compute generator might be an ACPN but we still need to apply it to the
  // resolved srcNodeTemplate, which means that our full computation isn't already completed. If we don't do this, then
  // we end up incorrectly triggering the optimization in optimus.graph.ConvertByNameToByValueNode.isTweakRedundant.
  override def resultIsStable() = false
  override def isReducibleToByValue: Boolean = false

  // For printing nicely:
  override def name_suffix: String = ":" + opName + "="

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

            val modifiedResult = op(srcR, computeR)
            completeWithResult(modifiedResult, ec)

          case otherwise =>
            throw new GraphInInvalidState(s"impossible state ${otherwise}")
        }
      }
    }

    out.attach(stack.withCacheableTransitively)
    out
  }
}

object TweakNodeModifyOriginal {
  final class Plus[T: Numeric](computeGen: AnyRef) extends TweakNodeModifyOriginal[T, Plus[T]](computeGen) {
    def copy(computeGen: AnyRef): Plus[T] = new Plus[T](computeGen)
    def opName: String = "+"
    def op(src: T, mod: T): T = implicitly[Numeric[T]].plus(src, mod)
  }

  final class Minus[T: Numeric](computeGen: AnyRef) extends TweakNodeModifyOriginal[T, Minus[T]](computeGen) {
    def copy(computeGen: AnyRef): Minus[T] = new Minus[T](computeGen)
    def opName: String = "-"
    def op(src: T, mod: T): T = implicitly[Numeric[T]].minus(src, mod)
  }

  final class Times[T: Numeric](computeGen: AnyRef) extends TweakNodeModifyOriginal[T, Times[T]](computeGen) {
    def copy(computeGen: AnyRef): Times[T] = new Times[T](computeGen)
    def opName: String = "*"
    def op(src: T, mod: T): T = implicitly[Numeric[T]].times(src, mod)
  }

  final class Div[T: Fractional](computeGen: AnyRef) extends TweakNodeModifyOriginal[T, Div[T]](computeGen) {
    def copy(computeGen: AnyRef): Div[T] = new Div[T](computeGen)
    def opName: String = "/"
    def op(src: T, mod: T): T = implicitly[Fractional[T]].div(src, mod)
  }
}
