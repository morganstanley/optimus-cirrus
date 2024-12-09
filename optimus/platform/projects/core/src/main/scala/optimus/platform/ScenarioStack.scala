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
package optimus.platform

import optimus.breadcrumbs.ChainedID
import optimus.config.scoped.ScopedSchedulerPlugin
import optimus.graph
import optimus.graph.EvaluationState
import optimus.graph.OGTrace
import optimus.graph._
import optimus.graph.cache.NodeCache
import optimus.graph.cache._
import optimus.graph.diagnostics.GivenBlockProfile
import optimus.graph.diagnostics.PScenarioStack
import optimus.graph.diagnostics.pgo.Profiler
import optimus.graph.tracking.DependencyTracker
import optimus.graph.tracking.SnapshotScenarioStack
import optimus.graph.tracking.ttracks.RefCounter
import optimus.observability.JobConfiguration
import optimus.platform.inputs.NodeInputMapValue
import optimus.platform.inputs.NodeInputs.ScopedSINodeInput
import optimus.platform.inputs.ScopedSIEntry
import optimus.platform.inputs.loaders.FrozenNodeInputMap
import optimus.platform.inputs.loaders.LoaderSource
import optimus.platform.util.html._
import optimus.ui.ScenarioReference

import java.io.Serializable
import java.lang.ref.WeakReference
import java.time.Instant
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * A scenario stack represents the nesting context for a scenario. This is similar in concept to a list of call frames
 * connected by static links in a conventional programming language runtime.
 */
object ScenarioStack {

  def newRoot(env: RuntimeEnvironment, uniqueID: Boolean): ScenarioStack =
    newRoot(ScenarioState(env), uniqueID)

  private[optimus] def newRoot(state: UntweakedScenarioState, uniqueID: Boolean): ScenarioStack =
    newRoot(state.environment, state.inputs, state.scopedConfiguration, uniqueID)

  private[platform] def newRoot(
      environment: RuntimeEnvironment,
      inputs: FrozenNodeInputMap,
      scopedConfiguration: Map[NodeTaskInfo, ScopedSchedulerPlugin],
      uniqueID: Boolean): ScenarioStack = {
    val r = ScenarioStackShared(environment, inputs, scopedConfiguration, uniqueID).scenarioStack
    if (NodeTrace.profileSSUsage.getValue) Profiler.t_sstack_root(r)
    r
  }

  private val emptySIParams = SIParams(FrozenNodeInputMap.empty, NullCancellationScope)
  val constant: ScenarioStack = createConstantStack(EvaluationState.CACHED_TRANSITIVELY)
  val constantNC: ScenarioStack = createConstantStack(0)

  private def equivSIParams(lhs: ScenarioStack, rhs: ScenarioStack): Boolean = {
    ((lhs.flags & rhs.flags & ~EvaluationState.ALL_SI_FLAGS) == 0) && (lhs.privateCache eq rhs.privateCache) &&
    ((lhs.siParams eq rhs.siParams) || lhs.siParams.equals(rhs.siParams))
  }

  private[optimus] def isSyntheticScenario(cur: ScenarioStack): Boolean = {
    (cur.parent ne null) && (cur.parent._cacheID eq cur._cacheID)
  }

  private def createConstantStack(flags: Int): ScenarioStack =
    new ScenarioStack(
      ssShared = null,
      flags = EvaluationState.CONSTANT | flags,
      siParams =
        ScenarioStack.emptySIParams.copy(parentTrackingNodeID = ChainedID.root, trackingNodeID = ChainedID.create())
    )

  /** much like NullNode, except that this is not associated with any particular environment */
  private[this] class ConstantNullNode extends Node[Unit] {
    initAsCompleted(constant)
    override def executionInfo: NodeTaskInfo = NodeTaskInfo.Start
    override def run(ec: OGSchedulerContext): Unit = throw new UnsupportedOperationException("Null node, can't be run")
    override def result: Unit = ()
    override def isStable: Boolean = true
  }

  def nullNode: Node[Unit] = new ConstantNullNode

  private[optimus] def modifySS(
      progressTracker: ProgressTracker = null,
      params: ProgressTrackerParams = null,
      cs: CancellationScope = NullCancellationScope)(ss: ScenarioStack): ScenarioStack = {
    /*
     * Attach a progress tracker for the batch update.
     * - 'progressTracker' is the tracker associated with the widget that triggered this update
     * (from _.progressTo := channelTracker)
     * - 'params' are the progress tracking parameters for this specific update
     * (in UI they come from follow on steps .withProgress(0,2)
     */
    val ssMaybeWithProgress = ss.maybeWithProgressTracker(progressTracker, params)
    val maybeTracker = ssMaybeWithProgress.progressTracker

    /*
     * In order to cancel the follow on steps, we make use of union
     *
     * This leads to the the below structure of SS and cancellation scopes (if cs passed to this method)
     *
     * user cs (maybeTracker.params.userCancellationScope) = global handler cs
     *             (coming from optimus.ui.server.widgets.HtmlWidget.getProgressTracker)
     * cs = cancellation scope for follow on step (from DependencyTracker queue)
     * finalCs = child of both above cancellation scopes
     *
     *  [user cs]      [cs]
     *     \           /
     *      \         /
     *        finalCs
     */
    val finalCs = if ((maybeTracker ne null) && maybeTracker.isCancellable) {
      cs.union(maybeTracker.params.userCancellationScope)
    } else cs // if no cs passed, this will be NullCancellationScope
    ssMaybeWithProgress.withCancellationScope(finalCs)
  }
}

/**
 * ScenarioStack is responsible for keeping track of the scenario in which the code is executing and finding a tweak
 * matching a NodeKey. Instantiated nodes are associated with a ScenarioStack. General strategy for this class: Give
 * other classes access to the scenario at the top of the stack, but not to the stack itself.
 */
final case class ScenarioStack private[optimus] (
    /**
     * The tracking depth represents the depth of DependencyTrackers. This is use for performance within invalidation.
     * N.b. Each dependency tracker is guaranteed to be greater than its parent but intermediate Basic Scenario Stacks may
     * have the same depth set
     */
    private[optimus] val trackingDepth: Int = 0,
    // *DO NOT* remove @transient, or you will hit a bug in fast serializer.
    // we should not need it, because it's supposed to be useless since we define `ScenarioStack.writeReplace` but alas...
    @transient private val scenario: Scenario = Scenario.empty,
    name: String = null,
    /** the parent is only used to navigate through the tweaks, so not preserved in the case of a non si -> Si call */
    private var _parent: ScenarioStack = null,
    private[optimus] val _cacheID: SSCacheID = SSCacheID.newUnique(),
    private[optimus] val flags: Int = 0,
    // this should be transient (used in NodeExecutor.hasRecordingSS - replace with RECORD_TWEAK_USAGE flag?
    @volatile private[optimus] var _tweakableListener: TweakableListener = NoOpTweakableListener,
    private[optimus] val siParams: SIParams = ScenarioStack.emptySIParams,
    private[optimus] val ssShared: ScenarioStackShared = null,
    private[optimus] val privateCache: PrivateCache = null,
    /**
     * Keep a reference to a 'stable' SS. This reference will be reset when:
     *   1. A cache ID is passed in the constructor, i.e., not copied from an existing SS.
     *   1. The CACHED_TRANSITIVELY flag is turned on.
     *
     * Note that the stable SS may have a different CancellationScope (and different SI Params) to this SS
     */
    @volatile private[optimus] var _stableSS: ScenarioStack = null,
    private[optimus] var pss: PScenarioStack = null
) extends Serializable {

  if (_stableSS eq null)
    _stableSS = this

  if ((siParams.scopedPlugins ne null) && (isRoot || (siParams.scopedPlugins eq parent.siParams.scopedPlugins)))
    // invoked every time we create a scenario stack (even on engines via deserialization in `ScenarioStackMoniker`)
    // we only mark the plugin tasks if the scenario stack is root OR the scoped plugins is different from the parent's.
    // there are situations where we can mark the same NodeTaskInfo multiple times (eg: two root scenario stacks created
    // with the same siParams), but that's safe cause marking is idempotent - we just want to avoid running this code
    // too many times for no good reason.
    siParams.markPluginNodeTaskInfos()

  /** Scenario State is one to one with ScenarioStack but we don't want to expose to the 'user' all innards */
  def asScenarioState: ScenarioState = ScenarioState(this)
  def asUntweakedScenarioState: UntweakedScenarioState =
    new UntweakedScenarioState(env, siParams.scopedPlugins, siParams.nodeInputs.freeze)

  // finds the parent ss that has the current scenarioReference as scenRef
  private[optimus] def findParentByScenarioReferenceId(scenRef: ScenarioReference): ScenarioStack = {
    var cur = this
    val key = ScenarioReference.current$newNode

    while (!cur.isRoot) {
      val (snapshotTweak, ss) = cur.getTweakAndScenarioStack(key.propertyInfo, key, null)
      if (snapshotTweak != null) {
        val res = snapshotTweak.tweakValue.asInstanceOf[ScenarioReference]
        if (res.name == scenRef.name)
          return ss
      }
      // skip over scenario stacks that don't have the current tweak defined
      cur = ss.parent
    }
    null
  }
  // get an SI scenario stack from the current one
  def asSiStack(locMarker: AnyRef): ScenarioStack =
    if (isScenarioIndependent) this
    else {
      val flagsToPropagate = flags & EvaluationState.FLAGS_TO_PROPAGATE_TO_SI_STACK
      val si = siRoot
      // only create a new SiScenario stack if we have to
      if (ScenarioStack.equivSIParams(this, si)) si
      else {
        def newSIStack: ScenarioStack = {
          val si =
            ssShared.siStack.copy(
              siParams = siParams,
              privateCache = privateCache,
              flags = ssShared.siStack.flags | flagsToPropagate)

          if (NodeTrace.profileSSUsage.getValue && (locMarker ne null))
            Profiler.t_sstack_usage(si, locMarker, hit = false)

          si
        }

        // it's only safe to use cache if none of the flags that get propagated were set and there isn't a private
        // cache (since we don't currently include any of this information in the cache key)
        if (Settings.reuseSIStacks && (flagsToPropagate == 0) && (privateCache eq null))
          ssShared.siRootCache.computeIfAbsent(siParams, _ => newSIStack)
        else newSIStack
      }
    }

  /** valueEquivalence object (do not rely on its content) */
  def valueEquivalence: AnyRef = _cacheID
  def isEmpty: Boolean = _cacheID.isEmpty

  // NB. This does not match caching identity, as it requires full stack conformation (e.g.
  // including depth).
  override def equals(other: Any): Boolean = {
    other match {
      case that: ScenarioStack =>
        (that eq this) || (
          // TODO (OPTIMUS-59089):
          //  It is possible in principle to batch from different tweakable listener if we correctly report to all of
          //  them tweaks used by any of them. This isn't done in practice, so to avoid terrible false cache hit bugs
          //  we simply refuse to compare equal scenario stacks that have different listeners.
          //
          // Also of possible interest is the fact that tweakableListener is a var. This means that it is possible to
          // have scenario stacks that compare equals but have different hashcodes, if the hashcodes were computed
          // before an update to the tweakableListeners. I don't think this ever happens currently.
          (tweakableListener eq that.tweakableListener) &&
            that.getClass == getClass &&
            that.trackingDepth == trackingDepth &&
            // Optimizations using reference compare of the roots.
            // before comparing the full set of scenarios
            // up the chain since if roots are different
            // the ScenarioStacks are different.
            // We won't do full comparison of root
            // since we will encounter root at the end
            // of the parent chain...
            (that.ssShared._cacheID eq ssShared._cacheID) &&
            that.scenario == scenario &&
            that._parent == _parent)
      case _ => false
    }
  }

  private[this] var _hashCode: Int = _
  // noinspection HashCodeUsesVar
  override def hashCode: Int = {
    if (_hashCode == 0) {
      // Not using root in the hashCode despite usage in equals
      // as in equals it's only used as an optimization to avoid
      // full scenario comparison for scenario stacks with different
      // roots.
      val hc = {
        41 * (41 * (41 * (41
          + System.identityHashCode(_tweakableListener))
          + trackingDepth)
          + (if (scenario == null) 0 else scenario.hashCode))
      } + (if (_parent == null) 0 else _parent.hashCode)

      _hashCode = hc
    }
    _hashCode
  }

  def flagsAsString: String = EvaluationState.toString(flags)

  /** Get a descriptive representation of the scenario stack. */
  def prettyString: String = writeHtml(new HtmlBuilder).toPlaintext

  def writeHtml(hb: HtmlBuilder, minScenarioStackDepth: Int = 0): HtmlBuilder = {
    def writeParent(): Unit = {
      if (trackingDepth > minScenarioStackDepth) {
        if (_parent ne null)
          _parent.writeHtml(hb.newLine(), minScenarioStackDepth)
        else ssShared.writeHtml(hb.newLine())

      }
    }

    if (trackingDepth >= minScenarioStackDepth) {
      val tweaksToWrite = hb.tweaksToWrite(expandedTweaks)
      if (!hb.cfg.scenarioStackSmart || tweaksToWrite.nonEmpty) {
        hb.namedGroup("ScenarioStack") {
          // Add SS header
          hb.buildLeaf(NoStyleNamed(if (isRoot) "RootScenarioStack" else "ScenarioStackHeader")) { sb =>
            sb ++= getClass.getSimpleName ++= ":" ++= trackingDepth.toString
            sb ++= "@" ++= flagsAsString ++= "@" ++= System.identityHashCode(this).toHexString
          }

          // Add SS profile
          if (givenBlockProfile ne null) hb.namedGroup("ScenarioProfile") {
            hb += "(" ++= Link(givenBlockProfile.source, givenBlockProfile.source) ++= ")"
          }

          // Print the stack
          hb += this._cacheID.tweakMaskString
          hb.squareBracketsIndent {
            if (!ScenarioStack.isSyntheticScenario(this)) {
              if (hb.cfg.scenarioStackEffective || !_cacheID.isEmpty)
                tweaksToWrite.foreach(_.writeHtml(hb).newLine()) // consider faking a Scenario to write this
              else if (scenario == null)
                hb ++= "null"
              else if (!scenario.isEmptyShallow)
                scenario.writeHtml(hb, printNested = false) // doesn't work properly if twkId doesn't match infectionSet
            } else {
              hb ++= "Empty SS"
            }

            writeParent()
          }
        }
      } else
        writeParent()
    }
    hb
  }

  def env: RuntimeEnvironment = if (ssShared ne null) ssShared.environment else null
  def rootScenarioStack: ScenarioStack = ssShared.scenarioStack
  def siRoot: ScenarioStack = ssShared.siStack
  def asSiStack: ScenarioStack = asSiStack(null)
  def isRoot: Boolean = _parent eq null

  def batchScope: BatchScope = siParams.batchScope
  def cancelScope: CancellationScope = siParams.cancelScope
  def progressReporter: ProgressReporter = siParams.progressReporter
  def progressTracker: ProgressTracker = siParams.progressTracker
  def profileBlockID: Int = siParams.profileBlockID
  def trackingNodeID: ChainedID = siParams.trackingNodeID
  def parent: ScenarioStack = _parent
  def topScenario: Scenario = scenario
  def givenBlockProfile: GivenBlockProfile = if (pss eq null) null else pss.givenProfile
  def jobConfiguration: JobConfiguration = siParams.jobConfiguration
  def requireJobConfiguration[T <: JobConfiguration]: T = jobConfiguration.asInstanceOf[T]
  def tweakableListener: TweakableListener = _tweakableListener
  def ignoreSyncStacks: Boolean = (flags & EvaluationState.IGNORE_SYNC_STACKS) != 0
  def failOnSyncStacks: Boolean = (flags & EvaluationState.FAIL_ON_SYNC_STACKS) != 0
  def auditorCallbacksDisabled: Boolean = (flags & EvaluationState.AUDITOR_CALLBACKS_DISABLED) != 0
  def cachedTransitively: Boolean = (flags & EvaluationState.CACHED_TRANSITIVELY) != 0
  def isScenarioIndependent: Boolean = (flags & EvaluationState.SCENARIO_INDEPENDENT_STACK) != 0
  def isSelfOrAncestorScenarioIndependent: Boolean =
    (flags & EvaluationState.SCENARIO_INDEPENDENT_SELF_OR_ANCESTOR) != 0

  /**
   * Return true for a scenario stack that records tweaks for all property nodes, such as Dependency Tracker scenario
   * stacks.
   */
  def isTrackingIndividualTweakUsage: Boolean = (flags & EvaluationState.TRACK_TWEAK_USAGE_PER_NODE) != 0

  /** Return true for stacks that introduced NonTransparentForCaching plugin tag and the immediate child */
  def isTransparentForCaching: Boolean = (flags & EvaluationState.NOT_TRANSPARENT_FOR_CACHING) == 0

  /** Return true for a scenario stack that records tweaks as part of XS evaluation. */
  def isRecordingTweakUsage: Boolean = (flags & EvaluationState.RECORD_TWEAK_USAGE) != 0

  /** Returns true for XS and Dependency Tracker scenarios. */
  def isTrackingOrRecordingTweakUsage: Boolean = (flags & EvaluationState.TRACK_OR_RECORD_TWEAKS) != 0

  /** Returns true for XS when when-node tracking is enabled */
  def isRecordingWhenDependencies: Boolean = (flags & EvaluationState.RECORD_WHEN_DEPENDENCIES) != 0

  /** Returns true in some special cases where not safe to wait for XS node to complete. See TweakNode#cloneWith */
  def noWaitForXSNode: Boolean = (flags & EvaluationState.NO_WAIT_FOR_XS_NODE) != 0

  def isConstant: Boolean = (flags & EvaluationState.CONSTANT) != 0

  /**
   * ****************** BEGIN Scenario manipulation ****************************************
   */
  /**
   * @param entryNode
   *   Entry node for creating children
   * @return
   *   Original ScenarioStack with root pointing to that of the RuntimeEnvironment
   */
  def withNewRoot(env: RuntimeEnvironment, entryNode: AnyRef): ScenarioStack = {
    val ssm = ScenarioStackMoniker(this)
    ssm.restoreOnEnv(env)
  }

  /**
   * Create a new ScenarioStack by adding a new scenario to the top of this one. The result may not be a unique object;
   * it may be a previously-created ScenarioStack that was cached, and is referenced by another thread.
   *
   * @param scenario
   *   Scenario to use as the top of the child stack.
   * @param entryNode
   *   A location marker for profiling.
   * @return
   *   The child ScenarioStack with scenario as its top, which may be a cached previously-created stack.
   */
  def createChild(scenario: Scenario, entryNode: AnyRef): ScenarioStack =
    createChild(scenario, FrozenNodeInputMap.empty, 0, entryNode)

  /**
   * Create a new ScenarioStack by adding a new scenario to the top of this one. The result may not be a unique object;
   * it may be a previously-created ScenarioStack that was cached, and is referenced by another thread.
   *
   * @param scenario
   *   Scenario to use as the top of the child stack - may be nested
   * @param nodeInputs
   *   The node inputs to add to the child scenario stack.
   * @param profMark
   *   A location marker for profiling.
   * @return
   *   The child ScenarioStack with scenario as its top, which may be a cached previously-created stack.
   */
  def createChild(
      scenario: Scenario,
      nodeInputs: FrozenNodeInputMap,
      extraFlags: Int,
      profMark: AnyRef,
      squashIfNoContent: Boolean = true,
      name: String = null
  ): ScenarioStack = {
    var r = createChildInternal(scenario.withoutNested, nodeInputs, extraFlags, profMark, squashIfNoContent, name)
    var ns = scenario.nestedScenarios
    while (ns.nonEmpty) {
      // n.b. we added the plugin tag kvs already to the first ss so they are already inherited by this one
      r = r.createChildInternal(
        ns.head.withoutNested,
        FrozenNodeInputMap.empty,
        extraFlags,
        profMark,
        squashIfNoContent,
        name)
      ns = ns.tail
    }
    r
  }

  /**
   * Create a new ScenarioStack by adding a new scenario to the top of this one. This particular method figures out
   * whether it can cache the scenario stack creation, and whether there was already a cached stack that can be used. It
   * can also add new plugin tags to the child scenario stack.
   *
   * @param scenario
   *   Scenario to use as the top of the child stack - must be non-nested
   * @param frozen
   *   The plugin tags to add to the child scenario stack.
   * @param entryNodeForProfiling
   *   A location marker for profiling.
   * @return
   *   The child ScenarioStack with scenario as its top, which may be a cached previously-created stack.
   */
  private[optimus] def createChildInternal(
      scenario: Scenario,
      frozen: FrozenNodeInputMap,
      extraFlags: Int,
      entryNodeForProfiling: AnyRef,
      squashIfNoContent: Boolean = true,
      name: String = null
  ): ScenarioStack = {
    if (scenario.isEmptyShallow && squashIfNoContent) {
      if (frozen.isEmpty) this
      else initChild(scenario, frozen, extraFlags, _tweakableListener, _cacheID, name)
    } else {
      val expandedScenario = if (scenario.hasUnresolvedOrMarkerTweaks) {
        TweakExpander.expandTweaks(scenario, this)
      } else scenario

      val cacheKey = new SSCacheKey(_cacheID.id, expandedScenario)
      val freshCacheID = SSCacheID.newUninitialized(expandedScenario)
      val cacheID = CacheIDs.putIfAbsent(cacheKey, freshCacheID)
      // tweaks effecting values (aka RT) are the same but other flags are not.
      // create a new child sharing the cacheID of the one we got from cache
      val ss = initChild(scenario, frozen, extraFlags, _tweakableListener, cacheID, name)
      // the line above will cause expansion of tweaks if needed

      if (NodeTrace.profileSSUsage.getValue)
        Profiler.t_sstack_usage(ss, entryNodeForProfiling, cacheID ne freshCacheID)
      ss
    }
  }

  private[optimus] def createChildForTrackingScenario(
      name: String,
      tweakableListener: TweakableListener,
      cacheId: SSCacheID,
      cacheable: Boolean,
      nodeInputMap: MutableNodeInputMap): ScenarioStack = {
    val tsFlags = EvaluationState.flagsForMutableScenarioStack(flags)
    copy(
      name = name,
      flags = if (cacheable) tsFlags else tsFlags & ~EvaluationState.CACHED_TRANSITIVELY,
      _parent = this,
      scenario = Scenario.empty,
      _tweakableListener = tweakableListener,
      _cacheID = cacheId,
      _stableSS = null,
      trackingDepth = trackingDepth + 2,
      siParams = siParams.copy(nodeInputs = nodeInputMap)
    )
  }

  /**
   * Initialize a new scenario stack from the current one, along with essential data. This acts as a sort of constructor
   * for scenario stacks.
   *
   * @param scenario
   *   The new top scenario.
   * @param nodeInputs
   *   A set of plugin tags to add to the new stack.
   * @param tweakableListener
   *   The parent for tracking scenarios.
   * @param cacheID
   *   The cache ID.
   */
  private[optimus] def initChild(
      scenario: Scenario,
      nodeInputs: FrozenNodeInputMap,
      extraFlags: Int,
      tweakableListener: TweakableListener,
      cacheID: SSCacheID,
      name: String): ScenarioStack = {
    val newFlags = flags & ~EvaluationState.clearForChild(flags)
    val inputs = siParams.nodeInputs
    val newInputs = inputs.freeze.mergeWith(nodeInputs)
    val newSiParams = if (newInputs ne inputs) siParams.copy(nodeInputs = newInputs) else siParams
    copy(
      flags = newFlags | extraFlags,
      _parent = this,
      trackingDepth = trackingDepth + 1,
      scenario = scenario,
      _tweakableListener = tweakableListener,
      _cacheID = cacheID,
      _stableSS = null,
      siParams = newSiParams,
      // don't share profile with parent stack
      pss = null,
      name = if (name ne null) name else this.name
    )
  }

  /** Used only by profiling tools */
  private[optimus] def withNewProfile(): ScenarioStack = copy(pss = new PScenarioStack(pss))

  private[optimus] def withFlag(flag: Int): ScenarioStack = copy(flags = flags | flag)

  private[optimus] def hasFlag(flag: Int): Boolean = (flags & flag) != 0

  def withoutProgressTracker(): ScenarioStack = copy(siParams = siParams.copy(progressTracker = null))

  private[optimus] def seqOpId: Long = siParams.seqOpID

  /**
   * Create identical scenario stack with IGNORE_SYNC_STACKS
   */
  private[optimus] def withIgnoreSyncStack: ScenarioStack =
    if (ignoreSyncStacks) this else withFlag(EvaluationState.IGNORE_SYNC_STACKS)

  /**
   * Create identical scenario stack with FAIL_ON_SYNC_STACKS
   */
  private[optimus] def withFailOnSyncStack: ScenarioStack = withFlag(EvaluationState.FAIL_ON_SYNC_STACKS)

  /**
   * In order for the auditor not to audit itself, we mark this scenario and children with AUDITOR_CALLBACKS_DISABLED
   */
  private[optimus] def withAuditorCallbacksDisabled: ScenarioStack =
    if (!auditorCallbacksDisabled) {
      // Don't want regular graph to re-use these values and avoid being audited
      val newCacheId =
        if (!Settings.allowCachingWithAuditorCallbackDisabled) _cacheID.dup
        else _cacheID
      copy(_cacheID = newCacheId, _stableSS = null, flags = flags | EvaluationState.AUDITOR_CALLBACKS_DISABLED)
    } else this

  private[optimus] def withTrackDependencies(collector: TrackDependencyCollector[_]): ScenarioStack =
    copy(
      _parent = this,
      flags = flags | EvaluationState.RECORD_TWEAK_USAGE,
      trackingDepth = trackingDepth + 1,
      scenario = Scenario.empty,
      _tweakableListener = new CollectingTweakableListener(tweakableListener, trackingDepth, collector),
      _cacheID = SSCacheID.newUnique(), // Do NOT share cache!
      _stableSS = null
    )

  private[optimus] def withRecording(
      listener: RecordingTweakableListener,
      noWaitForXs: Boolean,
      extraFlags: Int,
      cacheID: SSCacheID = SSCacheID.newUnique() /* don't share cache by default */ ): ScenarioStack =
    copy(
      _parent = this,
      _tweakableListener = listener,
      trackingDepth = trackingDepth + 1,
      scenario = Scenario.empty,
      _cacheID = cacheID,
      _stableSS = null,
      flags = EvaluationState.flagsForRecordingScenarioStack(flags | extraFlags, noWaitForXs)
    )

  /** snapshot overlay */
  private[optimus] def withOverlay(
      overlay: Scenario,
      frozen: FrozenNodeInputMap,
      ref: ScenarioReference): ScenarioStack = {
    createChild(overlay, frozen, 0, this, name = ref.name)
  }

  private[optimus] def fromScenarioAndSnapshot(scen: Scenario, sss: SnapshotScenarioStack): ScenarioStack =
    createChild(scen, sss.nodeInputs.freeze, 0, this, name = sss.ref.name)

  /** tracking overlay */
  private[optimus] def withOverlay(overlayScenarioRef: ScenarioReference, overlaySS: ScenarioStack): ScenarioStack = {
    // cacheKey for TOSS is just tuple of parent scenarioStack ID and overlaySS ID
    // and therefore it is not dependent on the state of tweaks in the dependency trackers,
    // only on any tweaks in given blocks (and we rely on invalidation for cache coherency)
    val overlayCacheID = {
      val overlayCacheID = overlaySS._cacheID
      val cacheKey = TOSSCacheKey(_cacheID.id, overlayCacheID.id)
      CacheIDs.putIfAbsent(cacheKey, SSCacheID.newForwarding(overlayCacheID))
    }

    // format: off
    //
    // Optimization: We can skip reporting to our parent if it is an overlay representing the parent of our overlay, i.e.
    //
    //      tweakableListener.forwardToListener
    //        ^
    //        |
    //      tweakableListener ------overlay--> overlaySS.parent.tweakableListener
    //        ^                         ^
    //        |                         |
    //      nss.tweakableListener --overlay--> overlaySS.tweakableListener
    //
    // It's safe to only report to parent.forwardToListener (which is somewhere in the parent's ancestry) because we have
    // already reported to overlaySS, and overlaySS will receive invalidation notifications from overlaySS.parent already,
    // so we don't need parent to report to overlaySS.parent.
    //
    // Note that it's not true if our parent is an overlay of some unrelated scenario, because in that case our overlaySS
    // won't be receiving any invalidation notifications, so we need our parent to report to that unrelated scenario, i.e.
    //
    //      tweakableListener.forwardToListener
    //        ^
    //        |
    //      tweakableListener ------overlay--> someUnrelatedScenario.tweakableListener
    //        ^
    //        |
    //      nss.tweakableListener --overlay--> overlaySS.tweakableListener
    //
    // That's why we have the if-guard in the case.
    //
    // format: on
    val forwardToListener: TweakableListener = tweakableListener match {
      case totl: TemporaryOverlayTweakableListener
          if totl.overlayTweakableListener eq overlaySS.parent.tweakableListener =>
        totl.forwardToListener
      case t => t
    }

    val overlayInputs = siParams.nodeInputs.freeze.mergeWith(overlaySS.siParams.nodeInputs.freeze)
    val overlayListener = new TemporaryOverlayTweakableListener(
      originalTweakableListener = tweakableListener,
      overlayTweakableListener = overlaySS.tweakableListener,
      forwardToListener = forwardToListener,
      trackingDepth = trackingDepth + 1,
      overlayTrackingDepth = overlaySS.trackingDepth
    )

    copy(
      _parent = this,
      flags = EvaluationState.flagsForMutableScenarioStack(flags),
      scenario = Scenario.empty,
      name = overlayScenarioRef.name,
      trackingDepth = trackingDepth + 1,
      siParams = siParams.copy(nodeInputs = overlayInputs),
      _tweakableListener = overlayListener,
      _cacheID = overlayCacheID,
      _stableSS = null
    )
  }

  /**
   * Used only by tracking scenario to create a copy of itself to be used for general handlers
   */
  private[optimus] def withoutCacheableTransitively = {
    if (cachedTransitively)
      copy(flags = flags ^ EvaluationState.CACHED_TRANSITIVELY)
    else this
  }

  /**
   * Create identical scenario stack with cachedTransitively It's a very common case to start with no-caching scenario
   * and 'upgrade' to caching scenario.... Therefore we effectively cache that value
   */
  private[optimus] def withCacheableTransitively: ScenarioStack = {
    def canUseCached = {
      val ss = _stableSS
      ss.cachedTransitively && (ss.siParams eq siParams) && (ss.privateCache eq privateCache)
    }

    if (cachedTransitively) this
    else if (canUseCached)
      _stableSS
    else
      this.synchronized {
        if (canUseCached) _stableSS
        else {
          // Note: we set _stableSS to null even though cacheID is not changed.
          // When the SS's CACHED_TRANSITIVELY flag is turned on, _stableSS should start pointing to that SS.
          _stableSS = copy(flags = flags | EvaluationState.CACHED_TRANSITIVELY, _stableSS = null)
          _stableSS
        }
      }
  }

  private[optimus] def withoutPrivateCache: ScenarioStack = {
    if (privateCache eq null) this
    else copy(privateCache = null)
  }

  private[optimus] def withPrivateProvidedCache(cache: UNodeCache, readFromOutside: Boolean): ScenarioStack = {
    val parent = if (readFromOutside) {
      if (privateCache eq null) PrivateCache.GlobalCaches
      else privateCache
    } else PrivateCache.NoOtherCache
    copy(privateCache = new PrivateCache(cache, parent))
  }

  /** Create identical scenario stack with new BatchScope */
  def withBatchScope(bs: BatchScope): ScenarioStack =
    copy(siParams = siParams.copy(batchScope = bs))

  /** Note: check for same scheduler happens at the call site */
  def withScheduler(scheduler: WeakReference[Scheduler]): ScenarioStack =
    copy(siParams = siParams.copy(scheduler = scheduler), _stableSS = null)

  /** Create (optional) identical scenario stack with matching CS and profileBlockID */
  def withCancellationScopeAndBlockID(ss: ScenarioStack): ScenarioStack = {
    if ((ss.cancelScope eq cancelScope) && ss.profileBlockID == profileBlockID) this
    else copy(siParams = siParams.copy(cancelScope = ss.cancelScope, profileBlockID = ss.profileBlockID))
  }

  /**
   * Create identical scenario stack with new CancellationScope parented by both cs and the current cancellation scope,
   * so that cancellations cs and from up the stack propagate.
   */
  def withCancellationScope(cs: CancellationScope): ScenarioStack = {
    if (cs eq this.cancelScope) this
    else withCancellationScopeRaw(cancelScope.union(cs))
  }

  /**
   * Create identical scenario stack with new CancellationScope parented by the current one
   */
  def withChildCancellationScope(): ScenarioStack = {
    // no need to register the child with the current cancel scope because it's already registered when created
    withCancellationScopeRaw(cancelScope.childScope())
  }

  def withProgressTracker(params: ProgressTrackerParams): ScenarioStack = {
    val tracker =
      if (progressTracker eq null) ProgressTracker.newTracker(params)
      else progressTracker.childTracker(params)

    copy(siParams = siParams.copy(progressTracker = tracker))
  }

  def withProgressTracker(tracker: ProgressTracker): ScenarioStack = {
    if (this.progressTracker eq tracker) this
    else copy(siParams = siParams.copy(progressTracker = tracker))
  }

  private[optimus] def maybeWithProgressTracker(
      tracker: ProgressTracker,
      trackerParams: ProgressTrackerParams): ScenarioStack =
    if (tracker eq null) this
    else {
      val trackerToAttach = if (trackerParams eq null) tracker else tracker.childTracker(trackerParams)
      withProgressTracker(trackerToAttach)
    }

  /** replaces cancellation scope without checks, you probably know what you are doing if you are using this API */
  private[optimus] def withCancellationScopeRaw(cs: CancellationScope): ScenarioStack = {
    if (cs eq this.cancelScope) this
    // Note: cacheID is the same so that we can share computed result
    else copy(siParams = siParams.copy(cancelScope = cs))
  }

  def withBlockID(blk: Int): ScenarioStack =
    if (blk == this.profileBlockID) this else copy(siParams = siParams.copy(profileBlockID = blk))

  def hasPluginTag(tag: PluginTagKey[_]): Boolean =
    siParams.nodeInputs.contains(tag)

  def pluginTags: collection.Seq[PluginTagKeyValue[_]] =
    siParams.nodeInputs.freeze.keyValuePairs
      .filter { kv => kv.nodeInput.isInstanceOf[PluginTagKey[_]] }
      .map(kv => PluginTagKeyValue(kv.nodeInput.asInstanceOf[PluginTagKey[Any]], kv.value))

  /**
   * Create sibling scenario stack with new or updated plugin tags
   */
  def withPluginTags(kvs: collection.Seq[PluginTagKeyValue[_]]): ScenarioStack = {
    if (kvs.isEmpty) this
    else {
      // noinspection ComparingUnrelatedTypes (if PluginTag extends this trait, set flag)
      val flag =
        if (kvs.exists(_.isInstanceOf[NotTransparentForCaching])) EvaluationState.NOT_TRANSPARENT_FOR_CACHING_BARRIER
        else 0
      copy(siParams = siParams.copy(nodeInputs = siParams.nodeInputs.freeze.withExtraInputs(kvs)), flags = flags | flag)
    }
  }

  private[optimus] def withScopedNodeInput[T](ni: ScopedSINodeInput[T], v: T) =
    copy(siParams = siParams.copy(nodeInputs = siParams.nodeInputs.freeze.withExtraInput(ni, v)))

  private[platform] def withFullySpecifiedScopedNodeInputMap[T](scopedMap: FrozenNodeInputMap) =
    copy(siParams = siParams.copy(nodeInputs = scopedMap))

  def withProgressReporter(pr: ProgressReporter): ScenarioStack = {
    if ((pr eq UnspecifiedProgressReporter) || (pr eq this.progressReporter)) this
    else copy(siParams = siParams.copy(progressReporter = pr))
  }

  /**
   * Create sibling scenario stack with additional flag and new or updated plugin tag One way to optimize tag discovery
   * is to also set a flag (aka tag present) flags are limited so should use it only in high traffic areas also if the
   * tag is transient then on dist it should be removed as well
   */
  private[optimus] def withPluginTag[T](extraFlag: Int, key: PluginTagKey[T], tag: T): ScenarioStack = {
    val needNewPluginTags = !findPluginTag(key).contains(tag)
    val flag =
      if (needNewPluginTags && key.isInstanceOf[NotTransparentForCaching])
        EvaluationState.NOT_TRANSPARENT_FOR_CACHING_BARRIER
      else 0
    val newFlags = this.flags | extraFlag | flag
    if (needNewPluginTags)
      copy(siParams = siParams.copy(nodeInputs = siParams.nodeInputs.freeze.withExtraInput(key, tag)), flags = newFlags)
    else if (flags != newFlags) copy(flags = newFlags)
    else this
  }

  /** Create sibling scenario stack with new or updated plugin tag */
  def withPluginTag[T](key: PluginTagKey[T], tag: T): ScenarioStack = withPluginTag(0, key, tag)

  def withoutPluginTag(key: PluginTagKey[_]): ScenarioStack = {
    val nss = copy(siParams = siParams.copy(nodeInputs = siParams.nodeInputs.freeze.withoutInput(key)))
    nss
  }

  def withTrackingNodeId(id: ChainedID, parentId: ChainedID = siParams.parentTrackingNodeID): ScenarioStack =
    copy(siParams = siParams.copy(trackingNodeID = id, parentTrackingNodeID = parentId))

  /**
   * creates a new ScenarioStack with the scenario from this, but the non-scenario properties from other
   */
  def withNonScenarioPropertiesFrom(other: ScenarioStack, clearFlags: Int = 0): ScenarioStack =
    copy(flags = other.flags & ~clearFlags, siParams = other.siParams)

  def withJobConfiguration(jobConfiguration: JobConfiguration): ScenarioStack =
    withSIParams(siParams.copy(jobConfiguration = jobConfiguration))

  def withSIParamsFrom(other: ScenarioStack): ScenarioStack = withSIParams(other.siParams)

  private[optimus] def withSIParams(otherSiParams: SIParams): ScenarioStack = {
    // (quick check with eq)
    if (siParams eq otherSiParams) this
    else copy(siParams = otherSiParams)
  }

  /**
   * Look up the plugin tag in the scenario stack. Nested scenarios hide the parent tags.
   */
  def findPluginTag[T](k: PluginTagKey[T]): Option[T] = siParams.nodeInputs.getInput(k)

  def ensureIgnoreSyncStack: ScenarioStack = if (ignoreSyncStacks) this else withIgnoreSyncStack

  /**
   * ****************** END Scenario manipulation ******************************************
   */
  /** Create identical scenario stack with tracking node */
  def withTrackingNode: ScenarioStack = withChildTrackingNode(getTrackingNodeID.child)

  /** Create identical scenario stack with tracking node */
  def withChildTrackingNode(id: ChainedID): ScenarioStack =
    copy(
      siParams = siParams.copy(parentTrackingNodeID = siParams.trackingNodeID, trackingNodeID = id),
      _stableSS = null // Don't lose trackingID because we need it for proper across task cache reuse attribution
    )

  def getTrackingNodeID: ChainedID = {
    // true clause should be much more common, though it is theoretically possible for someone to
    // create an SS with a null ID, so we need to keep the stack climbing in defensively
    if (trackingNodeID ne null) trackingNodeID
    else {
      var cur = this
      while ((cur ne null) && !cur.isRoot && (cur.trackingNodeID eq null)) {
        cur = cur.parent
      }
      // You'd think parent==null while isRoot==false wouldn't be possible, but it seems to be true for ConstantSS
      if (cur eq null)
        ChainedID.root
      else if (cur.trackingNodeID eq null) {
        if (cur.ssShared ne null) {
          val config = cur.ssShared.environment.config
          // Because DSIRuntimeEnvironment tends to lack config, and mock runtimes lack rootID
          if ((config ne null) && (config.runtimeConfig.rootID ne null)) config.runtimeConfig.rootID
          else ChainedID.root
        } else ChainedID.root
      } else
        cur.trackingNodeID
    }
  }

  def getParentTrackingNode: ChainedID = {
    // Shouldn't happen unless someone overrode ScenarioStack incorrectly, as has happened.
    if (siParams.parentTrackingNodeID ne null) siParams.parentTrackingNodeID
    else {
      // First find active tracking id
      var cur = this
      while ((cur ne null) && !cur.isRoot && (cur.trackingNodeID eq null)) {
        cur = cur.parent
      }
      // You'd think parent==null while isRoot==false wouldn't be possible, but it seems to be true for ConstantSS
      if (cur eq null)
        ChainedID.root
      else if (cur.trackingNodeID eq null) // we're already at the top
        cur.ssShared.environment.config.runtimeConfig.rootID
      else {
        val id = cur.trackingNodeID
        // Search up the stack for a different tracking id
        while ((cur ne null) && !cur.isRoot && ((cur.trackingNodeID eq null) || cur.trackingNodeID == id)) {
          cur = cur.parent
        }
        if (cur eq null)
          ChainedID.root
        else if (cur.trackingNodeID eq null) // we're already at the top
          cur.ssShared.environment.config.runtimeConfig.rootID
        else
          cur.trackingNodeID
      }
    }
  }

  /** Creates TwkResolver if tweak is possible or null otherwise * */
  private def getTweakResolver[R](requesting: NodeTask, info: NodeTaskInfo, key: NodeKey[R]): TwkResolver[R] = {
    if (info.wasTweakedAtLeastOnce) {
      new TwkResolver(requesting, info, key, startSS = this)
    } else if (isSelfOrAncestorScenarioIndependent) {
      // If the node was wasTweakedAtLeastOnce then the TwkResolver will take care of SI checking, but if not we must
      // check it here. If we or any of our ancestors are SI then we immediately know that this tweakble lookup is
      // illegal (we don't need to worry about the legal case where the tweakable was tweaked somewhere inside the
      // SI stack, because we know it was never tweaked).
      throw new IllegalScenarioDependenceException(key, requesting)
    } else null
  }

  /** Called from NCSuport#matchXscenario that supplies infoTarget */
  private[optimus] def getTweakAndScenarioStack(info: NodeTaskInfo, key: NodeKey[_], infoTarget: NodeTask) = {
    val tr = getTweakResolver(infoTarget, info, key)
    if (tr ne null) { tr.infoTarget = infoTarget; tr.syncResolve(); (tr.tweak, tr.cur) }
    else null
  }

  /** Called from NCSuport#hasTweaksForTweakables that supplies infoTarget and getTweak() below */
  def getTweak(info: NodeTaskInfo, key: NodeKey[_], infoTarget: NodeTask): Tweak = {
    val tr = getTweakResolver(infoTarget, info, key)
    if (tr ne null) { tr.infoTarget = infoTarget; tr.syncResolve().tweak }
    else null
  }

  def getTweak(info: NodeTaskInfo, key: NodeKey[_]): Tweak = getTweak(info, key, null)

  /** Last step in the resolution of a PropertyNode that didn't end up being tweaked */
  private[optimus] def completeGetNode[R](info: NodeTaskInfo, pnode: PropertyNode[R], ec: OGSchedulerContext) =
    if (info.getCacheable && !pnode.isStable) NodeCache.lookupAndInsert(info, pnode, privateCache, ec)
    else pnode

  private[optimus] def initialRuntimeScenarioStack: ScenarioStack = ssShared.scenarioStackWithInitialTime

  private[optimus] def initialRuntimeScenarioStack(requeryTime: Boolean): ScenarioStack =
    ssShared.scenarioStackWithInitialTime(requeryTime)

  private[optimus] def initialRuntimeScenarioStack(initialTime: Instant): ScenarioStack =
    env.initialRuntimeScenarioStack(this, initialTime)

  /** DO NOT USE THIS FUNCTION FOR ANYTHING BUT TEST */
  def getNode[R](key: NodeKey[R]): Node[R] =
    getNode(key.asInstanceOf[PropertyNode[R]], OGSchedulerContext.current())

  /**
   * Given a template node (i.e. scenarioStack == null):
   *   1. If there is a matching node in the cache, returns that node.
   *   1. Otherwise, if there is a tweak that matches the template, generates a node from that tweak, caches it, and
   *      returns it.
   *   1. Otherwise, caches and returns the template node.
   *
   * Given a node (i.e. scenarioStack != null)
   *   1. Tweak is looked up and returned otherwise the input node is returned The case here is essentially for the node
   *      that has a fixed value (a.k.a. scenario independent) except that can be tweaked e.g. LazyPickledReference and
   *      AlreadyCompletedPropertyNode
   *
   * Note that the scenarioStack of the returned node is not necessarily 'this'. e.g. for a TweakNode, it will be a
   * parent ScenarioStack.
   *
   * @param key
   *   Used as both the NodeKey to lookup, and the ''untweaked'' base node that would be returned if there are no
   *   scenarios in the stack.
   * @return
   *   The `Node` that can be evaluated to get the result for this `ScenarioStack`.
   */
  def getNode[R](key: PropertyNode[R], ec: OGSchedulerContext): Node[R] = {
    if (isConstant)
      throw new GraphInInvalidState("Nodes cannot be looked up in 'constant' scenario stack")

    if (Settings.schedulerAsserts) {
      if (key.isInstanceOf[TweakNode[_]]) throw new GraphInInvalidState(s"TweakNode as key: $key")
      if (key.info() ne null) throw new GraphInInvalidState(s"Re-used node as key: $key")
      if (key.propertyInfo.isDirectlyTweakable && !key.isTrackingValue)
        throw new GraphInInvalidState(s"Tweakable node is not tracking value: $key")
    }

    // Could use executionInfo, Nodes going via getNode they should implemented as def executionInfo = propertyInfo
    val info = key.propertyInfo

    val ssAdjusted = info.adjustScenarioStack(key, this)
    val needToMarkCached = info.getCacheable && !cachedTransitively
    val ss = if (needToMarkCached) ssAdjusted.withCacheableTransitively else ssAdjusted

    // Check for tweaks
    val tweakResolver = if (info.isDirectlyTweakable) { // [PERF_AVOID_DOUBLE_CHECKING_WITH_TWEAKEDATLEASTONCE]
      val requestingNode = ec.getCurrentNodeTask
      // Why is this OK even if propertyInfo.executionInfo.isScenarioIndependent? [SEE_AUDIT_TRACE_SI]
      requestingNode.markScenarioDependent() // Could be looking up tweaks, that makes current node "scenario dependent"
      getTweakResolver(requestingNode, info, key)
    } else null

    // Note: tracing and debugging are injected as suffix [SEE_verifyScenarioStackGetNode]
    if (tweakResolver ne null)
      tweakResolver.startAsyncResolve(ec, ss) // Returns itself (tweakResolver) or resolved tweak or value node
    else
      completeGetNode(info, key.prepareForExecutionIn(ss), ec)
  }

  private[optimus] def setParent(p: ScenarioStack): Unit = {
    _parent = p
  }

  // noinspection ScalaUnusedSymbol
  private def writeReplace(): AnyRef = graph.ScenarioStackMoniker(this)

  /** Returns true if this scenario stack (recursively) may have a tweak given a hash */
  private[optimus] def mayDependOnTweak(hash: NodeHash): Boolean = {
    var r = false
    var cur = this
    while ((cur ne null) && !cur.isRoot) {
      if (cur._cacheID.mayContainTweakAffecting(hash)) {
        r = true
        cur = null // break
      } else cur = cur._parent
    }
    r
  }

  /* All expanded tweaks, don't use if you care about performance */
  private[optimus] def expandedTweaks: Iterable[Tweak] = _cacheID.allTweaksAsIterable

  /**
   * Create a basic (i.e. non-tracking) copy of the current scenario stack by recursing up the first basic parent and
   * setting the parent to ourselves
   *
   * There must be at least one basic scenario on the stack, so this is guaranteed to terminate.
   */
  private[optimus] def asBasicScenarioStack: ScenarioStack =
    if (!isTrackingOrRecordingTweakUsage) this // This is already a basic ScenarioStack
    else asBasicScenarioStackWithParent(parent.asBasicScenarioStack, EvaluationState.TRACK_OR_RECORD_TWEAKS)

  /** Used for debugging scenario stacks only */
  private[optimus] def asVeryBasicScenarioStack: ScenarioStack = {
    if (!isTrackingIndividualTweakUsage && !isSelfOrAncestorScenarioIndependent)
      this // This is already a very basic ScenarioStack
    else asBasicScenarioStackWithParent(parent.asVeryBasicScenarioStack, EvaluationState.NON_BASIC_STACK)
  }

  private def asBasicScenarioStackWithParent(parentScenarioStack: ScenarioStack, flagsToClear: Int): ScenarioStack =
    copy(
      _parent = parentScenarioStack,
      flags = flags & ~flagsToClear,
      scenario = Scenario.empty,
      // Usually just root (except in some edge cases where trackDependencies is used)
      _tweakableListener = parentScenarioStack.tweakableListener,
      _cacheID = _cacheID.dup,
      _stableSS = null
    )

  /**
   * Called by a DelayedXSProxyNode to cleanup/freeze the tweakable listener of the XSOriginal node
   *
   * Notes:
   *  - No need to retain full tweaks (i.e. ScenarioStack), some might not even be used and could be large so reset
   *    _parent = root
   *      - The relevant tweakables are all in 'rt' which are 'Recorded' (i.e. frozen)
   *      - Keeping original ScenarioStack could be misleading if _parent is TrackingScenarioStack and keeps on mutating
   *  - Invariant: Only proxies are listeners to XSOriginal, everything else sees the proxy
   *  - Multiple proxies can wait on a single XSOriginal and will call this function which appears like a race...
   *    - But it's a benign race, because it will be settings the same value
   */
  private[optimus] def onXSOriginalCompleted(rt: RecordedTweakables): Unit = {
    if (Settings.schedulerAsserts) {
      if (!isRecordingTweakUsage)
        throw new GraphInInvalidState("Attempted to setRecordedTweakables on a non-recording ScenarioStack")
      if (_tweakableListener.isInstanceOf[RecordedTweakables] && (rt != _tweakableListener))
        throw new GraphInInvalidState("Attempted to overwrite RecordedTweakables with different value")
    }
    if (_parent ne ssShared.scenarioStack) {
      _tweakableListener = rt // First so that visibility of _parent write implies visibility of this write
      _parent = ssShared.scenarioStack
      _stableSS = this // stableSS is already always equals to `this`, except that dist managed to modify stableSS
    }
  }

  private[optimus] def combineTrackData(rTweakables: RecordedTweakables, node: NodeTask): Unit = {
    var i = 0
    val tweakable = rTweakables.tweakable
    while (i < tweakable.length) {
      combineTweakableData(tweakable(i), node)
      i += 1
    }

    i = 0
    val tweakableHashes = rTweakables.tweakableHashes
    while (i < tweakableHashes.length) {
      combineTweakableData(tweakableHashes(i), node)
      i += 1
    }

    i = 0
    val tweaked = rTweakables.tweaked
    while (i < tweaked.length) {
      combineTweakData(tweaked(i), node)
      i += 1
    }
  }

  private[optimus] def combineTweakableData(tweakable: PropertyNode[_], node: NodeTask): Unit = {
    _tweakableListener.onTweakableNodeUsedBy(tweakable, node)
  }

  private[optimus] def combineTweakableData(tweakableHash: NodeHash, node: NodeTask): Unit = {
    _tweakableListener.onTweakableNodeHashUsedBy(tweakableHash, node)
  }

  private[optimus] def combineTweakData(ttn: TweakTreeNode, node: NodeTask): Unit = {
    _tweakableListener.onTweakUsedBy(ttn, node)
    // note that we don't need to report nested dependency information up if node is recording because any such
    // information was already reported up by the XS Proxy created for each TweakNode. See [XS_BY_NAME_TWEAKS]
    if ((ttn.nested ne null) && !node.scenarioStack().isRecordingTweakUsage) {
      if (ttn.tweak.evaluateIn != Tweak.evaluateInParentOfGiven) {
        val xsNode = node.nodeName.name // current favorReuse node that we read the tweak in
        val tweak = ttn.tweak // Tweak.bind tweak
        throw new GraphException(
          s"Tweak.bind in cross-scenario node $xsNode is not supported (offending tweak is $tweak)")
      }

      var ssToReportTo = this // Find the scenario that is for sure below current tweak
      if (!ssToReportTo.isRoot) {
        do {
          ssToReportTo = ssToReportTo._parent
        } while (ssToReportTo.trackingDepth >= ttn.trackingDepth)
        ssToReportTo.combineTrackData(ttn.nested, node)
      }
    }
  }

  private[optimus] def combineWhenClauseData(
      key: PropertyNode[_],
      predicate: AnyRef,
      res: Boolean,
      rt: RecordedTweakables): Unit = {
    var listener: TweakableListener = null
    var reportTo = this
    while (!reportTo.isRoot) {
      // Avoid reporting to the same listener
      if (listener ne reportTo._tweakableListener) {
        listener = reportTo._tweakableListener
        listener.reportWhenClause(key, predicate, res, rt)
      }
      reportTo = reportTo.parent
    }
  }

  /** collect all referenced ScenarioReferences between us and root (for underlay/overlay) */
  private[optimus] def scenarioReferences: mutable.HashSet[ScenarioReference] = {
    val usAndParents = new mutable.HashSet[ScenarioReference]()
    var curr = this
    while (!curr.isRoot) {
      usAndParents.add(ScenarioReference.forOverlayStack(curr))
      curr = curr.parent
    }
    usAndParents
  }

  private def scenariosUpToCommonRoot(overlayTS: DependencyTracker): ArrayBuffer[(ScenarioReference, ScenarioStack)] = {
    val presentScenarioRefs = scenarioReferences
    val overlays = new ArrayBuffer[(ScenarioReference, ScenarioStack)]()
    var currTS = overlayTS
    while (!presentScenarioRefs.contains(currTS.scenarioReference)) {
      // add the main mutable scenario stack
      overlays += ((currTS.scenarioReference, currTS.scenarioStack))
      // add all the underlays until we get to the mutable scenario of our parent dependency tracker
      var underlaySS = currTS.scenarioStack.parent
      while (underlaySS ne currTS.parent.scenarioStack) {
        overlays += ((currTS.scenarioReference, underlaySS))
        underlaySS = underlaySS.parent
      }
      currTS = currTS.parent
      if (currTS eq null)
        throw new GraphInInvalidState("Unexpectedly reached root ScenarioStack (beyond DependencyTrackerRoot)")
    }
    overlays
  }

  /**
   * walk from 'this' to parentScenarioStack, creating a Scenario with nesting reflecting the structure (for underlays)
   */
  private[optimus] def nestScenariosUpTo(parentScenarioStack: ScenarioStack): Scenario = {
    var scenario = Scenario.empty
    var curSS = this
    while (curSS ne parentScenarioStack) { // ie walk up to parent tracking scenario stack
      scenario = curSS._cacheID.toScenario.nest(scenario) // underlays applied top-down
      curSS = curSS.parent
    }
    scenario
  }

  private[optimus] def overlay(overlayRef: ScenarioReference): ScenarioStack = {
    val snapshot = SnapshotScenarioStack.current(this)
    snapshot.overlay(this, overlayRef)
  }

  // Create a TOSS for each level of the stack to be overlayed, with each one forwarding to its reporting parents
  private[optimus] def overlay(overlayTS: DependencyTracker): ScenarioStack = {
    val overlayScenarios = scenariosUpToCommonRoot(overlayTS)

    // Loop from end of list, as scenariosUpToCommonRoot starts with the overlay scenario itself (ie, the child-most scenario).
    // At the end of this loop, curr should be the TOSS generated to represent the overlay scenario, and its parent
    // is either a TOSS representing its original parent (if there were more scenarios between the overlay and the common parent)
    // or the original SS (on top of which we are overlaying overlayTS).
    // Note: check for length > 0 is in scenariosUpToCommonRoot
    var i = overlayScenarios.length - 1
    var curr = this
    while (i >= 0) {
      val (ref, overlay) = overlayScenarios(i)
      curr = curr.withOverlay(ref, overlay)
      i -= 1
    }
    curr
  }

  override def toString: String = "ScenarioStack:" + toShortString

  def toShortString: String = {
    val trackerName = if (isTrackingIndividualTweakUsage) "." + name else ""
    trackingDepth + flagsAsString + trackerName + "@" + System.identityHashCode(this).toHexString
  }

}

// this is an abstract class rather than a trait because class methods are slightly faster to invoke than interface methods
private[optimus] abstract class TweakableListener {

  /** Logically a val, but to save space it's a def */
  def respondsToInvalidationsFrom: Set[TweakableListener] = Set.empty
  def isDisposed: Boolean = false

  /**
   * Counts TTrackRefs that referred to GC-ed nodes.
   *
   * This is used as a heuristic to determine whether a DependencyTracker cleanup or can be skipped.
   */
  def refQ: RefCounter[NodeTask] = null

  /** Returns outer tracking proxy -> a node that is in the original TrackingScenario... Not in the RecordingScenario */
  def trackingProxy: NodeTask = null

  /** Callback to ScenarioStacks. Currently only TrackingScenario */
  def onTweakableNodeCompleted(node: PropertyNode[_]): Unit

  /** Callback to ScenarioStacks. TrackingScenario uses node argument and RecordingScenario doesn't */
  def onTweakableNodeUsedBy(key: PropertyNode[_], node: NodeTask): Unit

  /** Callback to ScenarioStacks. TrackingScenario uses node argument and RecordingScenario doesn't */
  def onTweakableNodeHashUsedBy(key: NodeHash, node: NodeTask): Unit

  /**
   * Callback to ScenarioStacks. ttn.trackingDepth -> to quickly filter out interest in the tweaks (Tracking &
   * Recording) ttn.key -> to identify ttrack roots (Tracking and not Recording) ttn.node -> to attach ttracks (Tracking
   * and not Recording)
   */
  def onTweakUsedBy(ttn: TweakTreeNode, node: NodeTask): Unit

  /** ScenarioStacks that retain tweakable data will pass it onto node */
  def reportTweakableUseBy(node: NodeTask): Unit

  /** Listeners interested in when-clauses are given opportunity to collect that info */
  def reportWhenClause(key: PropertyNode[_], predicate: AnyRef, res: Boolean, rt: RecordedTweakables): Unit = {}

  /** Avoid annoying casts all over */
  def recordedTweakables: RecordedTweakables = null

  /** Allow TweakableListener to add flags when constructing ScenarioStack */
  def extraFlags: Int = 0

  /** If tweakablelistener has owner with type dependencyTrackerRoot, return its name */
  def dependencyTrackerRootName: String = "[not tracking]"
}

private[optimus] object NoOpTweakableListener extends TweakableListener with Serializable {
  private val moniker: AnyRef = new Serializable {
    // noinspection ScalaUnusedSymbol @Serial
    def readResolve(): AnyRef = NoOpTweakableListener
  }
  // noinspection ScalaUnusedSymbol @Serial
  private def writeReplace(): AnyRef = moniker

  // we don't respond to invalidations from anyone, not even ourselves
  override def respondsToInvalidationsFrom: Set[TweakableListener] = Set.empty
  override def onTweakableNodeCompleted(node: PropertyNode[_]): Unit = {}
  override def onTweakableNodeUsedBy(key: PropertyNode[_], node: NodeTask): Unit = {}
  override def onTweakableNodeHashUsedBy(key: NodeHash, node: NodeTask): Unit = {}
  override def onTweakUsedBy(ttn: TweakTreeNode, node: NodeTask): Unit = {}
  override def reportTweakableUseBy(node: NodeTask): Unit = {}
}

private[optimus] class NodeHash(
    val entityHash: Int,
    val argsHash: Int,
    val property: NodeTaskInfo,
    val opaqueIndex: Int)
    extends TweakableKey
    with Serializable {

  override def propertyInfo: NodeTaskInfo = property
  override def trackKey: TweakableKey = this
  override def tidyKey: TweakableKey = this

  def dbgKey: NodeKey[_] = null
  override def hashCode: Int = {
    var result = 19
    result = 31 * result + entityHash
    result = 31 * result + argsHash
    31 * result + property.hashCode()
  }
  override def equals(obj: Any): Boolean = obj match {
    case that: NodeHash => entityHash == that.entityHash && argsHash == that.argsHash && property.equals(that.property)
    case _              => false
  }
}

private[optimus] object NodeHash {
  def apply(nk: NodeKey[_], opaqueIndex: Int = -1): NodeHash = {
    if (NodeTrace.traceInvalidates.getValue)
      new NodeHash(nk.entity.hashCode(), nk.argsHash, nk.propertyInfo, opaqueIndex) {
        override def dbgKey: NodeKey[_] = nk
        override def equals(obj: scala.Any): Boolean = obj match {
          case nh: NodeHash =>
            val r = super.equals(nh)
            val nko = nh.dbgKey // node key other
            if (r && (nk ne null) && (nko ne null) && nko != nk)
              OGTrace.observer.nodeHashCollision(dbgKey.asInstanceOf[NodeTask])
            r
          case _ => false
        }
      }
    else
      new NodeHash(nk.entity.hashCode(), nk.argsHash, nk.propertyInfo, opaqueIndex)
  }
}

object RecordingScenarioStack {
  def withNewListener(
      from: ScenarioStack,
      trackingProxy: NodeTask,
      noWaitForXs: Boolean,
      earlyUpReport: Boolean): ScenarioStack = {
    val listener = new RecordingTweakableListener(null, trackingProxy, from, earlyUpReport)
    val stack = from.withRecording(listener, noWaitForXs = noWaitForXs, extraFlags = 0)
    listener.scenarioStack = stack
    stack
  }

  def withExistingListener(
      from: ScenarioStack,
      listener: RecordingTweakableListener,
      noWaitForXs: Boolean = false): ScenarioStack = {
    val stack = from.withRecording(listener, noWaitForXs = noWaitForXs, extraFlags = listener.extraFlags)
    stack
  }
}

trait FreezableNodeInputMap extends Serializable {
  def getInput[T](nodeInput: ScopedSINodeInput[T]): Option[T]
  def freeze: FrozenNodeInputMap
  def contains[T](nodeInput: ScopedSINodeInput[T]): Boolean
}

/*MutablePluginTagMap class is only used in dependency tracker*/
final class MutableNodeInputMap(
    private[this] var tagsThisLevelOnly: Map[ScopedSINodeInput[Any], NodeInputMapValue[Any]],
    parentTags: FreezableNodeInputMap)
    extends FreezableNodeInputMap
    with Cloneable {
  @volatile private[this] var cache: FrozenNodeInputMap = _
  override def contains[T](input: ScopedSINodeInput[T]): Boolean =
    tagsThisLevelOnly.contains(input.asInstanceOf[ScopedSINodeInput[Any]])
  def addInput[T](input: ScopedSINodeInput[T], newValue: T): Unit = {
    tagsThisLevelOnly.get(input.asInstanceOf[ScopedSINodeInput[Any]]) match {
      case None =>
        this.tagsThisLevelOnly += (input
          .asInstanceOf[ScopedSINodeInput[Any]] -> new NodeInputMapValue(LoaderSource.CODE, newValue))
      case Some(nodeInputMapValue) =>
        this.tagsThisLevelOnly += (input
          .asInstanceOf[ScopedSINodeInput[Any]] -> new NodeInputMapValue(
          LoaderSource.CODE,
          input.combine(
            nodeInputMapValue.source(),
            nodeInputMapValue.value().asInstanceOf[T],
            LoaderSource.CODE,
            newValue)))
    }
    this.invalidate()
  }

  override def getInput[T](input: ScopedSINodeInput[T]): Option[T] = freeze.getInput[T](input)
  def removeInput(input: ScopedSINodeInput[_]): FreezableNodeInputMap = {
    tagsThisLevelOnly -= input.asInstanceOf[ScopedSINodeInput[Any]]
    this.invalidate()
    this
  }

  def invalidate(): Unit = {
    cache = null
  }

  override def freeze: FrozenNodeInputMap = {
    var c = this.cache
    if (c eq null) synchronized {
      c = this.cache
      if (c eq null) {
        val n =
          if (tagsThisLevelOnly.isEmpty) parentTags.freeze
          else {
            val extraInputs: Seq[ScopedSIEntry[_]] = tagsThisLevelOnly.map { case (k, v) =>
              ScopedSIEntry(k, v.value())
            }.toSeq
            parentTags.freeze.withExtraInputs(extraInputs)
          }
        cache = n
        n
      } else c
    }
    else c
  }

  // noinspection ScalaUnusedSymbol @Serial
  private def writeReplace(): AnyRef = freeze
}
