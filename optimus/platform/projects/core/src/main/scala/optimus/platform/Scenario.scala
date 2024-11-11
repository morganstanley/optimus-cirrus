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

import scala.annotation.nowarn
import java.{util => ju}
import optimus.core.CoreHelpers
import optimus.core.MonitoringBreadcrumbs.sendTweaksConflictingWithScenarioUnordered
import optimus.core.log
import optimus.dist.HasDmcBinaryIdentity
import optimus.graph._
import optimus.platform.annotations.deprecating
import optimus.platform.annotations.expectingTweaks
import optimus.platform.annotations.nodeLiftByName
import optimus.platform.annotations.nodeLiftByValue
import optimus.platform.annotations.parallelizable
import optimus.platform.util.html.HtmlBuilder
import optimus.platform.util.html.NoStyle

import scala.util.hashing.MurmurHash3

/**
 * A scenario is a set of Tweaks, which are applied as a batch. The Scenario class also has awareness of its nested
 * scenarios.
 *
 * _instanceTweaks and _propertyTweaks
 *   1. For also-tweaks it's too early to check for duplicates (there could be what looks like a dup that expands to
 *      different tweaks and could be non-dup that expand to the same tweak)
 *   1. Currently storing into hashes is a waste because ScenarioStack will recreate hashes completely. We might as well
 *      (especially considering 1.) store as original Iterable[Tweak] However there is room to optimize (for a common
 *      case?) AND split also-tweaks from regular tweaks and regular tweaks hash can be re-used completely by
 *      ScenarioStack
 *
 * Note: due to the nature of nesting, nested scenarios can not have their own nested scenarios. These are flattened
 * when a scenario is nested (see 'nested' method for more information).
 *
 * allTweaks tweaks from which to construct the scenario. nestedScenarios A list of all nested scenarios, from innermost
 * to outermost.
 */
final class Scenario private[optimus] (
    private val _topLevelTweaks: Array[Tweak],
    private[optimus] val nestedScenarios: List[Scenario],
    /* Various computed states of this scenario */
    private val flags: Int)
    extends Serializable
    with HasDmcBinaryIdentity {

  def topLevelTweaks: collection.Seq[Tweak] = _topLevelTweaks

  /** true if any of the tweaks are convertible to byValue */
  private[optimus] def hasReducibleToByValueTweaks: Boolean = (flags & ScenarioFlags.hasReducibleToByValueTweaks) != 0

  private[optimus] def hasPossiblyRedundantTweaks: Boolean = (flags & ScenarioFlags.hasPossiblyRedundantTweaks) != 0

  /** true if we have unresolved tweaks (i.e. also-sets which have not been expanded, or tweak markers) */
  private[optimus] def hasUnresolvedOrMarkerTweaks: Boolean = (flags & ScenarioFlags.hasUnresolvedOrMarkerTweaks) != 0

  /** true if we have tweaks that depend on parent context */
  private[optimus] def hasContextDependentTweaks: Boolean = (flags & ScenarioFlags.hasContextDependentTweaks) != 0

  /** true to compare tweaks as set */
  private[optimus] def unorderedTweaks: Boolean = (flags & ScenarioFlags.unorderedTweaks) != 0

  private[optimus] def flagsWithoutHasRedundant: Int = flags & ~ScenarioFlags.hasPossiblyRedundantTweaks

  private[optimus] def flagsWithoutHasRedundantOrRBValue =
    (flags & ~ScenarioFlags.hasReducibleToByValueTweaks) & ~ScenarioFlags.hasPossiblyRedundantTweaks

  /** Same flags with ReducibleToByValueTweaks flag cleared */
  private[optimus] def flagsWithoutUnresolved: Int = flags & ~ScenarioFlags.hasUnresolvedOrMarkerTweaks

  private[optimus] var _createdAt: Exception = _
  private[optimus] def createdAtAsString: String =
    if (_createdAt eq null) "" else "\n" + _createdAt.getStackTrace.mkString("\n")

  def allTweaks: collection.Seq[Tweak] = {
    if (nestedScenarios.nonEmpty)
      throw new GraphException(
        "You are retrieving the tweaks from current scenario stack, but you have other tweaks inside nested scenarios. Are you sure this is what you want to do? If you are sure that this is the correct behavior please use Scenario.topLevelTweaks instead.")
    _topLevelTweaks
  }

  private[optimus] def existsWithNested(f: Scenario => Boolean) =
    if (f(this)) true
    else {
      var ns = this.nestedScenarios
      while (ns.nonEmpty && !f(ns.head)) ns = ns.tail
      ns.nonEmpty
    }

  /**
   * Check if there are any tweaks in this scenario or in any nested scenarios.
   *
   * @return
   *   Whether there are any tweaks at all.
   */
  def isEmpty: Boolean = _topLevelTweaks.length == 0 && (nestedScenarios.isEmpty || nestedScenarios.forall(_.isEmpty))

  /**
   * Check if there are any tweaks in this scenario or in any nested scenarios.
   *
   * @return
   *   Whether there are any tweaks at all. Returns true if there is tweak inside
   */
  def nonEmpty: Boolean = !this.isEmpty

  /**
   * Check if there are any tweaks in this scenario, but don't check in nested scenarios.
   *
   * @return
   *   Whether there are any tweaks in this scenario only.
   */
  def isEmptyShallow: Boolean = _topLevelTweaks.isEmpty

  // the main reason we cache this is not that it's expensive to call copyWithNested(Nil), but that it makes
  // SSCacheKey lookups faster if we always have the same withoutNested instance for a given scenario
  @transient
  private[this] var _withoutNested: Scenario = _

  /** Returns current scenario without nested scenarios. Given blocks always see flat one level scenarios */
  private[optimus] def withoutNested: Scenario = {
    if (nestedScenarios.isEmpty) this
    else {
      if (_withoutNested eq null) _withoutNested = copyWithNested(Nil)
      _withoutNested
    }
  }

  /** return structured nested scenarios below `this` */
  private[optimus] def withNestedAsScenario: Scenario =
    if (nestedScenarios.isEmpty) Scenario.empty
    else nestedScenarios.head.copyWithNested(nestedScenarios.tail)

  private def copyWithNested(replacementNested: List[Scenario]): Scenario =
    new Scenario(_topLevelTweaks, replacementNested, flags)

  /**
   * Check equality full for this scenario, including nested scenarios.
   *
   * @return
   *   Equality for this scenario and all nested scenarios.
   */
  override def equals(other: Any): Boolean = {
    other match {
      case that: Scenario =>
        if (this eq that) true
        else if (unorderedTweaks)
          _topLevelTweaks.toSet == that._topLevelTweaks.toSet && that.nestedScenarios == nestedScenarios
        else
          CoreHelpers.arraysAreEqual(_topLevelTweaks, that._topLevelTweaks) && that.nestedScenarios == nestedScenarios
      case _ => false
    }
  }

  @transient private[this] var _hashCode: Int = _

  /** General hashCode even though it ignores nested scenarios to be compatible with equals_ss */
  // noinspection HashCodeUsesVar
  override def hashCode: Int = {
    // intentionally unsynchronized for performance - worst case is that we recompute hashCode multiple times
    if (_hashCode == 0)
      _hashCode =
        if (unorderedTweaks) MurmurHash3.unorderedHash(_topLevelTweaks) else MurmurHash3.orderedHash(_topLevelTweaks)
    _hashCode
  }

  /**
   * Nest scenario 's' within this scenario. The nested scenarios will of the result will be the current nested
   * scenarios, then the argument, then the argument's nested scenarios.
   *
   * this is equivalent to: given(this.tweaks) given(this.nested scenarios) // each multiple givens, one for each item
   * in this.nested given(s.tweaks) given(s.nested scenarios) // each multiple givens, one for each item in s.nested
   *
   * Nested scenarios are not recursive - because we can flatten 's' and its nested scenarios into the nested scenarios
   * list on 'this'. (i.r. nested scenarios will not have nested scenarios). given(this.tweaks) given(this.nested :::
   * s.tweaks :: ) // each multiple givens, one for each item in this.nested given(s.tweaks) given(s.nest tweaks) //
   * each multiple givens, one for each item in s.nested
   *
   * @param s
   *   The scenario to test within this one
   * @return
   *   A new Scenario representing s nested within 'this'
   */
  def nest(s: Scenario): Scenario =
    if (s.isEmpty) this
    else if (this.isEmpty) s
    else copyWithNested(nestedScenarios ::: s.withoutNested :: s.nestedScenarios)

  def nestWithFlatten(s: Scenario): Scenario = {
    if (s.isEmpty) this
    else if (this.isEmpty) s
    else if (s.hasContextDependentTweaks) {
      copyWithNested(nestedScenarios ::: s.withoutNested :: s.nestedScenarios)
    } else {
      val combinedTweaks = new ju.HashMap[TweakTarget[_, _], Tweak]()
      val it = _topLevelTweaks.iterator
      while (it.hasNext) {
        val tweak = it.next()
        combinedTweaks.put(tweak.target, tweak)
      }
      val it2 = s._topLevelTweaks.iterator
      while (it2.hasNext) {
        val tweak = it2.next()
        combinedTweaks.put(tweak.target, tweak)
      }

      new Scenario(combinedTweaks.values().toArray(new Array[Tweak](0)), nestedScenarios, flags)
    }
  }

  def prettyString(isHtml: Boolean = false): String = {
    val builder = new HtmlBuilder
    writeHtml(builder)
    if (isHtml) builder.toString else builder.toPlaintext
  }

  def writeHtml(hb: HtmlBuilder, printNested: Boolean = true): HtmlBuilder = {
    hb.namedGroup("Scenario") {
      hb ++= getClass.getSimpleName
      if ((flags & ScenarioFlags.markedForDebugging) != 0) hb ++= "@init"
      if (this eq Scenario.empty) hb ++= NoStyle.unseparated("[]")
      else {
        hb.squareBracketsIndent {
          hb.tweaksToWrite(topLevelTweaks).foreach { _.writeHtml(hb).newLine() }

          if (printNested && nestedScenarios.nonEmpty)
            nestedScenarios.foreach(_.writeHtml(hb ++= "N:"))
        }
      }
    }
    hb.newLine() // so that nested scenarios are on the right level
  }

  override def toString: String = prettyString()

  override def dmcBinaryIdentity: java.io.Serializable = DmcScenarioRepresentation(this)
}

private object DmcScenarioRepresentation {
  def apply(scenario: Scenario): DmcScenarioRepresentation = {
    new DmcScenarioRepresentation(
      if (scenario.unorderedTweaks) scenario.topLevelTweaks.toSet else scenario.topLevelTweaks.toVector,
      scenario.nestedScenarios.map(apply))
  }
}

private class DmcScenarioRepresentation(
    val topLevelTweaks: Iterable[Tweak],
    val nestedScenarios: List[DmcScenarioRepresentation])
    extends Serializable {

  @transient override val hashCode: Int = MurmurHash3.arrayHash(Array(topLevelTweaks, nestedScenarios))

  def canEqual(other: Any): Boolean = other.isInstanceOf[DmcScenarioRepresentation]

  override def equals(other: Any): Boolean = other match {
    case that: DmcScenarioRepresentation =>
      (that canEqual this) &&
      topLevelTweaks == that.topLevelTweaks &&
      nestedScenarios == that.nestedScenarios
    case _ => false
  }

}

@parallelizable
object Scenario {
  val empty: Scenario = new Scenario(new Array[Tweak](0), Nil, ScenarioFlags.none)

  @expectingTweaks
  @nodeLiftByValue
  def byValue(tweaks: Tweak*): Scenario = apply(tweaks)

  @expectingTweaks
  @nodeLiftByName
  def byName(tweaks: Tweak*): Scenario = apply(tweaks)

  @expectingTweaks
  @nodeLiftByName
  def apply(tweaks: Tweak*): Scenario = apply(tweaks)

  @expectingTweaks
  @nowarn("msg=10500 optimus.platform.Scenario.unordered")
  def apply(tweaks: Iterable[Tweak]): Scenario = unordered(tweaks, unorderedTweaks = false)

  /**
   * Currently it's possible to compare two scenarios that are the same but don't compare equal because of initial tweak
   * ordering. This method tries to normalise this. Ideas for a full solution:
   *   1. Implement ordering on Tweak
   *   1. Construct scenario with Set[Tweak] (but this is expensive, so one overload would still be Seq - in the normal
   *      case we wouldn't incur the cost)
   */
  @expectingTweaks
  @nodeLiftByName
  @deprecating("Internal API only")
  def unordered(tweaks: Iterable[Tweak], unorderedTweaks: Boolean = true): Scenario = {
    val it = tweaks.iterator
    var flags = ScenarioFlags.none
    while (it.hasNext) {
      val tweak = it.next()
      if (tweak.isReducibleToByValue) flags |= ScenarioFlags.hasReducibleToByValueTweaks
      if (tweak.shouldCheckForRedundancy) flags |= ScenarioFlags.hasPossiblyRedundantTweaks
      if (tweak.target.isUnresolvedOrMarkerTweak) flags |= ScenarioFlags.hasUnresolvedOrMarkerTweaks
      if (tweak.isContextDependent) flags |= ScenarioFlags.hasContextDependentTweaks
      if (tweak.target.propertyInfo.tweakableID() == 1) flags |= ScenarioFlags.markedForDebugging
    }
    if (unorderedTweaks) flags |= ScenarioFlags.unorderedTweaks
    val scenario = Scenario.validate(tweaks, "When creating Scenario", unorderedTweaks, flags)
    if ((flags & ScenarioFlags.markedForDebugging) != 0)
      scenario._createdAt = new Exception()
    scenario
  }

  def toNestedScenarios(scenarios: collection.Seq[Scenario]): Scenario = {
    scenarios.reduceOption((a, b) => a.nest(b)).getOrElse(Scenario.empty)
  }

  private[optimus] def validate(
      topLevelTweaks: Iterable[Tweak],
      messagePrefix: String,
      unorderedTweaks: Boolean,
      flags: Int): Scenario = {
    var flagsToUse: Int = flags
    val it = topLevelTweaks.iterator
    var instanceTweaks: ju.HashMap[TweakableKey, Tweak] = null

    /** See: [[ optimus.graph.SSCacheID.tweaks ]] */
    var propertyTweaks: ju.HashMap[AnyRef, AnyRef] = null

    while (it.hasNext) {
      val tweak = it.next()
      val target = tweak.target

      val pinfo = target.propertyInfo
      val hashKey = target.hashKey

      if (hashKey ne null) {
        if (instanceTweaks eq null) instanceTweaks = new ju.HashMap[TweakableKey, Tweak]
        val prevTweak = instanceTweaks.put(hashKey, tweak)
        if ((prevTweak ne null) && (prevTweak != tweak && !tweak.dupAllowed)) {
          val tweakValues =
            if (Settings.showDuplicateInstanceTweakValues)
              s"Tweak 1: $prevTweak; Tweak 2: $tweak."
            else
              s"To see full details of the tweaks, set -D${Settings.ShowDuplicateInstanceTweakValuesProp}=true and " +
                s"-D${DiagnosticSettings.TRACE_TWEAKS}=true. "
          val ex = new IllegalArgumentException(
            s"$messagePrefix: Conflicting instance tweaks provided for node '${pinfo.fullName()}'. " +
              tweakValues +
              "See ConflictingTweaks.md for guidance. ")
          if (Settings.throwOnDuplicateInstanceTweaks) throw ex
          else if (unorderedTweaks) {
            sendTweaksConflictingWithScenarioUnordered(ex)
            throw new IllegalArgumentException("Conflicting tweaks found while using unordered scenario", ex)
          } else
            log.warn(
              "Conflicting tweak warning (non-fatal, but you should fix your code; " +
                "the last conflicting tweak for this node will be used)",
              ex)
          flagsToUse = flagsToUse & ~ScenarioFlags.hasPossiblyRedundantTweaks
        }
      } else {
        if (propertyTweaks eq null) propertyTweaks = new ju.HashMap[AnyRef, AnyRef]
        val newValue = tweak.target match {
          case target: TweakKeyExtractorTarget[_, _] =>
            val keyExtractor = target.keyExtractor
            val prevTweak = propertyTweaks.put(target, tweak)
            if ((prevTweak ne null) && prevTweak != tweak)
              throw new IllegalArgumentException(
                s"$messagePrefix: Same tweak extractors should have the same value producers for the same property: " +
                  s"$pinfo extractor: $keyExtractor  T1:$prevTweak   T2:$tweak")
            keyExtractor
          case _ => tweak
        }
        val prevValue = propertyTweaks.put(pinfo, newValue)
        if ((prevValue ne null) && (prevValue ne newValue))
          (prevValue, newValue) match {
            case (_: TweakKeyExtractor, _: TweakKeyExtractor) =>
              throw new IllegalArgumentException(
                s"$messagePrefix: Tweak extractors at the same level for the same property need to be equal: $pinfo  T1:$prevValue   T2:$newValue")
            case (_: Tweak, _: Tweak) =>
              throw new IllegalArgumentException(
                s"$messagePrefix: Multiple property-level tweaks to the same property in the same scenario not supported. See ConflictingTweaks.md for guidance. $pinfo  T1:$prevValue   T2:$tweak")
            case (_, _) =>
              throw new IllegalArgumentException(
                s"$messagePrefix: Cannot mix property-level tweaks and tweak extractors to the same property in the same scenario. $pinfo  T1:$prevValue   T2:$tweak")
          }

      }
    }
    new Scenario(topLevelTweaks.toArray, Nil, flagsToUse)
  }
}
