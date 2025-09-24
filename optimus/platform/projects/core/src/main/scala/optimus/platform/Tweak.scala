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

import optimus.core._
import optimus.graph.DiagnosticSettings.traceTweaksOverflowDetected
import optimus.graph._
import optimus.platform.annotations._
import optimus.platform.storable.Storable
import optimus.platform.util.html._

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.compat.immutable.ArraySeq
import scala.collection.mutable

/**
 * Class describes a general tweak, there are few classes that derive from Tweak, but they should only be used to convey
 * more type operations e.g. PropertyTweak
 */
class Tweak private[optimus] (
    final val target: TweakTarget[_, _],
    final val tweakTemplate: TweakNode[_],
    final private[optimus] val evaluateIn: Int,
    final private[optimus] val flags: Int,
    previousCreationStack: StackTraceElement)
    extends Serializable {

  protected def underlyingTarget: TweakTarget[_, _] = target

  final private[optimus] def alwaysInvalidateTrack: Boolean = (flags & TweakFlags.alwaysInvalidateTrack) != 0
  final private[optimus] def dupAllowed: Boolean = (flags & TweakFlags.dupAllowed) != 0
  final private[optimus] def keepLazyEval: Boolean = (flags & TweakFlags.keepLazyEval) != 0
  final private[optimus] def withPrivateCacheOnUse: Boolean = (flags & TweakFlags.withPrivateCacheOnUse) != 0

  @transient // need this regardless of OGTrace.observer.traceTweaks if traceTweaksEnabled
  private[optimus] val id =
    if (DiagnosticSettings.traceTweaksEnabled) {
      val v = Tweak.counter.incrementAndGet()
      if (v == Integer.MAX_VALUE) {
        log.error("Tweak ID overflowed, tweak tracing feature disabled")
        traceTweaksOverflowDetected = true
      }
      v
    } else 0

  @transient
  private[optimus] val initSite: StackTraceElement =
    if (previousCreationStack ne null) previousCreationStack
    else if (DiagnosticSettings.traceTweaksEnabled) CoreHelpers.tweakCreationSite()
    else null

  private[optimus] def this(target: TweakTarget[_, _], tweakTemplate: TweakNode[_]) =
    this(target, tweakTemplate, Tweak.evaluateInParentOfGiven, TweakFlags.none, null /*user entry point*/ )

  final private[optimus] def retarget(newTarget: TweakTarget[_, _]): Tweak =
    new Tweak(newTarget, tweakTemplate, evaluateIn, flags, initSite)

  if (evaluateIn != Tweak.evaluateInParentOfGiven && target.unresolved) {
    throw new RuntimeException("Tweak.bind is not supported for tweaks with handlers! Tweak: " + target.toString())
  }

  final def target(keyExtractor: TweakKeyExtractor, keyValue: Any): Tweak = {
    val propInfo = underlyingTarget.propertyInfo
    val fullKey = ExtractorTweakableKey(keyValue, propInfo)
    val target = new TweakKeyExtractorTarget(propInfo, keyExtractor, fullKey)
    new Tweak(target, tweakTemplate)
  }

  private[optimus] final def bind(evaluateIn: Int): Tweak =
    if (evaluateIn == this.evaluateIn) this
    else new Tweak(target, tweakTemplate, evaluateIn, flags, initSite)

  /**
   * Returns fully expanded tweaks, in the same binding scope as this
   *
   * @param currentScenarioStack
   *   the current stack. Must match EvaluationContext.scenarioStack
   */
  private[optimus /*platform*/ ] def expanded(currentScenarioStack: ScenarioStack): Seq[Tweak] = {
    if (target.unresolved) {
      val b = ArraySeq.newBuilder[Tweak]
      expandInto(b, currentScenarioStack)
      b.result()
    } else this :: Nil
  }

  /**
   * Adds fully expanded tweaks to mutable outputBuffer (more efficient version of #expanded)
   */
  private[optimus] def expandInto(outputBuffer: mutable.Growable[Tweak], currentScenarioStack: ScenarioStack): Unit = {
    if (target.unresolved) {
      val expansion = target.hashKey
        .asInstanceOf[PropertyNode[Any]]
        .transformTweak(tweakTemplate.asInstanceOf[TweakNode[Any]], currentScenarioStack)
        .iterator
      while (expansion.hasNext) expansion.next().expandInto(outputBuffer, currentScenarioStack)
    } else outputBuffer += this
  }

  override def equals(other: Any): Boolean = other match {
    case that: Tweak =>
      (that eq this) ||
      ((that canEqual this) &&
        that.target == target &&
        (that.tweakTemplate equalsAsTemplates tweakTemplate))
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Tweak]

  @transient private[this] var _hash: Int = _
  // noinspection HashCodeUsesVar
  override def hashCode: Int = {
    if (_hash == 0)
      _hash = 41 * (41 + target.hashCode) + tweakTemplate.hashCodeAsTemplate
    _hash
  }

  override def toString: String = prettyString
  def prettyString: String = prettyString(false)
  def prettyString(short: Boolean): String = {
    val builder = new HtmlBuilder(short = short)
    writeHtml(builder)
    builder.toPlaintext
  }

  /**
   * mockito doesn't cope properly with a default parameter in Scala, because if our method signature is: def
   * writeHtml(hb: HtmlBuilder, ignoreTracing: Boolean = false) then the compiler transforms a call to writeHtml(hb) to
   * writeHtml(hb, writeHtml$default$2). Since we don't want to mock the call to the default method in unit tests using
   * mock[Tweak], we just won't supply a default parameter here. This is a bit sad but I don't have it in me to fix
   * optimus mockito right now
   */
  def writeHtml(hb: HtmlBuilder): HtmlBuilder = writeHtml(hb, ignoreTracing = false)

  /** ignoreTracing used in RecordingScenario where we write tweaks as part of the TweakTreeNode, not in a Scenario */
  // noinspection ConvertibleToMethodValue (note: need _ because of DummyImplicit in buildPrettyLeaf)
  def writeHtml(hb: HtmlBuilder, ignoreTracing: Boolean): HtmlBuilder = {
    hb.namedGroup("Tweak") {
      val infected = ignoreTracing || hb.displayInColour(this.id)
      val xsftDependsOn = if (hb.tpdMask eq null) false else hb.tpdMask.intersects(target.propertyInfo.tweakMask)
      hb.buildPrettyLeaf(
        if (infected) TweakTargetStyle else (if (xsftDependsOn) TweakMid else TweakLow),
        target.writePrettyString _)

      hb.buildPrettyLeaf(
        if (infected) TweakVal else (if (xsftDependsOn) TweakMid else TweakLow),
        tweakTemplate.writePrettyString(_, this))
      hb.removeSeparator()

      hb.buildLeaf(TweakLow) { sb =>
        if (!hb.short) sb ++= " #" ++= hashCode.toHexString
        if (isContextDependent) sb ++= evaluateInDisplayValue
      }

      if (initSite ne null) {
        val source = initSite.getFileName + ":" + initSite.getLineNumber
        hb += "(" ++= Link(source, source) ++= ")"
      }
      hb.removeSeparator()
    }
  }

  /** sure we want to expose this? */
  private[optimus] def tweakValue: AnyRef =
    tweakTemplate.computeGenerator match {
      case n: Node[_] if n.isStable => n.resultObject
      case _                        => throw new GraphException("Tweak value cannot be returned")
    }

  protected def evaluateInDisplayValue: String = {
    if (evaluateIn == Tweak.evaluateInParentOfGiven) "" // it's a default value and no-point of displaying it all over
    else if (evaluateIn == Tweak.evaluateInCurrent) "current"
    else if (evaluateIn == Tweak.evaluateInGiven) "given"
    else "?!"
  }

  /**
   * Create an instance of the node that will produce the value in the given scenariostack(s)
   *
   * @param ss
   *   requesting scenarioStack
   * @param givenSS
   *   where the tweak was found/setup
   * @param key
   *   empty/un-attached node, that would be executed if not for the tweak
   */
  private[optimus] final def createNode[R](ss: ScenarioStack, givenSS: ScenarioStack, key: PropertyNode[R]) = {
    tweakTemplate.asInstanceOf[TweakNode[R]].cloneWith(this, evaluateIn, ss, givenSS, key)
  }

  /**
   * Create an instance of a node that will produce LHS of the tweak value
   */
  private[optimus] def computableValueNode[R](evaluateInSS: ScenarioStack): Node[R] = {
    tweakTemplate.asInstanceOf[TweakNode[R]].getComputeNode(evaluateInSS, null)
  }

  /*
   * Return the result of the evaluation of the LHS of the tweak value
   */
  private[optimus] def computableValueResult[R](evaluateInSS: ScenarioStack): R = {
    computableValueNode[R](evaluateInSS).result
  }

  /** False for the tweaks, that have computed/fixed value and the target is fixed and doesn't need to be resolved */
  private[optimus] final def isContextDependent =
    !tweakTemplate.resultIsScenarioIndependent() || target.hasWhenClause || target.unresolved

  /** True for tweaks whose target has when clause */
  private[optimus] final def hasWhenClause = target.hasWhenClause

  /** If all the inputs into RHS of the tweak are known at the entry into a given block, can replace with byValue RHS */
  private[optimus] final def isReducibleToByValue: Boolean =
    (evaluateIn == Tweak.evaluateInParentOfGiven) && tweakTemplate.isReducibleToByValue && !keepLazyEval

  /** Consider comparing tweak RHS ('to value') to the LHS (previous) value */
  private[optimus] final def shouldCheckForRedundancy: Boolean =
    tweakTemplate.resultIsStable() && target.fullSpecified && target.propertyInfo.canRemoveRedundant

  /** Retarget tweak to byValue... Warning: this method downgrades any tweak to a simple tweak */
  private[optimus] def reduceToByValue(v: Node[_]): Tweak = {
    if (Settings.schedulerAsserts && !(v.isDone && v.isStable))
      throw new GraphInInvalidState("reduceToByValue needs a done and stable Node!")
    new Tweak(target, new TweakNode(v), evaluateIn, flags & TweakFlags.dupAllowed, initSite)
  }

  /** Returns a new tweak that allows for duplicate instance tweaks on the same scenario */
  final def resolveDupsByTakingLast: Tweak =
    if (dupAllowed) this
    else new Tweak(target, tweakTemplate, evaluateIn, flags | TweakFlags.dupAllowed, initSite)

  /**
   * Return a new tweak that keeps byName tweak as lazy evaluated even if given tries to auto-convert to byValue tweaks
   */
  final def keepLazyEvaluation: Tweak =
    if (keepLazyEval) this
    else new Tweak(target, tweakTemplate, evaluateIn, flags | TweakFlags.keepLazyEval, initSite)

  /** Return a new tweak that always invalidates tweakable tracker (even if the value hasn't changed) */
  final def alwaysInvalidateTracking: Tweak =
    if (alwaysInvalidateTrack) this
    else new Tweak(target, tweakTemplate, evaluateIn, flags | TweakFlags.alwaysInvalidateTrack, initSite)

  /** Returns a new tweak that always when used in a ScenarioStack will start a private cache */
  final def withPrivateCache: Tweak = {
    if (withPrivateCacheOnUse) this
    else new Tweak(target, tweakTemplate, evaluateIn, flags | TweakFlags.withPrivateCacheOnUse, initSite)
  }
}

@parallelizable
object Tweak {
  private val counter = new AtomicInteger()

  private[optimus] final def evaluateInParentOfGiven = 1 // by name
  private[optimus] final def evaluateInCurrent = 2 // bind once
  private[optimus] final def evaluateInGiven = 3 // bind

  @expectingTweaks
  @nodeLiftByName
  def byName(tweak: Tweak): Tweak = tweak

  @expectingTweaks
  @nodeLiftByName
  def byName[E, WhenT <: AnyRef](tweak: PropertyTweak[E, WhenT]): PropertyTweak[E, WhenT] = tweak

  @expectingTweaks
  @nodeLiftByValue
  def byValue[E, WhenT <: AnyRef](tweak: PropertyTweak[E, WhenT]): PropertyTweak[E, WhenT] = tweak

  @expectingTweaks
  @nodeLiftByValue
  def byValue(tweak: Tweak): Tweak = tweak

  @expectingTweaks
  @nodeLiftByName
  def bind(tweak: Tweak): Tweak = tweak.bind(if (Settings.kludgeBind2BindOnce) evaluateInGiven else evaluateInCurrent)

  @expectingTweaks
  @nodeLiftByName
  def bindOnce(tweak: Tweak): Tweak = tweak.bind(evaluateInGiven)
}

@parallelizable
object Tweaks {
  import Tweak._

  // start - need to clean
  @expectingTweaks
  @nodeLiftByName
  def apply(tweaks: Tweak*): Seq[Tweak] = tweaks

  @expectingTweaks
  @nodeLiftByName
  def apply(tweak: Tweak): Seq[Tweak] = tweak :: Nil

  // end - need to clean

  @expectingTweaks
  @nodeLiftByName
  def byName(tweaks: Tweak*): Seq[Tweak] = tweaks

  @expectingTweaks
  @nodeLiftByName
  def byName(tweak: Tweak): Seq[Tweak] = tweak :: Nil

  @expectingTweaks
  @nodeLiftByName
  def byName(): Seq[Tweak] = Nil

  @expectingTweaks
  @nodeLiftByValue
  def byValue(tweaks: Tweak*): Seq[Tweak] = tweaks

  @expectingTweaks
  @nodeLiftByValue
  def byValue(tweak: Tweak): Seq[Tweak] = tweak :: Nil

  @expectingTweaks
  @nodeLiftByValue
  def byValue(): Seq[Tweak] = Nil

  @expectingTweaks
  @nodeLiftByName
  def bind(tweaks: Tweak*): Seq[Tweak] = tweaks map {
    _.bind(if (Settings.kludgeBind2BindOnce) evaluateInGiven else evaluateInCurrent)
  }

  @expectingTweaks
  @nodeLiftByName
  def bind(tweak: Tweak): Seq[Tweak] =
    tweak.bind(if (Settings.kludgeBind2BindOnce) evaluateInGiven else evaluateInCurrent) :: Nil

  @expectingTweaks
  @nodeLiftByName
  def bindOnce(tweaks: Tweak*): Seq[Tweak] = tweaks map { _.bind(evaluateInGiven) }

  @expectingTweaks
  @nodeLiftByName
  def bindOnce(tweak: Tweak): Seq[Tweak] = tweak.bind(evaluateInGiven) :: Nil

  def apply(): Seq[Tweak] = Nil
  def empty: Seq[Tweak] = Nil

}

class PropertyTweak[E, WhenT <: AnyRef](_target: TweakTarget[_, _], _tweakTemplate: TweakNode[_])
    extends Tweak(_target, _tweakTemplate) {

  @nodeSyncLift
  @nodeLiftByName
  final def target(@nodeLift @withNodeClassID predicate: WhenT): Tweak = target$withNode(predicate)
  final def target$withNode(vn: AnyRef /* FunctionN(... Node[R])*/ ): Tweak =
    new PropertyTweak(
      PredicatedPropertyTweakTarget(underlyingTarget.asInstanceOf[DefPropertyInfo[E, WhenT, _, _]], vn),
      tweakTemplate)

  final def targetPredicate(predicate: NodeFunction1[E, Boolean]): Tweak =
    target$withNode(NodeFunction1.nodeGenerator(predicate))

  final def targetEntity(entity: E): Tweak = underlyingTarget match {
    // if target is a PropertyInfo0, we have all the ingredient to generate the instance tweak
    case _: PropertyInfo0[E @unchecked, _] =>
      // type constraint is slightly different in PropertyInfo0, hence mixin Storable to make compiler happy
      new Tweak(
        new InstancePropertyTarget(
          underlyingTarget
            .asInstanceOf[PropertyInfo0[E with Storable, _]]
            .createNodeKey(entity.asInstanceOf[E with Storable])),
        tweakTemplate
      )
    // if target is not a PropertyInfo0, we do NOT have enough information to generate the instance tweak,
    // so return a partially targeted tweak
    case _: PropertyInfo1[E @unchecked, _, _] =>
      target$withNode({ (e: E, _: Any) =>
        new AlreadyCompletedNode(e == entity)
      })
    case _: PropertyInfo2[E @unchecked, _, _, _] =>
      target$withNode({ (e: E, _: Any, _: Any) =>
        new AlreadyCompletedNode(e == entity)
      })
    case _: PropertyInfo3[E @unchecked, _, _, _, _] =>
      target$withNode({ (e: E, _: Any, _: Any, _: Any) =>
        new AlreadyCompletedNode(e == entity)
      })
    case _: PropertyInfo4[E @unchecked, _, _, _, _, _] =>
      target$withNode({ (e: E, _: Any, _: Any, _: Any, _: Any) =>
        new AlreadyCompletedNode(e == entity)
      })
    case _: PropertyInfo5[E @unchecked, _, _, _, _, _, _] =>
      target$withNode({ (e: E, _: Any, _: Any, _: Any, _: Any, _: Any) =>
        new AlreadyCompletedNode(e == entity)
      })
    case _: PropertyInfo6[E @unchecked, _, _, _, _, _, _, _] =>
      target$withNode({ (e: E, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any) =>
        new AlreadyCompletedNode(e == entity)
      })
    case _: PropertyInfo7[E @unchecked, _, _, _, _, _, _, _, _] =>
      target$withNode({ (e: E, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any) =>
        new AlreadyCompletedNode(e == entity)
      })
    case _: PropertyInfo8[E @unchecked, _, _, _, _, _, _, _, _, _] =>
      target$withNode({ (e: E, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any) =>
        new AlreadyCompletedNode(e == entity)
      })
    case _: PropertyInfo9[E @unchecked, _, _, _, _, _, _, _, _, _, _] =>
      target$withNode({ (e: E, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any) =>
        new AlreadyCompletedNode(e == entity)
      })
    case _: PropertyInfo10[E @unchecked, _, _, _, _, _, _, _, _, _, _, _] =>
      target$withNode({ (e: E, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any) =>
        new AlreadyCompletedNode(e == entity)
      })
    case _: PropertyInfo11[E @unchecked, _, _, _, _, _, _, _, _, _, _, _, _] =>
      target$withNode({
        (e: E, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any) =>
          new AlreadyCompletedNode(e == entity)
      })
    case _: PropertyInfo12[E @unchecked, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
      target$withNode({
        (e: E, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any) =>
          new AlreadyCompletedNode(e == entity)
      })
    case _: PropertyInfo13[E @unchecked, _, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
      target$withNode({
        (
            e: E,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any) =>
          new AlreadyCompletedNode(e == entity)
      })
    case _: PropertyInfo14[E @unchecked, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
      target$withNode({
        (
            e: E,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any) =>
          new AlreadyCompletedNode(e == entity)
      })
    case _: PropertyInfo15[E @unchecked, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
      target$withNode({
        (
            e: E,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any) =>
          new AlreadyCompletedNode(e == entity)
      })
    case _: PropertyInfo16[E @unchecked, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
      target$withNode({
        (
            e: E,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any) =>
          new AlreadyCompletedNode(e == entity)
      })
    case _: PropertyInfo17[E @unchecked, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
      target$withNode({
        (
            e: E,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any) =>
          new AlreadyCompletedNode(e == entity)
      })
    case _: PropertyInfo18[E @unchecked, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
      target$withNode({
        (
            e: E,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any) =>
          new AlreadyCompletedNode(e == entity)
      })
    case _: PropertyInfo19[E @unchecked, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
      target$withNode({
        (
            e: E,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any) =>
          new AlreadyCompletedNode(e == entity)
      })
    case _: PropertyInfo20[E @unchecked, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
      target$withNode({
        (
            e: E,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any) =>
          new AlreadyCompletedNode(e == entity)
      })
    case _: PropertyInfo21[E @unchecked, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _] =>
      target$withNode({
        (
            e: E,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any,
            _: Any) =>
          new AlreadyCompletedNode(e == entity)
      })
  }

}

object SimpleValueTweak {
  @nodeLift
  @expectingTweaks
  def apply[T](n: T)(value: T): Tweak = optimus.core.needsPlugin
  def apply[T](node: NodeKey[T])(value: T) =
    new Tweak(new InstancePropertyTarget(node), new TweakNode(new AlreadyCompletedNode(value)))
  def apply$node[T](node: NodeKey[T], value: T): Tweak = apply(node)(value)
}
