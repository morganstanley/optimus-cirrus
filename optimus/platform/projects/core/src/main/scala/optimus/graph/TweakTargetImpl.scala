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

import optimus.platform.PluginHelpers.mkCompute
import optimus.platform.{PropertyTweak, ScenarioStack, Tweak}
import optimus.platform.annotations.{nodeLift, nodeLiftByName, nodeSyncLift, tweakOperator, withNodeClassID}
import optimus.platform.util.PrettyStringBuilder

/**
 * Identifies a Property on a specific entity instance and with specific arguments as a TweakTarget. e.g. <i>e.foo(3)<i>
 * @todo
 *   Target that supports properties with args
 * @todo
 *   Different TweakTargets will be added that support identifying a property type on any instance of an entity type, or
 *   by tag (both with predicate support).
 *
 * Don't think you can change to a case class, because that will bring a whole bunch of other methods. Given that we
 * have an implicit conversion to InstancePropertyTarget this would result in a lot of strange errors.
 */
final class InstancePropertyTarget[R, SetT](override val key: NodeKey[R])
    extends TweakTargetKey[R, SetT]
    with Serializable {
  // We have to test for isTweakable, because some code creates InstancePropertyTarget on keys without any confidence checks
  if ((key.entity eq null) || (key.propertyInfo eq null) || !key.propertyInfo.isTweakable)
    throw new GraphInInvalidState(s"Invalid Tweak Key for: ${key.propertyInfo}")

  override def propertyInfo: NodeTaskInfo = key.propertyInfo
  override def matchesKeyFromRSS(tssKey: NodeKey[_]): Boolean = tssKey.propertyInfo == propertyInfo && tssKey == key
  override def hashKey: NodeKey[R] = this.key
  override def unresolved: Boolean = key.propertyInfo.hasTweakHandler

  override def hashCode: Int = key.hashCode
  override def equals(rhs: Any): Boolean =
    (rhs.asInstanceOf[AnyRef] eq this) || (rhs match {
      case x: InstancePropertyTarget[_, _] => x.key == key
      case _                               => false
    })

  def writePrettyString(sb: PrettyStringBuilder): PrettyStringBuilder = {
    if (!sb.short) {
      val entityString =
        try key.entity.toString
        catch { case _: Exception => key.entity.getClass.getName + "[.toString threw!]" }
      sb ++= entityString ++= "."
    }
    sb ++= propertyInfo.name
  }
}

/**
 * Tweak target created with a custom keyExtractor
 */
final class TweakKeyExtractorTarget[R, SetT](
    val info: NodeTaskInfo,
    val keyExtractor: TweakKeyExtractor,
    val key: ExtractorTweakableKey)
    extends TweakTarget[R, SetT]
    with Serializable {
  if ((info eq null) || !info.isTweakable)
    throw new GraphInInvalidState("Invalid Tweak Key")

  override def propertyInfo: NodeTaskInfo = info
  override def matchesKeyFromRSS(tssKey: NodeKey[_]): Boolean = tssKey.propertyInfo == info

  override def hashCode: Int = info.hashCode() * 31 + key.hashCode
  override def equals(rhs: Any): Boolean =
    (rhs.asInstanceOf[AnyRef] eq this) || (rhs match {
      case x: TweakKeyExtractorTarget[_, _] => x.info == info && x.key == key && x.keyExtractor == keyExtractor
      case _                                => false
    })

  def writePrettyString(sb: PrettyStringBuilder): PrettyStringBuilder = {
    if (!sb.short) {
      val entityString =
        try key.toString
        catch { case _: Exception => key.getClass.getName + "[.toString threw!]" }
      sb ++= entityString ++= "."
    }
    sb ++= propertyInfo.name
  }
}

/**
 * Identifies a Property on any instance of an Entity as a TweakTarget.
 */
trait PropertyTarget[E, WhenT <: AnyRef, SetT <: AnyRef, R] extends TweakTarget[R, SetT] with Serializable {
  this: DefPropertyInfo[E, WhenT, SetT, R] =>

  override def propertyInfo: NodeTaskInfo = this

  if (this.hasTweakHandler)
    throw new UnsupportedOperationException("Only direct tweaking is supported for property: " + propertyInfo)

  override def matchesKeyFromRSS(key: NodeKey[_]): Boolean = key.propertyInfo == propertyInfo

  def tweakByValueExtractor(extractor: TweakKeyExtractor, key: AnyRef, valueProvider: SITweakValueProvider): Tweak = {
    val fullKey = ExtractorTweakableKey(key, this)
    val target = new TweakKeyExtractorTarget(this, extractor, fullKey)
    new Tweak(target, new TweakNode(new TweakValueProviderNode(valueProvider, modify = false)))
  }

  def tweakByValueExtractor(
      extractor: TweakKeyExtractor,
      keys: Seq[AnyRef],
      valueProvider: SITweakValueProvider): Seq[Tweak] =
    keys.map(tweakByValueExtractor(extractor, _, valueProvider))

  def tweakByModifyValueExtractor(
      extractor: TweakKeyExtractor,
      key: AnyRef,
      valueProvider: SITweakValueProvider): Tweak = {
    val fullKey = ExtractorTweakableKey(key, this)
    val target = new TweakKeyExtractorTarget(this, extractor, fullKey)
    new Tweak(target, new TweakNode(new TweakValueProviderNode(valueProvider, modify = true)))
  }

  def tweakByModifyValueExtractor(
      extractor: TweakKeyExtractor,
      keys: Seq[AnyRef],
      valueProvider: SITweakValueProvider): Seq[Tweak] =
    keys.map(tweakByModifyValueExtractor(extractor, _, valueProvider))

  @nodeSyncLift
  @nodeLiftByName
  def when(@nodeLift @withNodeClassID predicate: WhenT): PredicatedPropertyTweakTarget[E, WhenT, SetT, R] =
    PredicatedPropertyTweakTarget[E, WhenT, SetT, R](this, predicate)
  // noinspection ScalaUnusedSymbol
  def when$withNode(vn: AnyRef /* FunctionN(... Node[R])*/ ): PredicatedPropertyTweakTarget[E, WhenT, SetT, R] =
    PredicatedPropertyTweakTarget[E, WhenT, SetT, R](this, vn)

  /* BEGIN DUPLICATION OF THE TweakTarget INTERFACE. The only change is to allow for PropertyTweak type propagation */
  private[this] def makeTweak(tweakTemplate: TweakNode[R]) = new PropertyTweak[E, WhenT](this, tweakTemplate)

  @tweakOperator
  @nodeSyncLift
  override def :=(@nodeLift @withNodeClassID value: => R): PropertyTweak[E, WhenT] =
    makeTweak(new TweakNode[R](mkCompute(value _)))
  // noinspection ScalaUnusedSymbol
  override def $colon$eq$withValue(value: R): PropertyTweak[E, WhenT] =
    makeTweak(new TweakNode[R](new AlreadyCompletedNode(value)))
  override def $colon$eq$withNode(vn: Node[R]): PropertyTweak[E, WhenT] = makeTweak(new TweakNode[R](vn))

  @tweakOperator
  @nodeSyncLift
  override def :=(@nodeLift @withNodeClassID f: SetT): PropertyTweak[E, WhenT] =
    makeTweak(new TweakNode[R](f))
  override def $colon$eq$withNode(vn: AnyRef /* FunctionN(... Node[R])*/ ): PropertyTweak[E, WhenT] =
    makeTweak(new TweakNode[R](vn))

  @tweakOperator
  @nodeSyncLift
  override def :+=(@nodeLift @withNodeClassID value: => R)(implicit ev: Numeric[R]): PropertyTweak[E, WhenT] =
    makeTweak(mkPlus(mkCompute(value _)))
  // noinspection ScalaUnusedSymbol
  override def $colon$plus$eq$withValue(value: R)(implicit ev: Numeric[R]): PropertyTweak[E, WhenT] = makeTweak(
    mkPlus(new AlreadyCompletedNode(value))(ev))
  override def $colon$plus$eq$withNode(vn: Node[R])(implicit ev: Numeric[R]): PropertyTweak[E, WhenT] = makeTweak(
    mkPlus(vn))

  @tweakOperator
  @nodeSyncLift
  override def :-=(@nodeLift @withNodeClassID value: => R)(implicit ev: Numeric[R]): PropertyTweak[E, WhenT] =
    makeTweak(mkMinus(mkCompute(value _)))
  // noinspection ScalaUnusedSymbol
  override def $colon$minus$eq$withValue(value: R)(implicit ev: Numeric[R]): PropertyTweak[E, WhenT] =
    makeTweak(mkMinus(new AlreadyCompletedNode(value)))
  override def $colon$minus$eq$withNode(vn: Node[R])(implicit ev: Numeric[R]): PropertyTweak[E, WhenT] =
    makeTweak(mkMinus(vn))

  @tweakOperator
  @nodeSyncLift
  override def :*=(@nodeLift @withNodeClassID value: => R)(implicit ev: Numeric[R]): PropertyTweak[E, WhenT] =
    makeTweak(mkTimes(mkCompute(value _)))
  // noinspection ScalaUnusedSymbol
  override def $colon$times$eq$withValue(value: R)(implicit ev: Numeric[R]): PropertyTweak[E, WhenT] =
    makeTweak(mkTimes(new AlreadyCompletedNode(value)))
  override def $colon$times$eq$withNode(vn: Node[R])(implicit ev: Numeric[R]): PropertyTweak[E, WhenT] =
    makeTweak(mkTimes(vn))

  @tweakOperator
  @nodeSyncLift
  override def :/=(@nodeLift @withNodeClassID value: => R)(implicit ev: Fractional[R]): PropertyTweak[E, WhenT] =
    makeTweak(mkDiv(mkCompute(value _)))
  // noinspection ScalaUnusedSymbol
  override def $colon$div$eq$withValue(value: R)(implicit ev: Fractional[R]): PropertyTweak[E, WhenT] =
    makeTweak(mkDiv(new AlreadyCompletedNode(value)))
  override def $colon$div$eq$withNode(vn: Node[R])(implicit ev: Fractional[R]): PropertyTweak[E, WhenT] =
    makeTweak(mkDiv(vn))

  /* END DUPLICATION OF THE TweakTarget INTERFACE. */

  def writePrettyString(sb: PrettyStringBuilder): PrettyStringBuilder =
    sb ++= "P:" ++= propertyInfo.entityInfo.toString ++= "." ++= propertyInfo.name
}

/**
 * A TweakTarget predicated on (Entity, a1, a2,....)
 */
final case class PredicatedPropertyTweakTarget[E, WhenT <: AnyRef, SetT, R](
    propertyInfo: DefPropertyInfo[E, WhenT, SetT, R],
    predicate: AnyRef)
    extends TweakTarget[R, SetT]
    with Serializable {

  override def equals(other: Any): Boolean = other match {
    case that: PredicatedPropertyTweakTarget[_, _, _, _] =>
      (that eq this) ||
      ((that canEqual this) &&
        that.propertyInfo == propertyInfo &&
        predicateEquals(that.predicate))
    case _ => false
  }

  private var _tag: String = _
  def tag(tag: String): PredicatedPropertyTweakTarget[E, WhenT, SetT, R] = {
    _tag = tag
    this
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[PredicatedPropertyTweakTarget[_, _, _, _]]
  private def predicateEquals(other: AnyRef) = NodeClsIDSupport.equals(other, predicate)

  override def hashCode: Int =
    41 * (41 + propertyInfo.hashCode) + (if (predicate.isInstanceOf[NodeClsID]) NodeClsIDSupport.hashCode(predicate)
                                         else predicate.hashCode)

  override def matchesKeyFromRSS(key: NodeKey[_]): Boolean =
    false // Predicated tweak targets are not supported by this API

  override def hasWhenClause: Boolean = true
  override def whenPredicate: AnyRef = predicate
  override def whenClauseNode(key: NodeKey[_], scenarioStack: ScenarioStack): Node[Boolean] = {
    val node = key.argsCopy(predicate).asInstanceOf[Node[Boolean]]
    // [XS_NO_WAIT]
    // whenNode is evaluated synchronously (runAndWait) during XS matching
    // Can deadlock if we wait for an XS match on an owner that triggered this whenNode to run
    val newSS = scenarioStack.withFlag(EvaluationState.NO_WAIT_FOR_XS_NODE)
    node.attach(newSS)
    node
  }

  def writePrettyString(sb: PrettyStringBuilder): PrettyStringBuilder = {
    val predicateString = if (_tag ne null) _tag else "{ predicate... }"
    sb ++= "P:" ++= propertyInfo.entityInfo.toString ++= "." ++= propertyInfo.name ++= " when " + predicateString
  }
}
