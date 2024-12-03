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
import optimus.platform._
import optimus.platform.annotations._
import optimus.platform.util.PrettyStringBuilder

/**
 * Identifies target of a scenario Tweak, and provides a DSL for creating a Tweak to this TweakTarget.
 */
trait TweakTarget[R, SetT] {
  def propertyInfo: NodeTaskInfo

  /**
   * Returns true if this TweakTarget matches the given NodeKey recorded earlier in the RecordingScenarioStack.
   */
  def matchesKeyFromRSS(key: NodeKey[_]): Boolean

  /** Returns true for PropertyTweaks with 'when' clause */
  def hasWhenClause = false

  /** Returns predicate for PropertyTweaks with `when` clause, else null */
  def whenPredicate: AnyRef = null

  /** Returns whenClause as a node to be evaluated for PropertyTweaks with 'when' clause. Null otherwise */
  def whenClauseNode(key: NodeKey[_], scenarioStack: ScenarioStack): Node[Boolean] = null

  /** Only InstanceTweaks can have tweak handlers (aka also sets) and can be unresolved */
  def unresolved = false

  final private[optimus] def isMarker: Boolean = propertyInfo eq dal.MarkerTaskInfo
  final private[optimus] def isUnresolvedOrMarkerTweak = unresolved || isMarker

  /**
   * For plain old instance tweaks this returns the PropertyNode hiding as NodeKey
   * and for old plain property tweaks and extractor tweaks, it will return null
   */
  def hashKey: NodeKey[_] = null // Can be used as the direct
  /** Simple boring instance tweak On ACPN without also-sets */
  final def fullSpecified: Boolean = hashKey.isInstanceOf[AlreadyCompletedPropertyNode[_]] && !unresolved

  private[this] def makeTweak(tweakTemplate: TweakNode[R]) = new Tweak(this, tweakTemplate)
  protected def mkPlus(vn: AnyRef)(implicit ev: Numeric[R]): TweakNode[R] =
    new TweakNodeModifyOriginal.Plus[R](vn)
  protected def mkMinus(vn: AnyRef)(implicit ev: Numeric[R]): TweakNode[R] =
    new TweakNodeModifyOriginal.Minus[R](vn)
  protected def mkTimes(vn: AnyRef)(implicit ev: Numeric[R]): TweakNode[R] =
    new TweakNodeModifyOriginal.Times[R](vn)
  protected def mkDiv(vn: AnyRef)(implicit ev: Fractional[R]): TweakNode[R] =
    new TweakNodeModifyOriginal.Div[R](vn)

  @tweakOperator
  @nodeSyncLift
  def :=(@nodeLift @withNodeClassID value: => R): Tweak = makeTweak(new TweakNode[R](mkCompute(value _)))
  // noinspection ScalaUnusedSymbol
  def $colon$eq$withValue(value: R): Tweak = makeTweak(new TweakNode(new AlreadyCompletedNode(value)))
  def $colon$eq$withNode(vn: Node[R]): Tweak = makeTweak(new TweakNode[R](vn))

  @tweakOperator
  @nodeSyncLift
  def :=(@nodeLift @withNodeClassID f: SetT): Tweak = makeTweak(new TweakNode[R](f.asInstanceOf[AnyRef]))
  def $colon$eq$withNode(vn: AnyRef /* FunctionN(... Node[R])*/ ): Tweak = makeTweak(new TweakNode[R](vn))

  @tweakOperator
  @nodeSyncLift
  def :+=(@nodeLift @withNodeClassID value: => R)(implicit ev: Numeric[R]): Tweak = makeTweak(
    mkPlus(mkCompute(value _)))
  // noinspection ScalaUnusedSymbol
  def $colon$plus$eq$withValue(value: R)(implicit ev: Numeric[R]): Tweak = makeTweak(
    mkPlus(new AlreadyCompletedNode(value))(ev))
  def $colon$plus$eq$withNode(vn: Node[R])(implicit ev: Numeric[R]): Tweak = makeTweak(mkPlus(vn))

  @tweakOperator
  @nodeSyncLift
  def :-=(@nodeLift @withNodeClassID value: => R)(implicit ev: Numeric[R]): Tweak =
    makeTweak(mkMinus(mkCompute(value _)))
  // noinspection ScalaUnusedSymbol
  def $colon$minus$eq$withValue(value: R)(implicit ev: Numeric[R]): Tweak =
    makeTweak(mkMinus(new AlreadyCompletedNode(value)))
  def $colon$minus$eq$withNode(vn: Node[R])(implicit ev: Numeric[R]): Tweak = makeTweak(mkMinus(vn))

  @tweakOperator
  @nodeSyncLift
  def :*=(@nodeLift @withNodeClassID value: => R)(implicit ev: Numeric[R]): Tweak =
    makeTweak(mkTimes(mkCompute(value _)))
  // noinspection ScalaUnusedSymbol
  def $colon$times$eq$withValue(value: R)(implicit ev: Numeric[R]): Tweak =
    makeTweak(mkTimes(new AlreadyCompletedNode(value)))
  def $colon$times$eq$withNode(vn: Node[R])(implicit ev: Numeric[R]): Tweak = makeTweak(mkTimes(vn))

  @tweakOperator
  @nodeSyncLift
  def :/=(@nodeLift @withNodeClassID value: => R)(implicit ev: Fractional[R]): Tweak =
    makeTweak(mkDiv(mkCompute(value _)))
  // noinspection ScalaUnusedSymbol
  def $colon$div$eq$withValue(value: R)(implicit ev: Fractional[R]): Tweak =
    makeTweak(mkDiv(new AlreadyCompletedNode(value)))
  def $colon$div$eq$withNode(vn: Node[R])(implicit ev: Fractional[R]): Tweak = makeTweak(mkDiv(vn))

  override def toString: String = { writePrettyString(new PrettyStringBuilder()).toString }

  def writePrettyString(builder: PrettyStringBuilder): PrettyStringBuilder
}

trait TweakTargetKey[R, SetT] extends TweakTarget[R, SetT] {
  def key: NodeKey[R]
}
