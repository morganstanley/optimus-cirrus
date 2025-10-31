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

import optimus.graph.diagnostics.NodeName
import optimus.graph.{PropertyNode => PN}
import optimus.graph.{TweakNode => TN}
import optimus.platform._
import optimus.platform.util.{PrettyStringBuilder => PSB}
import optimus.platform.{ScenarioStack => SS}

abstract sealed class TweakTemplate(val computeGenerator: Any) extends Serializable {
  private[optimus] def isConstant: Boolean = resultIsStable
  def constantValue: Any = computeGenerator

  /** Returns true if the tweak is byName AND does NOT depend on the input key */
  def isReducibleToByValue: Boolean = false

  /** Returns true if the result is stable or SI */
  def resultIsScenarioIndependent: Boolean = resultIsStable

  /** Returns true if the result of evaluation of this tweak is a stable value (non-context dependent) */
  def resultIsStable: Boolean = false

  /** Returns RHS value if available and marker object otherwise */
  private[graph] def immediateResult: Any = if (isConstant) computeGenerator else TweakTemplate.UnknownValue

  def opName: String = ""

  /**
   * Add to builder the tweak RHS in pretty print of scenarios and tweaks
   */
  final def writePrettyString(sb: PSB, tweak: Tweak): PSB = {
    sb ++= ":" ++ opName ++ "=" ++ " "
    computeGenAsPrettyString(sb, tweak)
  }

  protected def computeGenAsStringHelper(sb: PSB): String = NodeName
    .fromSubProfile(computeGenerator)
    .toString(shortened = true, includeHint = sb.includeHint)
    .stripSuffix(NodeTaskInfo.StoredNodeFunction.rawName())

  protected def computeGenAsPrettyString(sb: PSB, tweak: Tweak): PSB = sb ++= computeGenerator.toString

  /** Extra location information for diagnostics if available */
  def location: StackTraceElement = null

  override def toString: String = writePrettyString(new PSB(), null).toString

  /**
   * Used in the context of Scenario and Tweak equality
   * Needs to check class due to the overrides for :*= :+= etc.
   */
  override def equals(that: Any): Boolean = that match {
    case tn: TweakTemplate if getClass == tn.getClass => computeGenerator == tn.computeGenerator
    case tn: TweakTemplateLazyVal                     => tn.constantValue == computeGenerator // see comments on LazyVal
    case _                                            => false
  }

  /** HashCode. Note: tweaking to null is supported */
  override def hashCode(): Int = if (computeGenerator == null) 0 else computeGenerator.hashCode()

  def createTweakNode[T](tweak: Tweak, requestingSS: SS, givenSS: SS, key: PN[T]): TN[T] = {
    val tn = new TN[T](tweak, givenSS, key)
    tn.initialize(requestingSS)
    tn
  }

  /** Creates a node to compute RHS of a tweak */
  def getComputeNode(evaluateInScenarioStack: ScenarioStack, key: PropertyNode[_]): Node[_] = {
    // For loom missing
    // case lnodeDef: LNodeFunction[_] => lnodeDef.toNodeWith(key)
    val computeNode = getComputeNode(key)
    if (computeNode.scenarioStack eq null) computeNode.replace(evaluateInScenarioStack.withCacheableTransitively)
    computeNode
  }

  def getComputeNode(key: PropertyNode[_]): Node[_] = throw new UnsupportedOperationException(
    s"getComputeNode not implemented for ${this.getClass.getName}")
}

final class TweakTemplateConstant(val v: Any) extends TweakTemplate(v) {
  override def resultIsStable: Boolean = true

  override def createTweakNode[T](tweak: Tweak, requestingSS: SS, givenSS: SS, key: PN[T]): TN[T] = {
    val tn = new TN[T](tweak, givenSS, key)
    tn.initWithValue(v, requestingSS)
    tn
  }

  // Note: only the case of ModifyOriginal!
  override def getComputeNode(evaluateIn: SS, key: PN[_]): Node[_] = new AlreadyCompletedNode(v)
}

final class TweakTemplateException(ex: Throwable) extends TweakTemplate(ex) {
  // One case where the result is stable BUT it's not a constant value
  private[optimus] override def isConstant: Boolean = false
  override def resultIsStable: Boolean = true

  override def createTweakNode[T](tweak: Tweak, requestingSS: SS, givenSS: SS, key: PN[T]): TN[T] = {
    val tn = new TN[T](tweak, givenSS, key)
    tn.initWithException(ex, requestingSS)
    tn
  }

  // Note: only the case of ModifyOriginal!
  override def getComputeNode(evaluateIn: SS, key: PN[_]): Node[_] = new AlreadyFailedNode(ex)
}

/**
 * It's basically a constant value that can be computed lazily from a provider
 * Currently only initialTime is using this feature, and it has a requirement that the value is computed (hydrated)
 * before it's ever sent to an engine. If another use case arises we can generalize this further.
 * Note: equals is not checking for providers to be of the same type and blindly hydrates two values.
 * Note: equals on Constant is testing against the lazy provider, it's just to make a very questionable test to pass
 * All of this is subject to change
 */
final class TweakTemplateLazyVal(provider: SITweakValueProvider) extends TweakTemplate(provider) {
  override def resultIsStable: Boolean = true

  override def constantValue: Any = provider.value

  override def createTweakNode[T](tweak: Tweak, requestingSS: SS, givenSS: SS, key: PN[T]): TN[T] = {
    val tn = new TN[T](tweak, givenSS, key)
    tn.initWithValue(constantValue, requestingSS)
    tn
  }

  /** The value might not be available yet, and we can't change the hashCode */
  override def hashCode(): Int = 1
  override def equals(obj: Any): Boolean = obj match {
    case that: TweakTemplateLazyVal => (this eq that) || constantValue == that.constantValue
    case _                          => false
  }

  override protected def computeGenAsPrettyString(sb: PSB, tweak: Tweak): PSB =
    sb ++= constantValue

  // it's important to fully initialize the value before sending over to an engine.
  // hydrating it not RT! And has to be done on a client.
  // noinspection ScalaUnusedSymbol
  private def writeReplace(): AnyRef = TweakTemplate(constantValue)
}

final class TweakTemplateByKey(cg: AnyRef) extends TweakTemplate(cg) {
  override def createTweakNode[T](tweak: Tweak, requestingSS: SS, givenSS: SS, key: PN[T]): TN[T] = {
    val tn = new TN[T](tweak, givenSS, key)
    tn.initialize(requestingSS)
    tn
  }

  override def getComputeNode(key: PN[_]): Node[_] = key.argsCopy(computeGenerator)
  override def hashCode(): Int = NodeClsIDSupport.hashCode(computeGenerator)
  override def equals(obj: Any): Boolean = obj match {
    case that: TweakTemplateByKey => NodeClsIDSupport.equals(computeGenerator, that.computeGenerator)
    case _                        => false
  }

  override protected def computeGenAsPrettyString(sb: PSB, tweak: Tweak): PSB = {
    sb ++= computeGenAsStringHelper(sb)
  }
  override def location: StackTraceElement = NodeName.asStackTraceElement(cg.getClass)
}

final class TweakTemplateByNode(vn: Node[_]) extends TweakTemplate(vn) {
  // Nodes can be executed once their context (aka evaluateInScenarioStack) is known
  override def isReducibleToByValue: Boolean = true

  override def getComputeNode(key: PN[_]): Node[_] = if (vn.isStable) vn else vn.cloneTask().asInstanceOf[Node[_]]
  override def hashCode(): Int = NodeClsIDSupport.hashCode(computeGenerator)
  override def equals(obj: Any): Boolean = obj match {
    case that: TweakTemplateByNode => NodeClsIDSupport.equals(computeGenerator, that.computeGenerator)
    case _                         => false
  }

  override protected def computeGenAsPrettyString(sb: PSB, tweak: Tweak): PSB = {
    if (vn.isDone) sb ++= vn.resultAsString()
    else sb ++= computeGenAsStringHelper(sb)
  }

  override def location: StackTraceElement = NodeName.asStackTraceElement(vn.getClass)
}

object TweakTemplate {
  private object UnknownValue

  private[graph] def clean(_computeGenerator: Any): TweakTemplate = _computeGenerator match {
    case node: Node[_] =>
      if (node.isDoneWithResult) new TweakTemplateConstant(node.resultObject())
      else if (node.isDoneWithException) new TweakTemplateException(node.exception())
      else new TweakTemplateByNode(node)
    case _ => new TweakTemplateConstant(_computeGenerator)
  }

  /**
   * Result always depends on a key (value to be modified)
   * Note: For the RHS of modify original we only support const/exception/node (not byKey or more rare cases
   */
  abstract sealed class TTModifyOriginal[K](orgTemplate: TweakTemplate) extends TweakTemplate(orgTemplate) {
    override def resultIsStable = false

    def this(computeGen: Any) = this(clean(computeGen))

    override def createTweakNode[T](tweak: Tweak, requestingSS: SS, givenSS: SS, key: PN[T]): TN[T] = {
      val tn = new TweakNodeModifyOriginal[T](tweak, givenSS, this.asInstanceOf[TTModifyOriginal[T]], key)
      tn.initialize(requestingSS)
      tn
    }

    /** Creates a node to compute RHS of a tweak */
    override def getComputeNode(evaluateInSS: SS, key: PN[_]): Node[_] = orgTemplate.getComputeNode(evaluateInSS, key)

    override protected def computeGenAsPrettyString(sb: PSB, tweak: Tweak): PSB =
      orgTemplate.computeGenAsPrettyString(sb, tweak)

    def op(src: K, mod: K): K
  }

  final class Plus[T: Numeric](computeGen: Any) extends TTModifyOriginal[T](computeGen) {
    override def opName: String = "+"
    override def op(src: T, mod: T): T = implicitly[Numeric[T]].plus(src, mod)
  }
  final class Minus[T: Numeric](computeGen: Any) extends TTModifyOriginal[T](computeGen) {
    override def opName: String = "-"
    override def op(src: T, mod: T): T = implicitly[Numeric[T]].minus(src, mod)
  }
  final class Times[T: Numeric](computeGen: Any) extends TTModifyOriginal[T](computeGen) {
    override def opName: String = "*"
    override def op(src: T, mod: T): T = implicitly[Numeric[T]].times(src, mod)
  }
  final class Div[T: Fractional](computeGen: Any) extends TTModifyOriginal[T](computeGen) {
    override def opName: String = "/"
    override def op(src: T, mod: T): T = implicitly[Fractional[T]].div(src, mod)
  }

  /** Note: we can't tweak node values, they are assumed internals (it's a good thing) */
  def apply(computeGenerator: Any): TweakTemplate = clean(computeGenerator)
  def byKey(vn: AnyRef): TweakTemplate = new TweakTemplateByKey(vn)
  def lazyVal(provider: SITweakValueProvider): TweakTemplate = new TweakTemplateLazyVal(provider)
}
