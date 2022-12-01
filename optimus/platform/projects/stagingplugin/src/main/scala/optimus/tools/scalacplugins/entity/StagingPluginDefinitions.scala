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
package optimus.tools.scalacplugins.entity

import scala.tools.nsc.Global
import optimus.scalacompat.isAtLeastScala2_13

trait StagingPluginDefinitions {
  val global: Global
  import global._
  import definitions._
  import rootMirror.{getClassIfDefined, getRequiredClass, requiredClass}

  lazy val GenTraversableOnceClass = getClassIfDefined("scala.collection.GenTraversableOnce")
  lazy val GenTraversableOnce_to = getMemberIfDefined(GenTraversableOnceClass, TermName("to"))
  lazy val IterableOnceOpsClass = getClassIfDefined("scala.collection.IterableOnceOps")
  lazy val CanBuildFromClass = getClassIfDefined("scala.collection.generic.CanBuildFrom")
  lazy val IterableOnceOps_to = getMemberIfDefined(IterableOnceOpsClass, TermName("to"))
  lazy val StreamClass = getClassIfDefined("scala.collection.immutable.Stream")
  lazy val LazyListClass = getClassIfDefined("scala.collection.immutable.LazyList")
  lazy val Predef_augmentString = getMemberIfDefined(definitions.PredefModule, TermName("augmentString"))
  lazy val Predef_wrapString = getMemberIfDefined(definitions.PredefModule, TermName("wrapString"))

  lazy val Predef_fallbackStringCBF =
    definitions.getMemberIfDefined(PredefModule, TermName("fallbackStringCanBuildFrom"))

  lazy val ValAccessorAnnotation = getClassIfDefined("optimus.platform.annotations.valAccessor")
  lazy val CollectionMapClass = requiredClass[scala.collection.Map[Any, Any]]
  lazy val TraversableLike_++ =
    if (isAtLeastScala2_13) NoSymbol
    else
      definitions.getDecl(getRequiredClass("scala.collection.TraversableLike"), TermName("++").encodedName)

  private def oneArg(s: Symbol) = s.paramss match {
    case List(List(_)) => true
    case _             => false
  }
  lazy val OptimusDoubleBuilderClass = getClassIfDefined("optimus.collection.OptimusDoubleBuilder")
  lazy val GrowablePlusEquals =
    getClassIfDefined("scala.collection.mutable.Growable").info.member(TermName("+=").encode).filter(oneArg)

  def isValAccessor(sym: Symbol) = ValAccessorAnnotation.exists && sym.hasAnnotation(ValAccessorAnnotation)

  lazy val nullaryIn213: Set[Symbol] = {
    val numericOps = definitions.getMemberClass(requiredClass[scala.math.Numeric[_]], TypeName("Ops"))
    Set(nme.UNARY_-.toString, "abs", "signum", "toInt", "toLong", "toFloat", "toDouble").map(n =>
      getMemberMethod(numericOps, TermName(n)): Symbol)
  } + getMemberMethod(requiredClass[scala.math.ScalaNumericConversions], TermName("underlying")) ++ {
    val numConv = requiredClass[scala.math.ScalaNumericAnyConversions]
    Set("isWhole", "underlying", "byteValue", "shortValue", "intValue", "longValue", "floatValue", "doubleValue").map(
      n => getMemberMethod(numConv, TermName(n)))
  } ++ {
    val bd = requiredClass[scala.math.BigDecimal]
    Set("toBigInt", "toBigIntExact").map(n => getMemberMethod(bd, TermName(n)))
  } + getMemberMethod(requiredClass[scala.concurrent.duration.Duration], TermName("isFinite"))

  lazy val nullaryIn213Names = nullaryIn213.map(_.name)

  lazy val allowAutoApplication = Set(
    definitions.Any_##,
    definitions.Any_toString,
    definitions.Any_hashCode
  ) ++ nullaryIn213

  lazy val allowAutoApplicationNames = allowAutoApplication.map(_.name)

  // Extractor for Apply and TypeApply. Applied.unapply matches any tree, not just applications
  object Application {
    def unapply(t: GenericApply): Some[(Tree, List[Tree], List[List[Tree]])] = {
      val applied = treeInfo.dissectApplied(t)
      Some((applied.core, applied.targs, applied.argss))
    }
  }

  def foreachParamsAndArgs(params: List[Symbol], args: List[Tree])(f: (Symbol, Tree) => Unit): Unit = args match {
    case Nil =>
    case a :: as =>
      params match {
        case p :: Nil => f(p, a); foreachParamsAndArgs(params, as)(f)
        case p :: ps  => f(p, a); foreachParamsAndArgs(ps, as)(f)
        case Nil      => throw new IllegalStateException() // empty params, nonempty args?
      }
  }
}
