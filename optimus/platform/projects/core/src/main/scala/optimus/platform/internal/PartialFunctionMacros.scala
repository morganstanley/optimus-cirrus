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
package optimus.platform.internal

import optimus.exceptions.RTExceptionInterface
import optimus.exceptions.RTExceptionTrait
import optimus.platform.NodeTry

import optimus.platform.OptimusPartialFunction
import optimus.graph.Node
import optimus.tools.scalacplugins.entity.reporter.PartialFunctionAlarms
import optimus.utils.MacroUtils

import optimus.scalacompat.collection.BuildFrom
import scala.reflect.macros.blackbox

final class PartialFunctionMacro(val c: blackbox.Context) {
  import c.universe._

  def pfToOpf[T1: WeakTypeTag, R: WeakTypeTag](
      pf: Expr[PartialFunction[T1, R]]
  ): Expr[OptimusPartialFunction[T1, R]] = {
    val matches =
      pf.tree.collect { case DefDef(_, TermName("applyOrElse"), _, _, _, Match(_, cases)) =>
        cases.filter {
          // filter out the auto-generated "defaultCase$ @ _", since we add one back in explicitly later in resTemplate
          case CaseDef(Bind(TermName("defaultCase$"), _), _, _) => false
          case _                                                => true
        }
      }

    val partialCases = matches.headOption.getOrElse {
      OptimusReporter.abort(c, PartialFunctionAlarms.NON_PARTIAL_FUNCTION)(c.macroApplication.pos, pf.tree)
    }

    // _ bound to defaultCase$ below to avoid an "unreachable code" error is someone puts a `case _` in their
    // partial function. "defaultCase$" is a magic variable name which is treated specially by the scala compiler;
    // unfortunately it's not available as a constant outside of the compiler universe, so we have to hardcode it here.
    val resTemplate = reify {
      new OptimusPartialFunction[T1, R] {
        final override def apply$_newNode(x: T1): Node[R] = optimus.core.CoreAPI.nodify[R] {
          (x: @unchecked) match {
            case defaultCase$ @ _ => throw new MatchError(x)
          }
        }

        final override protected def isDefinedAt$_newNode(x: T1): Node[Boolean] = optimus.core.CoreAPI.nodify[Boolean] {
          (x: @unchecked) match {
            case defaultCase$ @ _ => false
          }
        }
      }
    }

    val transformer = new Transformer {
      var _methodName: String = _

      private def changeRHS(cs: CaseDef): CaseDef = {
        treeCopy.CaseDef(cs, cs.pat, cs.guard, Literal(Constant(true)))
      }
      override def transform(tree: Tree): Tree = tree match {
        case dd: DefDef =>
          _methodName = dd.name.toString
          super.transform(tree)

        case mat @ Match(sel, cases) =>
          if (_methodName.startsWith("isDefinedAt$"))
            c.untypecheck(
              treeCopy.Match(
                mat,
                mat.selector,
                (partialCases map { cs =>
                  changeRHS(cs)
                }) ::: mat.cases))
          else if (_methodName.startsWith("apply$"))
            c.untypecheck(treeCopy.Match(mat, mat.selector, partialCases ::: mat.cases))
          else OptimusReporter.abort(c, PartialFunctionAlarms.ERROR_TRANSFORM)(c.macroApplication.pos, tree)
        case _ => super.transform(tree)
      }
    }

    val res = transformer.transform(resTemplate.tree)
    c.Expr[OptimusPartialFunction[T1, R]](res)
  }

  // Unearth exceptions that we don't think can be caught in a referentially transparent manner.
  private def findNaughtyExceptions(tree: Tree): Unit = {
    lazy val rtExceptionModuleSym = rootMirror.staticModule("optimus.exceptions.RTException")

    val patTraverser = new Traverser {
      var withinRTExceptionUnapply = false
      override def traverse(tree: Tree): Unit = tree match {
        case Bind(_, pat)       => traverse(pat)
        case Alternative(trees) => trees.foreach(traverse(_))
        case UnApply(Apply(TypeApply(Select(qual, TermName("unapply")), _throwable), _match), arg :: Nil)
            if qual.symbol == rtExceptionModuleSym =>
          val prev = withinRTExceptionUnapply
          withinRTExceptionUnapply = true
          traverse(arg)
          withinRTExceptionUnapply = prev
        case UnApply(Apply(Select(qual, TermName("unapply")), _), arg :: Nil) if qual.symbol == rtExceptionModuleSym =>
          val prev = withinRTExceptionUnapply
          withinRTExceptionUnapply = true
          traverse(arg)
          withinRTExceptionUnapply = prev
        case UnApply(_, args) =>
          if (withinRTExceptionUnapply)
            OptimusReporter.alarm(c, PartialFunctionAlarms.NESTED_UNAPPLY)(tree.pos)
          else
            args.foreach(traverse(_))
        case Typed(_, tpt) if tpt.tpe =:= typeOf[RTExceptionTrait] || tpt.tpe =:= typeOf[RTExceptionInterface] =>
          OptimusReporter.alarm(c, PartialFunctionAlarms.BROAD_EXCEPTION)(tree.pos)
        case Typed(_, tpt) if tpt.tpe <:< typeOf[RTExceptionTrait]            => // ok
        case Typed(_, tpt) if tpt.tpe <:< typeOf[RTExceptionInterface]        => // ok
        case Typed(_, tpt) if MacroUtils.isRtException(c)(tpt.tpe.typeSymbol) => // ok
        case Typed(_, tpt) =>
          OptimusReporter.abort(c, PartialFunctionAlarms.BAD_EXCEPTION)(tpt.pos, tpt.tpe.toString())
        // Uncomment if we wish to return to deny-list
        //     fatals.foreach {
        //        tp => if(tpt.tpe <:< tp) OptimusReporter.abort(c,PartialFunctionAlarms.BAD_EXCEPTION,tpt.pos,errorString.toString)
        //      }
        //      roots.foreach {
        //         tp => if (tpt.tpe =:= tp)
        //         OptimusReporter.abort(c,PartialFunctionAlarms.BAD_EXCEPTION,tpt.pos,errorString.toString)
        //       }
        case t if t.tpe <:< typeOf[RTExceptionTrait] => // ok
        case i @ (Ident(_) | Select(_, _)) =>
          if (!withinRTExceptionUnapply)
            OptimusReporter.abort(c, PartialFunctionAlarms.UNSPECIFIED_EXCEPTION)(i.pos)
        case x =>
          OptimusReporter.abort(c, PartialFunctionAlarms.ERROR_TRANSFORM)(x.pos, showRaw(x))
        // Uncomment if we wish to go back to deny-listing.
        // case _ => ok
      }
    }

    val traverser = new Traverser {
      override def traverse(tree: Tree): Unit = tree match {
        case CaseDef(pat, guard, body) if pat.tpe <:< typeOf[optimus.exceptions.ExceptionProxy] =>
        // Acceptable
        case CaseDef(Bind(TermName("defaultCase$"), _), _, _) =>
        // ok
        case CaseDef(pat, guard, body) =>
          patTraverser.traverse(pat)
        case _ => super.traverse(tree)
      }
    }
    traverser.traverse(tree)
  }

  def collectTransform[CC <: Iterable[A]: WeakTypeTag, A: WeakTypeTag, B: WeakTypeTag, That: WeakTypeTag](
      pf: Expr[PartialFunction[A, B]])(cbf: Expr[BuildFrom[CC, B, That]]): Expr[That] = {
    val opf = pfToOpf(pf)
    val caller = c.prefix.asInstanceOf[Expr[optimus.platform.AsyncBase[A, CC]]]
    MacroUtils.typecheckAndValidateExpr(c)(reify(caller.splice.collectPF(opf.splice)(cbf.splice)))
  }

  def collectFirstTransform[CC <: Iterable[A]: WeakTypeTag, A: WeakTypeTag, B: WeakTypeTag](
      pf: Expr[PartialFunction[A, B]]): Expr[Option[B]] = {
    val opf = pfToOpf(pf)
    val caller = c.prefix.asInstanceOf[Expr[optimus.platform.AsyncBase[A, CC]]]
    MacroUtils.typecheckAndValidateExpr(c)(reify(caller.splice.collectFirstPF(opf.splice)))
  }

  def collectTransformOpt[A: WeakTypeTag, B: WeakTypeTag](pf: Expr[PartialFunction[A, B]]): Expr[Option[B]] = {
    val opf = pfToOpf(pf)
    val caller = c.prefix.asInstanceOf[c.Expr[optimus.platform.OptAsync[A]]]
    MacroUtils.typecheckAndValidateExpr(c)(reify(caller.splice.collectPF(opf.splice)))
  }

  def genRecoverTransform[T: WeakTypeTag, I: WeakTypeTag, R: WeakTypeTag](
      pf: Expr[PartialFunction[Throwable, I]],
      method: String): Expr[R] = {
    findNaughtyExceptions(pf.tree)
    val opf = pfToOpf(pf)
    val meth = TermName(method)
    val caller = c.prefix
    val nodeTryImplType = weakTypeOf[optimus.platform.NodeTryImpl[T]]
    c.Expr(
      MacroUtils.typecheckAndValidate(c)(
        q"$caller.asInstanceOf[$nodeTryImplType].$meth(${MacroUtils.splice(c)(opf.tree)})"))
  }

  def recoverTransform[T: WeakTypeTag, U >: T: WeakTypeTag](pf: Expr[PartialFunction[Throwable, U]]) =
    genRecoverTransform[T, U, NodeTry[U]](pf, "recoverPF")
  def recoverWithTransform[T: WeakTypeTag, U >: T: WeakTypeTag](pf: Expr[PartialFunction[Throwable, NodeTry[U]]]) =
    genRecoverTransform[T, NodeTry[U], NodeTry[U]](pf, "recoverWithPF")
  def recoverAndGetTransform[T: WeakTypeTag, U >: T: WeakTypeTag](pf: Expr[PartialFunction[Throwable, U]]) =
    genRecoverTransform[T, U, U](pf, "recoverAndGetPF")

}
