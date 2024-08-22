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
package optimus.tools.scalacplugins.entity.refchecks
import optimus.tools.scalacplugins.entity.TypedUtils
import optimus.tools.scalacplugins.entity.reporter.OptimusErrors
import optimus.tools.scalacplugins.entity.staged

import scala.annotation.tailrec

trait IteratorUsage extends TypedUtils {
  import global._

  object OptimusIteratorLinter {
    private val ChunkRegex = """([A-Z]|^)?[a-z]*""".r
    private lazy val ioSource = rootMirror.getClassIfDefined("scala.io.Source")
    private lazy val optimusProvider = rootMirror.getClassIfDefined("optimus.dsi.base.maintenance.Provider")

    private object ValueApplication {
      def unapply(t: Apply): Some[(Tree, Symbol, List[List[Tree]])] = {
        val applied = treeInfo.dissectApplied(t)
        Some((applied.callee, applied.core.symbol, applied.argss))
      }
    }
  }

  class OptimusIteratorLinter extends Traverser {
    import OptimusIteratorLinter._
    import global._
    import global.{definitions => d}
    import staged.scalaVersionRange

    // type.find, but skip over refinements.
    // We consider references to Iterator in refinements accidental, for example
    //   val x = new T { val it = someIterator; def impl = it.something }
    // The type of `x` is `T { val it: Iterator }`
    def skipRefinementsFindTypeCollector(p: Type => Boolean): FindTypeCollector =
      if (scalaVersionRange("2.13:"): @staged) {
        new FindTypeCollector(p) {
          override def apply(tp: Type): Unit = tp match {
            case RefinedType(parents, _) => parents.foreach(super.apply)
            case _                       => super.apply(tp)
          }
        }
      } else {
        new FindTypeCollector(p) {
          override def traverse(tp: Type): Unit = tp match {
            case RefinedType(parents, _) => parents.foreach(super.traverse)
            case _                       => super.traverse(tp)
          }
        }
      }

    private def typeExists(tp: Type, pred: Type => Boolean): Boolean =
      skipRefinementsFindTypeCollector(pred).collect(tp).nonEmpty

    private def ignoredIterator(sym: Symbol): Boolean =
      sym.isNonBottomSubClass(ioSource) || sym.isNonBottomSubClass(optimusProvider)
    private def refersToIterator(tp: Type): Boolean = typeExists(
      tp,
      t => {
        val sym = t.typeSymbol
        sym.isNonBottomSubClass(d.IteratorClass) && !ignoredIterator(sym)
      })

    private def refersToIterableOnce(tp: Type): Boolean =
      typeExists(tp, _.typeSymbol.isNonBottomSubClass(GenTraversableOnceClass))

    // foreach has signature [U](f: A => U): U, instead of Unit.
    // range.foreach(map.put(x, iterator)) has param type x => Iterator, so it looks like an iterator could escape
    private def nonEscapingMethod(sym: Symbol): Boolean = sym.name == nme.foreach

    private def exprNameOK(tree: Tree): Boolean = tree match {
      case Ident(n)     => nameOK(n)
      case Select(_, n) => nameOK(n)
      case _            => false
    }

    private def nameOK(name: Name): Boolean = {
      val chunks = ChunkRegex.findAllIn(name.toString).filterNot(_.isEmpty).toList
      chunks.exists(chunk => chunk.startsWith("it") || chunk.startsWith("It"))
    }

    private val extractLocalHint = "Refactor the expression into a local val or def with Iterator/Iter/It in its name."

    private def warn(pos: Position, tp: Type, hint: String): Unit =
      alarm(OptimusErrors.ITERATOR_ESCAPES, pos, tp.toString, hint)

    private var enclosingMethod: DefDef = null

    private def checkDefResult(defSym: Symbol, defTpt: Tree, expr: Tree): Unit = {
      // Definitions where the rhs has type iterator need to
      //   - have "it" or "iterator" in their name, or
      //   - have an explicit type referring to `Iterator`, or
      //   - override another definition
      // This applies to local variables, even though such iterators don't escape. In this case,
      // the goal is to prevent accidental iterators to be used multiple times.
      val iteratorOK = nameOK(defSym.name) || !refersToIterator(expr.tpe) || {
        val tptRefersIterator = refersToIterableOnce(defSym.info.finalResultType)
        tptRefersIterator && {
          val wasExplicitType = defTpt match {
            case tt: TypeTree => tt.original != null
            case _            => false
          }
          val isOverride = defSym.overrideChain.drop(1).nonEmpty
          wasExplicitType || isOverride
        }
      }
      if (!iteratorOK)
        warn(
          expr.pos,
          expr.tpe,
          s"Add an explicit type to ${defSym.toString} or change its name to include Iterator/Iter/It.")
    }

    override def traverse(tree: Tree): Unit = tree match {
      case _: SymTree if tree.symbol.isSynthetic || tree.symbol.isAccessor || tree.symbol.isArtifact =>

      case df: ValOrDefDef =>
        checkDefResult(df.symbol, df.tpt, df.rhs)
        df match {
          case dd: DefDef =>
            val saved = enclosingMethod
            enclosingMethod = dd
            try super.traverse(tree)
            finally enclosingMethod = saved

          case _ =>
            super.traverse(tree)
        }

      case ValueApplication(valueFun, funSym, argss) =>
        // Application of a function to values. The function might be a TypeApply, e.g. in `List.apply(iterator)`
        // we get an inferred `TypeApply(List.apply, Iterator[_])`. This TypeApply's `tpe` (i.e., `valueFun.tpe`)
        // is instantiated, so in the example a `MethodType(Iterator[_]*, List[Iterator[_]])`.

        // If the function result type refers to Iterator (e.g., List[Iterator[_]]), we don't check the arguments
        // passed to the function. We assume that the iterator arguments are in some way included in the result.
        // It doesn't matter if the `Iterator` part of the result type was inferred or explicit.
        // Example: `List.apply(iterator)`, no warning on the argument. Escape checking continues with the
        // resulting `List[Iterator]`, so it depends where the result goes.
        if (!refersToIterableOnce(valueFun.tpe.finalResultType) && !nonEscapingMethod(funSym)) {
          // Given `def consume[A](a: A): Unit`, there's a warning for `consume(iterator)`. But
          // not if the type argument is explicit: `consume[Iterator[_]](iterator)`.
          val explicitIteratorTarg = valueFun match {
            case TypeApply(_, targs) =>
              targs.exists({
                case tt: TypeTree => tt.original != null && refersToIterableOnce(tt.tpe)
                case _            => false
              })
            case _ => false
          }
          if (!explicitIteratorTarg) {
            // Here we compute the type of the invoked function as seen from its prefix. Examples
            //   - `(m: Map[Int, Iterator[_]]).put(0, iterator)`
            //     method `put` has type `(Int, Iterator[_]): Option[Iterator[_]]`
            //   - `def consume[A](a: A): Unit`
            //     the type of `consume` is `[A](a: A): Unit`
            // Since `put` has parameter type `Iterator`, no warning is issued when passing an iterator to it.
            // For `consume`, a warning is issued if an iterator is passed.
            val funTpe = {
              @tailrec
              def loop(fun: Tree): Type = fun match {
                case TypeApply(fun, _) => loop(fun)
                case Select(qual, _)   => qual.tpe.memberType(fun.symbol)
                case _                 => fun.symbol.tpe
              }
              loop(valueFun)
            }
            foreach2(funTpe.paramss, argss) { (params, args) =>
              foreachParamsAndArgs(params, args) { (param, arg) =>
                if (!exprNameOK(arg)) {
                  val paramTp = d.dropByName(d.dropRepeated(param.tpe))
                  if (refersToIterator(arg.tpe) && !refersToIterableOnce(paramTp))
                    warn(arg.pos, arg.tpe, extractLocalHint)
                }
              }
            }
          }
        }
        traverse(valueFun)
        traverseTreess(argss)

      case Return(expr) =>
        if (enclosingMethod != null && tree.symbol == enclosingMethod.symbol)
          checkDefResult(enclosingMethod.symbol, enclosingMethod.tpt, expr)
        super.traverse(tree)

      case Assign(lhs, expr) =>
        if (!exprNameOK(expr) && !refersToIterableOnce(lhs.tpe) && refersToIterator(expr.tpe))
          warn(expr.pos, expr.tpe, extractLocalHint)

      case _ =>
        super.traverse(tree)
    }
  }
}
