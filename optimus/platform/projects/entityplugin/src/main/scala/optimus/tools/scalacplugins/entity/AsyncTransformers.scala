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

import scala.tools.nsc.transform.TypingTransformers
import scala.tools.nsc.Global

trait AsyncTraversers {
  val global: Global

  import global._

  /** Walks the 'immediate' tree children, i.e. code that can undergo async callsite transform */
  abstract class AsyncTraverser extends Traverser {
    var stop = false
    final def break(): Unit = stop = true
    override def traverse(tree: Tree): Unit = {
      tree match {
        case _: DefDef | Function(_, _) | Template(_, _, _) =>
          ;
        case Apply(fun, args) =>
          visit(tree)
          if (!stop) {
            if ((fun.tpe eq null) || (args.length != fun.tpe.params.length))
              traverseTrees(args)
            else
              for ((param, arg) <- fun.tpe.params.zip(args)) {
                if (!param.isByNameParam) traverse(arg)
              }
            traverse(fun)
          }
        case _ =>
          visit(tree)
          if (!stop) super.traverse(tree)
      }
    }
    def visit(tree: Tree): Unit
  }

}

trait AsyncUtils { this: PluginUtils =>
  import global._

  lazy val NodeSyncAnnotation: ClassSymbol = rootMirror.getRequiredClass("optimus.platform.annotations.nodeSync")
  lazy val ParallelizableAnnotation: ClassSymbol =
    rootMirror.getRequiredClass("optimus.platform.annotations.parallelizable")
  lazy val SequentialAnnotation: ClassSymbol =
    rootMirror.getRequiredClass("optimus.platform.annotations.sequential")
  lazy val NodeSyncLiftAnnotation: ClassSymbol =
    rootMirror.getRequiredClass("optimus.platform.annotations.nodeSyncLift")
  lazy val ValAccessorAnnotation: ClassSymbol =
    rootMirror.getRequiredClass("optimus.platform.annotations.valAccessor")

  // optimus language options
  lazy val optimusLanguage: Symbol = rootMirror.getPackageIfDefined("optimus.language")
  object language {
    lazy val autopar: TermName = TermName("autopar")
  }

  // If the symbol is from a different module, then we expect to see the @nodeSync annotation. In this module,
  // we may see @nodeSync but we could also see the Node attachment without @nodeSync (if generatenodemethods
  // hasn't run for sym yet).
  def isNode(sym: Symbol): Boolean = sym.hasAnnotation(NodeSyncAnnotation) || sym.hasAttachment[Attachment.Node]

  // TODO (OPTIMUS-34257): The following is used only in auto-async; if
  // isNode, above, returns true for references to symbols of nodes, we get unexplained aborts in auto-async.
  def isNodeOrRef(sym: Symbol): Boolean =
    isNode(sym) || (sym.isTerm && sym.asTerm.referenced.exists && isNodeOrRef(sym.asTerm.referenced))

  def isNodeOrSyncLift(sym: Symbol): Boolean = isNode(sym) || sym.hasAnnotation(NodeSyncLiftAnnotation)
}

/*
 * Plugin support specific to async transform.
 */
trait AsyncTransformers extends AsyncUtils with AsyncTraversers with AsyncDefns {
  this: TypingTransformers with PluginUtils =>
  import global._

  abstract class AsyncCallsiteVisitor extends AsyncTraverser {
    final override def visit(tree: Tree): Unit = tree match {
      case _: Select if isNode(tree.symbol) =>
        nodeSyncCall(tree)
      case Apply(fun, _) if fun.symbol == FSM_await =>
        nodeSyncCall(tree)
      case _ =>
    }

    def nodeSyncCall(tree: Tree): Unit
  }

  object FSM {
    // NB: The type of the underlying 'tree' may actually be different than the 'tpe' supplied.
    // This happens due to the way typechecking works: the originally typed tree (the sync call)
    // may produce TypeSkolems, etc. that are different than the ones produced when typing the async call.
    // An alternative solution may be to rebuild the Apply chain without invoking the typer, but this
    // is a somewhat delicate process because the types of foo and foo$newNode may not actually be the same
    // modulo T vs. Node[T].  E.g. we may be a NullaryMethod(Node[T]) and the other a MethodType(Nil, T)
    // in the case of overriding.  It may be possible to fix up the types to match as there should not be any callsites
    // to the $newNode methods prior to the AsyncGraph phase (and if there are and we screw things up it's such a corner
    // case in the first place).  But then we still have to rewrite the (nested) MethodTypes for the call (which probably
    // isn't too hard).
    //
    // Note that scalac seems to do something similar internally to cope with the same issue around macro expansion.
    // See the long comment in Typers.scala around line 1233.
    def mkAttributedAwait(tpe: Type, tree: Tree): Tree = {
      val fsm = gen.mkAttributedRef(FSMModule)
      val typedAwait @ TypeApply(fun, _) = gen.mkAttributedTypeApply(fsm, FSM_await, tpe :: Nil)
      typedAwait.setType(fun.tpe.resultType.instantiateTypeParams(fun.tpe.typeParams, tpe :: Nil))
      atPos(tree.pos.makeTransparent)(Apply(typedAwait, tree :: Nil) setType tpe)
    }
  }
}

trait AsyncDefns {
  val global: Global
  import global._, definitions._, rootMirror._

  lazy val FSMModule: ModuleSymbol = getRequiredModule("optimus.core.FSM")
  lazy val FSM_await: TermSymbol = getMemberMethod(FSMModule, TermName("await"))
  lazy val AggressiveParScopeSym: TermSymbol =
    getMemberMethod(getRequiredClass("optimus.platform.AsyncCollectionHelpers"), newTermName("withAdvancedCodeMotion"))
  lazy val PluginDebugScopeSym: TermSymbol =
    getMemberMethod(getRequiredClass("optimus.platform.AsyncCollectionHelpers"), newTermName("pluginDebug"))
}
