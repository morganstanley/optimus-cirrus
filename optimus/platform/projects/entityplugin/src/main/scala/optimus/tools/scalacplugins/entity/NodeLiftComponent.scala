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

import scala.tools.nsc.transform.{Transform, TypingTransformers}
import optimus.tools.scalacplugins.entity.reporter.OptimusErrors

import scala.annotation.tailrec
import scala.reflect.internal.util.TriState

/*
 * Find all the method calls with @nodeLift annotation and replace with method$node method
 * Arguments will be replaced with arg$node
 */
class NodeLiftComponent(val plugin: EntityPlugin, val phaseInfo: OptimusPhaseInfo)
    extends EntityPluginComponent
    with Transform
    with TransformGrep
    with TypingTransformers
    with TypedUtils
    with TreeDuplicator
    with WithOptimusPhase {
  import global._

  def newTransformer0(unit: CompilationUnit) = new NodeLift(unit)

  class NodeLift(unit: CompilationUnit) extends TypingTransformer(unit) {
    import global._

    private var expectingTweaks = false
    private var byValueNodeLift: TriState = TriState.Unknown
    @inline final def atApply(tree: Tree, fun: Tree)(f: => Tree): Tree = {
      val lastExpectingTweaks = expectingTweaks
      val lastByValueNodeLift = byValueNodeLift
      if (!fun.isInstanceOf[Apply] && tree.symbol.hasAnnotation(ExpectingTweaksAnnotation))
        expectingTweaks = true
      if (tree.symbol.hasAnnotation(NodeLiftByValueAnnotation))
        byValueNodeLift = true
      else if (tree.symbol.hasAnnotation(NodeLiftByNameAnnotation))
        byValueNodeLift = false

      val result = f

      expectingTweaks = lastExpectingTweaks // Restore on the way out
      byValueNodeLift = lastByValueNodeLift
      result
    }

    override def transform(tree: Tree): Tree = {
      try { transformSafe(tree) }
      catch {
        case ex: Throwable =>
          ex.printStackTrace()
          alarm(OptimusErrors.TRANSFORM_ERROR5, tree.pos, s"$phaseName", s"$ex")
          null
      }
    }

    def errorOnNotTweakable(arg: Tree): Unit = {
      if (!isTweakable(arg)) // Verify that nodeLifts are allowed in scoped arguments only on tweakable properties
        alarm(OptimusErrors.LIFT_NONTWEAK, arg.pos, arg.symbol.fullLocationString)
    }

    private def mkSelectNode(arg: Select, tree: Tree, queued: Boolean): Tree = {
      val Select(q, name) = arg
      val getNodeName = if (queued) mkGetNodeName(arg.symbol.name) else mkCreateNodeName(arg.symbol.name)
      val decl = arg.symbol.owner.tpe.decl(getNodeName)
      val ok = (decl ne NoSymbol) && (queued || decl.info.finalResultType.typeSymbol.isSubClass(NodeKey))
      if (!ok) {
        if (expectingTweaks) alarm(OptimusErrors.LIFT_NONTWEAK, tree.pos, arg)
        else if (tree.symbol == Value2TweakTarget) alarm(OptimusErrors.NODELIFT_UNEXPECTED_VALUE2TT, tree.pos, arg)
        else if (isNode(arg.symbol)) alarm(OptimusErrors.LIFT_NONPROP, arg.pos, q, name)
        else alarm(OptimusErrors.LIFT_NONNODE, arg.pos, q, name)
        EmptyTree
      } else {
        // we do have a node, and we're about to use it as a tweak target; check if it is tweakable
        if (expectingTweaks) errorOnNotTweakable(arg)
        Select(q, decl).setPos(arg.pos)
      }
    }

    private def newName(name: Name, suffix: String): TermName = newTermName("" + name + suffix)

    def nodeLift(tree: Tree, fun: Tree, args: List[Tree], queued: Boolean): Tree = {
      @tailrec
      def extractSelectAndArgs(tree: Tree, argsSoFar: List[Tree]): (Tree, List[Tree], List[Tree]) =
        tree match {
          case Apply(t, a)                   => extractSelectAndArgs(t, a ::: argsSoFar)
          case TypeApply(sel: Select, targs) => (sel, targs, argsSoFar)
          case sel: Select                   => (sel, Nil, args)
        }
      val (Select(qual, funName), targs, allArgs) = extractSelectAndArgs(fun, args)
      val newFuncName = newName(funName, if (queued) suffixes.NODE_QUEUED else suffixes.NODE_CLASS)
      val newSel = Select(transform(qual), newFuncName).setPos(qual.pos)
      val newFun = if (targs.nonEmpty) { TypeApply(newSel, targs).setPos(targs.head.pos) }
      else newSel
      val newFuncSymAlternatives = tree.symbol.owner.info.decl(newFuncName)
      if (newFuncSymAlternatives == NoSymbol) {
        alarm(OptimusErrors.NODELIFT_NOMATCH, tree.pos, newFuncName)
        tree
      } else {
        newFuncSymAlternatives.suchThat(s => s.tpe.paramss.size == 1 && s.tpe.params.size == allArgs.size) match {
          case NoSymbol =>
            alarm(
              OptimusErrors.NODELIFT_NOMATCH,
              tree.pos,
              s"$newFuncName (it should have one parameter list with ${allArgs.size} parameter(s))")
            tree
          case newFuncSym =>
            val newArgs = map2(newFuncSym.info.params, allArgs) { (param, arg) =>
              if (
                (queued && !param.tpe.typeSymbol.isSubClass(Node)) ||
                (!queued && !param.tpe.typeSymbol.isSubClass(NodeKey))
              ) arg
              else
                localTyper.typed(arg match {
                  case treeInfo.Applied(sel: Select, targs, vargss) =>
                    val newArgss = vargss mapConserve transformTrees
                    val selNode = mkSelectNode(sel, tree, queued)
                    if (selNode == EmptyTree) arg.setType(definitions.NothingTpe)
                    else
                      atPos(arg.pos) {
                        newArgss.foldLeft(gen.mkTypeApply(selNode, targs))(Apply.apply)
                      }
                  case _ =>
                    val error =
                      if (expectingTweaks) OptimusErrors.LIFT_NONTWEAK
                      else if (tree.symbol == Value2TweakTarget) OptimusErrors.NODELIFT_UNEXPECTED_VALUE2TT
                      else OptimusErrors.NODELIFT_UNSUPPORT
                    alarm(error, tree.pos, arg)
                    arg.setType(definitions.NothingTpe)
                })
            }

            localTyper.typed(Apply(newFun, newArgs).setPos(tree.pos))
        }
      }
    }

    abstract class ApplyUpdater(strict: Boolean) {
      def updateTyped(tree: Tree): Tree = localTyper.typed(update(tree))

      def update(tree: Tree): Tree = tree match {
        case Apply(fun, args)             => Apply(update(fun), transformTrees(args))
        case TypeApply(sel: Select, args) => TypeApply(update(sel), transformTrees(args))
        case sel: Select =>
          val param = sel.tpe.params.head // the tweak :*= API always has exactly one param!
          if (!param.isByNameParam) {
            if (strict) alarm(OptimusErrors.CAN_CONVERT_TO_BYVALUE, tree.pos)
            sel
          } else update(transform(sel.qualifier), sel.name)
      }
      def update(qual: Tree, name: Name): Select
    }

    def transformSafe(tree: Tree): Tree = tree match {
      case cd @ (_: ClassDef | _: ModuleDef) => atAsyncOffScope(cd)(super.transform(cd))
      case Apply(fun, args) =>
        atApply(tree, fun) {

          val singleArg = args.nonEmpty && args.tail.isEmpty
          if (singleArg && tree.symbol.hasAnnotation(OnlyTweakableArgAnnotation))
            errorOnNotTweakable(args.head)

          if (curInfo.isLoom && !currentClass.isAnonymousClass && tree.symbol.hasAnnotation(NodeSyncLiftAnnotation)) {
            addLoomIfMissing(currentClass)
          }
          if (hasNodeLiftAnno(tree.symbol)) {
            nodeLift(tree, fun, args, queued = false)
          } else if (tree.symbol.hasAnnotation(NodeLiftQueuedAnnotation)) {
            nodeLift(tree, fun, args, queued = true)
          } else if (fun.symbol.hasAnnotation(TweakOperatorAnnotation)) {
            if (!expectingTweaks)
              alarm(OptimusErrors.USE_TWEAK_OPT_DIRECT, fun.pos)

            if (byValueNodeLift != TriState.False) {
              new ApplyUpdater(strict = byValueNodeLift.isKnown) {
                def update(qual: Tree, name: Name): Select = Select(qual, newName(name, suffixes.WITH_VALUE))
              }.updateTyped(tree)
            } else super.transform(tree)
          } else
            super.transform(tree)
        }
      case _ => super.transform(tree)
    }
  }
}
