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

import optimus.tools.scalacplugins.entity.reporter.OptimusErrors
import optimus.tools.scalacplugins.entity.reporter.OptimusNonErrorMessages

import scala.tools.nsc.transform.Transform
import scala.tools.nsc.transform.TypingTransformers

class LocationTagComponent(val plugin: EntityPlugin, val phaseInfo: OptimusPhaseInfo)
    extends EntityPluginComponent
    with Transform
    with TransformGrep
    with TypingTransformers
    with AsyncTransformers
    with WithOptimusPhase
    with TypedTransformingUtils {
  import global.CompilationUnit

  def newTransformer0(unit: CompilationUnit) = new LocationTag(unit)

  class LocationTag(unit: CompilationUnit) extends TypingTransformer(unit) {
    import global._

    var injectionValDef: Option[Tree] = None

    override def transform(tree: Tree): Tree = {
      try {
        transformSafe(tree)
      } catch {
        case ex: Throwable =>
          ex.printStackTrace()
          alarm(OptimusErrors.ERROR_LOCATION_TAG_GENERATION, tree.pos, ex)
          tree
      }
    }

    private def transformSafe(tree: Tree): Tree = tree match {
      case vd: ValDef if vd.symbol.owner.hasAnnotation(EntityAnnotation) =>
        withInjectionValDef(Some(vd)) { super.transform(tree) }
      case _: ClassDef | _: DefDef =>
        withInjectionValDef(None) { super.transform(tree) }
      case _: Apply | _: TypeApply if tree.symbol.hasAnnotation(WithLocationTagAnnotation) =>
        mkCallWithLocationTag(tree)
      case ap @ Apply(sel: Select, _) if ap.symbol == WithDerivedTag =>
        mkCallWithDerivedTag(ap, sel)
      case ap: Apply if !isSafeApply(ap) =>
        transformUnsafeApply(ap)
      case _ =>
        super.transform(tree)
    }

    /**
     * Transform the tree while keeping track of the val on the left hand side to which a unique field is to be
     * assigned. We set that parameter to None whenever we transform an unsafe tree, i.e., whenever transforming
     * a tree may result in non-unique fields.
     */
    private def withInjectionValDef(currentValDef: Option[Tree])(f: => Tree): Tree = {
      val previousValDef = injectionValDef
      injectionValDef = currentValDef
      val transformed = f
      injectionValDef = previousValDef
      transformed
    }

    /**
     * An Apply tree is guaranteed to be safe if it is called on a subclass of an option. This is because even if
     * some of the arguments are anonymous functions, they will be called at most once.
     */
    private def isSafeApply(ap: Apply): Boolean =
      ap.symbol != null && ap.symbol.owner.tpe.typeSymbol.isSubClass(definitions.OptionClass)

    private def transformUnsafeApply(ap: Apply): Tree = {
      val newArgs = ap.args.map(arg => {
        if (isAnonymousFunction(arg)) withInjectionValDef(None) { super.transform(arg) }
        else transform(arg)
      })
      val newFun = transform(ap.fun)

      treeCopy.Apply(ap, newFun, newArgs)
    }

    private def isAnonymousFunction(tree: Tree): Boolean = tree match {
      case f: Function => f.symbol.isAnonymousFunction
      case b: Block    => (b.expr.symbol ne null) && b.expr.symbol.isAnonymousFunction
      case _           => false
    }

    private def mkCallWithLocationTag(tree: Tree): Tree = {
      val call = treeInfo.dissectApplied(tree)
      val treeInfo.Applied(sel @ Select(qual, _), targs, args0) = call
      val args = args0.mapConserve(transformTrees)

      val isValid = injectionValDef.isDefined

      getNewMethodName(tree, isValid) match {
        case Some(newFuncName) =>
          val newSel = Select(transform(qual), newFuncName).setPos(sel.pos)
          val newArgs = if (isValid) {
            getLocationTag(tree, injectionValDef) :: args.head
          } else args.head

          val newInnerApp = targs match {
            case Seq() => Apply(newSel, newArgs)
            case _     => Apply(TypeApply(newSel, targs), newArgs)
          }

          val newApp = args0 match {
            case _ :: extraArgs :: _ => Apply(newInnerApp, extraArgs)
            case _                   => newInnerApp
          }

          if (!isValid)
            alarm(OptimusNonErrorMessages.LOCATION_TAG_INVALID_USE, tree.pos)

          localTyper.typed(newApp.setPos(tree.pos))
        case _ => EmptyTree
      }
    }

    private def mkCallWithDerivedTag(ap: Apply, sel: Select): Tree = {
      val method = sel.name
      val qual = sel.qualifier

      val newMethod = mkDerivedTagName(method)
      val newSel = Select(transform(qual), newMethod).setPos(sel.pos)
      val newArgs = getLocationTag(ap, None) :: Nil

      localTyper.typed(Apply(newSel, newArgs).setPos(ap.pos))
    }

    private def getNewMethodName(tree: Tree, isValid: Boolean): Option[Symbol] = {
      val (name, owner, params) = getMethodInfo(tree)
      val newParams = if (isValid) LocationTagClass.tpe.typeSymbol :: params else params
      val methodFullName = s"${owner.nameString}.$name"

      val matchingMethodsSeq =
        if (isValid)
          matchingMethods(name, owner, newParams) ++ matchingMethods(mkLocationTagName(name), owner, newParams)
        else matchingMethods(mkNonRTName(name), owner, newParams)

      matchingMethodsSeq match {
        case Seq(meth) => Some(meth)
        case Seq() if !isValid =>
          alarm(OptimusErrors.ERROR_LOCATION_TAG_MUST_BE_RT, tree.pos, methodFullName)
          None
        case _ =>
          alarm(OptimusErrors.ERROR_LOCATION_TAG_MATCHING_METHOD, tree.pos, matchingMethodsSeq.size, methodFullName)
          None
      }
    }

    private def getMethodInfo(tree: Tree): (Name, Symbol, List[Symbol]) =
      (tree.symbol.name, tree.symbol.owner, tree.symbol.info.params)

    private def matchingMethods(funName: Name, owner: Symbol, params: Seq[Symbol]): Seq[Symbol] = {
      val allMethods = owner.info.decls

      allMethods
        .filter(f =>
          f.isMethod &&
            f.name == funName &&
            params.size == f.info.params.size &&
            params.zip(f.info.params).forall(a => a._1.info.typeSymbol.name == a._2.info.typeSymbol.name))
        .toSeq
    }

    private def getLocationTag(tree: Tree, owningVal: Option[Tree]): Tree = {
      val pos = tree.pos

      val line = Literal(Constant(pos.line))
      val column = Literal(Constant(pos.column))
      val file = Literal(Constant(pos.source.path))
      val name = owningVal.map(_.symbol.nameString).getOrElse("")
      val containing = owningVal.map(t => This(t.symbol.owner)).getOrElse(Literal(Constant(null)))

      q"""$LocationTagPkg.apply($line, $column, $name, $file, $containing)"""
    }
  }
}
