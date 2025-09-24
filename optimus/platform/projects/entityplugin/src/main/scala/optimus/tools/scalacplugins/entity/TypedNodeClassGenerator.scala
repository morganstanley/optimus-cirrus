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

import scala.tools.nsc.symtab.Flags

trait TypedNodeClassGenerator extends OptimusNames with TreeDuplicator with TypedUtils {
  import global._
  import definitions._
  import CODE._

  val liftedSyms = new scala.collection.mutable.HashSet[Symbol]()

  class NodeClassBuilder(
      curInfo: AsyncScopeInfo,
      parentFunction: Symbol,
      parentDef: Symbol,
      fun: Tree,
      arg: Tree,
      param: Symbol,
      nodeResultType: Type,
      vparams: List[ValDef],
      isByName: Boolean,
      addNodeClsID: Boolean,
      argf: Tree,
      funcTransformer: (Tree, Symbol, Symbol, Symbol) => Tree) {
    private val isStoredClosure = param.hasAnnotation(WithNodeClassIDAnnotation)
    private val isProperty = param.hasAnnotation(PropertyNodeLiftAnnotation)
    private val isCreatedByAsync = parentFunction.hasAnnotation(DefAsyncCreatorAnnotation)
    private val isCreatedByNode = parentFunction.hasAnnotation(DefNodeCreatorAnnotation)
    private val isCreatedByJob = parentFunction.hasAnnotation(DefJobCreatorAnnotation)
    private val isUserAnnotated = isCreatedByAsync || isCreatedByNode
    private val isAlwaysUnique = isUserAnnotated && parentFunction.hasAnnotation(AsyncAlwaysUniqueAnnotation)

    // find the trait that the node should implement (if required). this trait would have been created by AdjustASTComponent
    private val nodeTrait = if (isCreateNodeName(parentFunction.name)) {
      val nodeName = nodeNameFromCreateNodeName(parentFunction.name)
      val entityInfo = parentFunction.owner.info
      val nodeSymbol = entityInfo.decl(nodeName).alternatives.find {
        _.hasAnnotation(NodeSyncAnnotation)
      } // get the @node one... there can be only one @node with a given name, but could be other non-node methods
      nodeSymbol // @elevated tacitly implies @node(exposeArgTypes = true)
        .filter(node => hasExposeArgTypes(node) || node.hasAnnotation(ElevatedAnnotation))
        .map { _ =>
          val nodeTraitSym = entityInfo.decl(mkNodeTraitName(nodeName))
          val targs = parentDef.info.typeParams.map(_.tpeHK)
          appliedType(nodeTraitSym, targs)
        }
    } else None

    lazy val needAsync: Boolean = {
      assert(rhsTransformed)
      curInfo.needAsync
    }

    lazy val isSimple: Boolean = {
      assert(rhsTransformed)
      curInfo.isSimple
    }

    private[this] var rhsTransformed: Boolean = false

    def mkBaseType(cls: Symbol): Type = appliedType(cls, nodeResultType :: Nil)

    lazy val baseType = {
      val baseClass = (isProperty, needAsync) match {
        case (false, false) =>
          if (isAlwaysUnique) NodeSyncAlwaysUnique
          else if (isUserAnnotated) NodeSyncWithExecInfo
          else if (isStoredClosure) NodeSyncStoredClosure
          else NodeSync
        case (false, true) =>
          if (isAlwaysUnique) {
            if (isSimple) NodeDelegateAlwaysUnique else NodeFSMAlwaysUnique
          } else if (isUserAnnotated) {
            if (isSimple) NodeDelegateWithExecInfo else NodeFSMWithExecInfo
          } else if (isStoredClosure) {
            if (isSimple) NodeDelegateStoredClosure else NodeFSMStoredClosure
          } else {
            if (isSimple) NodeDelegate else NodeFSM
          }
        case (true, false) => PropertyNodeSync
        case (true, true)  => if (isSimple) PropertyNodeDelegate else PropertyNodeFSM
      }

      mkBaseType(baseClass)
    }

    var nodeIdxCounter = 0

    def mkNodeClass: ClassDef = {
      // we do the byName transform here, can if the function is tweak operator, and we are inside the @node, we should not allow that
      if (
        fun.symbol.hasAnnotation(TweakOperatorAnnotation) && (parentDef ne NoSymbol) && (parentDef.hasAnnotation(
          DefNodeCreatorAnnotation) || parentDef.hasAnnotation(DefAsyncCreatorAnnotation))
      ) {
        val nodeName = {
          // less ad-hoc than it looks: if we are node-lifting, our parent fn is the node liftee which has not yet gotten
          // a name (we assign it at the very last moment for some reason, after transforming the body) so find the
          // enclosing method instead. This is not needed for the other instances of parentFunction.name below, because
          if (parentFunction.name == nme.EMPTY)
            // this certainly exists and is a TermName, but let's be cautious because I don't want to be the reason
            // that we crash! the actual error emitted is tested in APICheckTests(!) so using a default should be fine.
            parentFunction.ownersIterator.find(m => m.isMethod && m.name.nonEmpty).fold(nme.EMPTY)(_.name.toTermName)
          else parentFunction.name
        }
        alarm(OptimusErrors.TWEAK_BYNAME_IN_NODE, fun.pos, nodeNameFromCreateNodeName(nodeName).decoded)
      }

      val nameStr =
        if (isProperty || (fun.symbol eq NodifyCall)) nodeNameFromCreateNodeName(parentFunction.name)
        else {
          val pos = if (arg != EmptyTree) arg.pos else fun.pos
          // we get NoPosition when we're running inside the ToolBox at runtime
          val line =
            if (pos != NoPosition) s"${pos.line}_${pos.column}"
            else {
              nodeIdxCounter += 1
              "noline" + nodeIdxCounter
            }
          val enclosing_sym =
            if (parentFunction.isMethod && !parentFunction.isSynthetic) "" + parentFunction.name + "_" + fun.symbol.name
            else fun.symbol.name
          "" + enclosing_sym + "_" + line
        }
      val name = newTypeName("$" + nameStr + "$node")
      val nodeClass = parentFunction.newClass(name, fun.pos, Flags.FINAL | Flags.SYNTHETIC)

      val decls = newScope

      def mkNodeInfo(tpe: Type) = {
        val clsId = if (addNodeClsID) Some(NodeClsID.tpe) else None

        val baseTypes = List(Some(tpe), nodeTrait, clsId).flatten
        ClassInfoType(baseTypes, decls, nodeClass)
      }

      // During typing we may generate a nested node class that contains references to the parameters to this node method.
      // Those references will be rewritten from the original param symbols to the member vals of this node class.
      // That node will be typed which will cause those references to be typed.  That typing will fail unless this
      // nodeClass has a reasonable info.  We know that we're a subclass of Node with nodeTrait (if any) so speculatively
      // assign that as our type.
      // TODO (OPTIMUS-0000): investigate NOT rewriting to member vals and instead generate member defs that point to the captured member vals.
      nodeClass setInfo mkNodeInfo(mkBaseType(Node))

      // During addFuncMethod the rhs is transformed (via funcTransformer) which rewrites the callsites to @node methods.
      // After we do this we know whether we need to create a Sync or Async Node class
      val funcDef = addFuncMethod(nodeClass)

      nodeClass setInfo mkNodeInfo(baseType)
      nodeClass.info.decls enter funcDef.symbol

      val propertyNodeMethods = addPropertyNodeMethods(parentFunction.owner, nodeClass, nodeTrait, parentDef, fun.pos)

      val defs2add =
        if (addNodeClsID) funcDef :: mkNodeClsIdGetter(nodeClass) :: propertyNodeMethods
        else funcDef :: propertyNodeMethods

      val paramMods = Modifiers(Flags.PARAMACCESSOR | Flags.PRIVATE)
      val ctorArgs = vparams map { p =>
        p.symbol.flags |= Flags.PARAMACCESSOR
        nodeClass.info.decls.enter(p.symbol)
        treeCopy.ValDef(p, paramMods, p.name, p.tpt, p.rhs)
      }

      assert(nodeClass.info ne null)
      createClassDef(nodeClass, NoMods, List(ctorArgs), List(Nil), defs2add, scala.reflect.internal.util.NoPosition)
    }

    def addPropertyNodeMethods(
        parentClass: Symbol,
        nodeClass: ClassSymbol,
        nodeTrait: Option[Type],
        parentDef: Symbol,
        pos: Position) = {
      var defs2add: List[Tree] = Nil

      val needBasicGetters = isProperty || nodeTrait.isDefined
      // when we cook a nodifyProperty (as we are here), we base the node on the args of the parent function.
      // e.g. for "def foo(a: Double, b: Int) = nodify(a*b)", it's a and b which are the params of the node
      val nodeParams = if (needBasicGetters) parentDef.paramss.flatten else Nil

      // TODO (OPTIMUS-0000): update property info to actually avoid caching for byName arguments
      // TODO (OPTIMUS-0000): fix other functions such as args() and argsCopy()
      val hasByNameArgs = nodeParams.exists(_.isByNameParam)

      val finalOverrideFlags = Flags.FINAL | Flags.OVERRIDE

      if (needBasicGetters || isCreatedByJob) {
        // create a propertyInfo method on this node class which retrieves the corresponding property info from the entity module.
        // e.g. for "@node def foo", create "def propertyInfo = FooCompanion.foo"
        // or for "@async def foo", create "def executionInfo = FooCompanion.foo"
        val propertyInfoDef = addNullarySynthMethod(
          if (isProperty) names.propertyInfo else names.executionInfo,
          nodeClass,
          pos,
          finalOverrideFlags) { _ =>
          val selectProp = mkSelectPropertyInfoFromModule(parentFunction)
          if (hasByNameArgs)
            Select(selectProp, ensureNotCacheable).setType(ensureNotCacheable.tpe.resultType)
          else
            selectProp
        }
        defs2add = propertyInfoDef :: defs2add

      }

      val entityDef = if (needBasicGetters) {
        // create an entity method on this node class which retrieves the parent entity
        val entityDef = addNullarySynthMethod(names.entity, nodeClass, pos, finalOverrideFlags) { _ =>
          gen.mkAttributedThis(parentFunction.owner)
        }
        defs2add = entityDef :: defs2add

        entityDef
      } else null

      def entityCall = gen.mkAttributedIdent(entityDef.symbol)

      // n.b. if node params are empty, we don't need to override these methods, since the base class implementations are fine.
      if (nodeParams.nonEmpty) {
        if (isProperty && !hasByNameArgs) {
          val argsDef = addArgsMethod(nodeClass, pos, nodeParams)
          defs2add = argsDef :: defs2add

          val argsCopyDef = addArgsCopyMethod(parentClass, nodeClass, pos, nodeParams, entityCall)
          defs2add = argsCopyDef :: defs2add
        }

        // create accessor methods for the node params (in order to implement the "trait foo$node").
        // This trait would have been created by AdjustASTComponent if the node was annotated with
        // `exposeArgTypes = true`. Typically that's only used for nodes which have application specific plugins,
        // where it's nice to be able to access node params in a type- and name-safe way.
        // TODO (OPTIMUS-0000): If we have by-name args then this gets screwed up.  We should really just disallow that as it makes very little sense.
        if (!hasByNameArgs && needBasicGetters) {
          val accessors = nodeParams.map { param =>
            // e.g. for a node param "x: String", we generate "def x: String = x"
            val accessorType = NullaryMethodType(param.tpe)
            val accessorSym = nodeClass
              .newMethod(param.name.toTermName, pos, Flags.FINAL)
              .setInfo(accessorType)
            markAsGenerated(accessorSym)
            nodeClass.info.decls enter accessorSym
            val body = gen.mkAttributedRef(param)
            DefDef(accessorSym, body)
          }
          defs2add = accessors ::: defs2add
        }
      }

      if (isProperty) {
        val tweakHandler = tweakHandlerFor(parentFunction)
        if (tweakHandler != NoSymbol) {
          // validate the tweak handler signature
          val resultType = tweakHandler.typeSignature.resultType
          val paramTypes = tweakHandler.typeSignature.paramTypes
          if (!(resultType <:< SeqOfTweaksType))
            alarm(OptimusErrors.WRONG_TWEAK_HANDLER_RETURN_TYPE, tweakHandler.pos, resultType)

          val expectedParamTypes = nodeResultType :: nodeParams.map(_.tpe)
          if (!(paramTypes corresponds expectedParamTypes)(_ weak_<:< _)) {
            alarm(
              OptimusErrors.WRONG_TWEAK_HANDLER_PARAM_TYPE,
              tweakHandler.pos,
              expectedParamTypes.mkString(", "),
              paramTypes.mkString(", "))
          }

          val transformTweakDef =
            addTransformTweakMethod(parentClass, nodeClass, pos, entityCall, tweakHandler, expectedParamTypes)

          defs2add = transformTweakDef :: defs2add
        }
      }
      defs2add
    }

    private def applyNodeMacro(nodeCall: Tree) = {
      // We can get some skolem bugs when we type spliced trees.  We're basically doing a
      // macro expansion here, so we are following the same path described in Typers.adaptMismatchedSkolems()
      nodeCall.updateAttachment(analyzer.MacroExpansionAttachment(EmptyTree, EmptyTree))
    }

    // create the func() method on the node class
    def addFuncMethod(nodeClass: Symbol): DefDef = {
      val (tree, oldOwner) = if (isByName) {
        (arg, parentFunction)
      } else {
        vparams foreach { p =>
          p.symbol.owner = nodeClass
          liftedSyms += p.symbol
        }
        val argf = asFunction(arg)

        (argf.body, argf.symbol)
      }

      val funcMethod = {
        import Flags._
        // create the func() method on the node class
        val ret = nodeClass.newMethod(nme.EMPTY, arg.pos, SYNTHETIC | FINAL | OVERRIDE)
        new ChangeOwnerTraverser(oldOwner, ret) traverse tree
        ret
      }
      val nodeParam = funcMethod.newSyntheticValueParam(appliedType(CompletableNode, nodeResultType :: Nil))
      val ecParam = funcMethod.newSyntheticValueParam(OGSchedulerContext.tpe, newTermName("ec"))

      // After this we know if we need to async (as we've rewritten the callsites)
      val rhs = {
        val transformed = funcTransformer(tree, funcMethod, nodeParam, ecParam)
        rhsTransformed = true

        if (isSimple) {
          // remove wrappers from async(await(node$queued))
          (transformed: @unchecked) match {
            case Block(ClassDef(_, _, _, Template(_, _, body)) :: _ :: Nil, _) =>
              (body.collectFirst {
                case dd @ DefDef(_, _, _, _, _, rhs) if dd.symbol.name == nme.apply =>
                  (rhs: @unchecked) match {
                    case Block(Apply(qual, Apply(_, nodeCall :: Nil) :: Nil) :: Nil, Literal(Constant(()))) =>
                      assert(qual.symbol.name == names.locally, qual)
                      applyNodeMacro(nodeCall.changeOwner(dd.symbol, funcMethod))
                  }
              }) match {
                case Some(tree) => tree
                case _          => throw new MatchError(body)
              }

            case Apply(_, Apply(_, nodeCall :: Nil) :: Nil) =>
              applyNodeMacro(nodeCall)
          }
        } else
          transformed
      }

      val (funcType, funcName) = {
        if (isSimple)
          (NullaryMethodType(appliedType(NodeFuture, nodeResultType :: Nil)), names.childNode)
        else if (needAsync) {
          (MethodType(nodeParam :: ecParam :: Nil, definitions.UnitTpe), names.funcFSM)
        } else
          (NullaryMethodType(nodeResultType), names.funcSync)
      }

      funcMethod.setName(funcName)
      funcMethod.setInfo(funcType)

      DefDef(funcMethod, rhs)
    }

    // generates: def args = { val r = new Array(2); r(0) = arg1.asInstanceOf[AnyRef]; r(1) = arg2.asInstanceOf[AnyRef] ... }
    // Package all arguments for the node into array and return ... see Node.args
    def addArgsMethod(nodeClass: ClassSymbol, pos: Position, nodeParams: List[Symbol]): DefDef = {
      val anyRefArrayType = appliedType(ArrayClass.tpe, List(AnyRefClass.tpe))
      val dd = addNullarySynthMethod(names.args, nodeClass, pos, Flags.FINAL | Flags.OVERRIDE | Flags.SYNTHETIC) {
        argsSym =>
          val createArray = Apply(Select(New(TypeTree(anyRefArrayType)), nme.CONSTRUCTOR), List(LIT(nodeParams.size)))
          val tmpArraySym = argsSym.newValue(names.tmp, pos).setInfo(anyRefArrayType)
          val tmpArrayDef = ValDef(NoMods, names.tmp, TypeTree(anyRefArrayType), createArray).setSymbol(tmpArraySym)
          val listOfArrayAssignment = for ((arg, idx) <- nodeParams.zipWithIndex) yield {
            Apply(
              Select(gen.mkAttributedRef(tmpArraySym), nme.update),
              List(Literal(Constant(idx)), gen.mkAttributedCast(gen.mkAttributedRef(arg), definitions.ObjectTpe))
            )
          }
          val rhs = Block(tmpArrayDef :: listOfArrayAssignment, gen.mkAttributedRef(tmpArraySym))
          new ChangeOwnerTraverser(parentFunction, argsSym) traverse rhs
          rhs
      }
      dd.symbol setInfo NullaryMethodType(anyRefArrayType)
      dd
    }

    // generates: "def argsCopy(gen : AnyRef): Node[T]  = gen.asInstanceOf[(E, A1, A2) => Node[T]].(e, arg1, arg2, ...)"
    def addArgsCopyMethod(
        parentClass: Symbol,
        nodeClass: Symbol,
        pos: Position,
        nodeParams: List[Symbol],
        entityCall: Tree): DefDef = {
      val argsCopySym = nodeClass
        .newMethod(names.argsCopy, pos)
        .setFlag(Flags.OVERRIDE | Flags.FINAL | Flags.SYNTHETIC)
      val argSym = argsCopySym.newValueParameter(names.arg, pos).setInfo(AnyRefClass.tpe)
      val argVal =
        ValDef(NoMods, names.gen, TypeTree(AnyRefClass.tpe), EmptyTree) setType (AnyRefClass.tpe) setSymbol (argSym)
      val nodeType = appliedType(Node.tpe, List(nodeResultType))
      argsCopySym.setInfo(MethodType(List(argSym), nodeType))
      nodeClass.info.decls enter argsCopySym
      // the function class should be FunctionN(EntityClass, Arg1, ..., ArgN, Node[T])
      val typeArgs = parentClass.tpe :: nodeParams.map(_.tpe) ::: nodeType :: Nil
      val functionParams = entityCall :: nodeParams.map(gen.mkAttributedRef)
      val functionClass = FunctionClass(functionParams.length)
      val functionType = appliedType(functionClass.tpe, typeArgs)
      val generator = TypeApply(
        gen.mkAttributedSelect(gen.mkAttributedRef(argSym), Any_asInstanceOf),
        List(TypeTree(functionType))) setType (functionType)
      generator.setType(functionType)
      val applyMethod = functionClass.info.decl(
        nme.apply
      ) // it's necessary to call .apply explicitly because the magic for "Apply means .apply" is done earlier in the compiler
      val body = Apply(gen.mkAttributedSelect(generator, applyMethod), functionParams)
      body.setType(nodeType)
      DefDef(
        Modifiers(Flags.OVERRIDE | Flags.FINAL),
        names.argsCopy,
        Nil,
        List(List(argVal)),
        TypeTree(nodeType),
        body) setSymbol (argsCopySym) setType (nodeType)
    }

    def addTransformTweakMethod(
        parentClass: Symbol,
        nodeClass: Symbol,
        pos: Position,
        entityCall: Tree,
        tweakHandler: Symbol,
        expectedTypes: List[Type]) = {
      // creates "def transformTweak(value: Any): Seq[Tweak] = entity.foo_:=(value, <node args>)"
      val transTweakSym = nodeClass
        .newMethod(names.transformTweak, pos)
        .setFlag(Flags.OVERRIDE | Flags.FINAL | Flags.SYNTHETIC)
      val inputSym = transTweakSym.newValueParameter(names.value, pos).setInfo(AnyClass.tpe)
      val seqOfTweakTpe = appliedType(CollectionSeqClass.tpe, Tweak.tpe :: Nil)
      transTweakSym.setInfo(MethodType(inputSym :: Nil, seqOfTweakTpe))
      nodeClass.info.decls enter transTweakSym
      val input = Ident(inputSym) setType inputSym.tpe

      val argsSym = PropertyNode.info.decl(names.args)
      val applySym = argsSym.info.resultType.decl(names.apply)
      val applyParamType = applySym.asMethod.firstParam.info
      val argInputs = expectedTypes.tail.zipWithIndex.map(t =>
        Apply(
          gen.mkAttributedSelect(gen.mkAttributedSelect(gen.mkAttributedThis(nodeClass), argsSym), applySym),
          List(Literal(Constant(t._2)) setType (applyParamType))) setType (t._1))

      val rhs = Apply(gen.mkAttributedSelect(entityCall, tweakHandler), input :: argInputs)
      rhs setType seqOfTweakTpe

      DefDef(transTweakSym, rhs)
    }
  }

  def asFunction(tree: Tree): Function = {
    def fail = { // the bare minimum so that we don't crash
      val fn = NoSymbol.newAnonymousFunctionValue(tree.pos).setInfo(tree.tpe)
      val paramtps :+ restpe = tree.tpe.typeArgs // it's a function type
      val params = paramtps.map(tp => ValDef(fn.newSyntheticValueParam(tp), EmptyTree))
      Function(params, gen.mkAttributedRef(Predef_???).setType(restpe)).setType(tree.tpe).setSymbol(fn)
    }
    tree match {
      case f: Function             => f
      case Block(Nil, f: Function) => f
      case Block(stats, f: Function) =>
        val how2Fn =
          if (f.tpe.typeArgs.isEmpty) "prepending `() => `"
          else {
            val argSubs = List.fill(f.tpe.typeArgs.length - 1)("_")
            s"appending `${argSubs mkString ("(", ",", ")")}`"
          }
        alarm(
          OptimusErrors.NODELIFT_ETA_EXPANSION,
          tree.pos,
          how2Fn,
          stats.collect { case ValDef(_, _, _, lifted) => "`" + show(lifted) + "`" }.mkString(", "))
        fail
      case _ =>
        alarm(OptimusErrors.UNHANDLED_FUNCTION_PATTERN, tree.pos, show(tree))
        fail
    }
  }

  def mkSelectPropertyInfoFromModule(parentFunction: Symbol): Tree = {
    // grab the PropertyInfo method for this node from the corresponding module. that method is generated by PropertyInfoProvider
    val nodeDefName = nodeNameFromCreateNodeName(parentFunction.name)
    val moduleInfo = infoHolderForSym(parentFunction.owner)
    val propertyInfoOnModuleName = mkPropertyInfoName(moduleInfo, nme.unexpandedName(nodeDefName))
    // find the synthetic value with the right name on the module (we specifically check that it's a val *and* a
    // synthetic in case there are other user defs with the same name)
    val propertyInfoOnModule = moduleInfo.module.info.decls
      .lookupAll(propertyInfoOnModuleName)
      .find(sym => sym.isGetter && sym.isSynthetic)
      .get

    gen.mkAttributedSelect(genQualifiedModuleRef(moduleInfo.module), propertyInfoOnModule).setType(NodeTaskInfo.tpe_*)
  }

  def tweakHandlerFor(parentFunction: Symbol): Symbol = {
    val nodeName = nodeNameFromCreateNodeName(parentFunction.name)
    parentFunction.owner.info.decl(mkTweakHandlerName(nodeName.toTermName))
  }
}
