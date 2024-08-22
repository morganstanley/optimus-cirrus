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

import java.nio.charset.StandardCharsets
import optimus.tools.scalacplugins.entity.reporter.{OptimusErrors, OptimusNonErrorMessages}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.tools.nsc.symtab.Flags
import scala.tools.nsc.transform._

class AsyncScopeInfo(
    val canAsync: Boolean,
    val stopAsync: Boolean, // If @asyncOff is encountered everything in scope(s) below will not be asynced!
    var needAsync: Boolean = false,
    var isSimple: Boolean = false,
    val isLoom: Boolean = false)

object AsyncGraphComponent {
  private[optimus] val eventStoreContextSyncStackInfo =
    "storeContext (which is the default valid time parameter to an @event uniqueInstance method if not provided)"
}

class AsyncGraphComponent(val plugin: EntityPlugin, val phaseInfo: OptimusPhaseInfo)
    extends EntityPluginComponent
    with Transform
    with TransformGrep
    with TypingTransformers
    with AsyncTransformers
    with TypedNodeClassGenerator
    with WithOptimusPhase {
  import global._

  def newTransformer0(unit: CompilationUnit) = new AsyncGraph(unit)

  private lazy val NodeStateMachineClass = rootMirror.getRequiredClass("optimus.graph.NodeStateMachine")

  object ModuleCtorStatsTraverser extends AsyncCallsiteVisitor {
    private[AsyncGraphComponent] def ctorAlarm(tree: Tree, fromSuper: Boolean): Unit = {
      if (fromSuper)
        alarm(OptimusNonErrorMessages.CALL_ON_GRAPH_IN_CTOR_SUPER, tree.pos, tree)
      else
        alarm(OptimusNonErrorMessages.CALL_ON_GRAPH_IN_CTOR, tree.pos, tree)

      if (
        tree.symbol.owner.isNonBottomSubClass(AsyncBaseClass) ||
        tree.symbol.owner.isNonBottomSubClass(AsyncBaseAutoAsyncClass)
      )
        alarm(OptimusNonErrorMessages.PARTICULARLY_PERNICIOUS_USE_OF_ASYNC_LAMBDA_IN_CTOR, tree.pos)
    }
    override def nodeSyncCall(tree: Tree): Unit = ctorAlarm(tree, fromSuper = false)
  }

  object ModuleCtorSuperClassArgsTraverser extends AsyncCallsiteVisitor {
    override def nodeSyncCall(tree: Tree): Unit = ModuleCtorStatsTraverser.ctorAlarm(tree, fromSuper = true)
  }

  // Attachments for passing flag info into codemotion
  sealed trait AggressivePar
  object AggressivePar extends AggressivePar
  sealed trait PluginDebugScope
  object PluginDebugScope extends PluginDebugScope

  class AsyncGraph(unit: CompilationUnit) extends TypingTransformer(unit) {
    import global.definitions._

    override def transformUnit(unit: CompilationUnit): Unit = {
      liftedSyms.clear()
      super.transformUnit(unit)
    }

    var interestingTree: Tree = _
    private var aggressivePar = false
    private var pluginDebug = false
    private var entersGraph = false // and therefore turn-off warning on calling async from sync
    private var inClosureEnteringGraphPosition = false
    private var isScalaTest = false
    private var nodeLiftByName = false // Default to lift by value
    private lazy val serialVersionUIDAnnotation =
      AnnotationInfo(SerialVersionUIDAttr.tpe, List(Literal(Constant(0))), List())

    @inline private final def maybeWithNodeLiftByName[T](tree: Tree)(f: => T): T = {
      val last = nodeLiftByName
      if (hasNodeLiftByName(tree.symbol))
        nodeLiftByName = true
      else if (hasNodeLiftByValue(tree.symbol))
        nodeLiftByName = false
      val r = f
      nodeLiftByName = last
      r
    }

    def asyncCall(tree: Tree, sel: Select, tpe: Type)(f: Tree => Tree): Tree = maybeWithNodeLiftByName(tree) {
      val isNodeCall = isNode(tree.symbol)
      val needAsyncArgs = tree.symbol.hasAnnotation(NodeSyncLiftAnnotation)

      if (!curInfo.canAsync || !isNodeCall) {
        if (isNodeCall && !entersGraph && !curInfo.stopAsync) {
          tree match {
            case Select(qual, name)
                if name.containsName(names.uniqueInstance$default) && qual.tpe.typeSymbol.companionClass.hasAnnotation(
                  EventAnnotation) =>
              alarm(
                OptimusNonErrorMessages.CALL_ASYNC_FROM_SYNC,
                tree.pos,
                AsyncGraphComponent.eventStoreContextSyncStackInfo)
            case _ =>
              alarm(OptimusNonErrorMessages.CALL_ASYNC_FROM_SYNC, tree.pos, tree)
          }
        }
        if (needAsyncArgs) {
          val newName = mkWithNodeName(sel.name)
          val newQual = transform(sel.qualifier)
          val newSel = Select(newQual, newName)
          localTyper.typedPos(sel.pos)(f(newSel))
        } else
          localTyper.typedPos(sel.pos)(f(super.transform(sel)))
      } else {
        curInfo.needAsync = true
        val newName = mkGetNodeName(sel.symbol.name)
        val newQual = transform(sel.qualifier)
        val newSym = newQual.tpe.member(newName)
        if (newSym == NoSymbol)
          internalErrorAbort(sel.pos, s"Internal error: $newName does not exist.")
        val newSel = Select(newQual, newSym)
        val transformed = f(newSel)
        val t = localTyper.typedPos(tree.pos) { transformed }
        FSM.mkAttributedAwait(tpe, t)
      }
    }

    def mkAsyncCall(tree: Tree): Tree = {
      val call = treeInfo.dissectApplied(tree)
      val treeInfo.Applied(core: Select, targs, argss) = call
      val fromSym = call.callee.symbol.paramss
      val fromTpe = call.callee.tpe.paramss
      val newArgss = map3(fromSym, fromTpe, argss) { (fs, ft, args) =>
        transformAsyncArgs(tree, fs, ft, args)
      }
      asyncCall(call.callee, core, tree.tpe) { newCore =>
        wrapSelectInApplies(newCore, targs, newArgss)
      }
    }

    private def wrapSelectInApplies(tree: Tree, newTargs: List[Tree], newArgss: List[List[Tree]]): Tree = {
      @tailrec
      def loop(qual: Tree, argss: List[List[Tree]]): Tree = argss match {
        case Nil => qual
        // We're applying to a Nil final arglist.  In this case we're not sure if we
        // have a NullaryMethodType or a MethodType(Nil, _), so let the typer
        // put in the last apply if needed.
        // NB We can get different types than the original method decl in the case
        // of overrides: See OverrideWithNullaryMethod.scala
        case List(Nil) => qual
        case hd :: tl  => loop(Apply(qual, hd), tl)
      }
      loop(gen.mkTypeApply(tree, newTargs), newArgss)
    }

    def nothingToCompute(arg: Tree): Boolean = {
      def isStable(a: Tree) = {
        val sym = a.symbol
        (sym ne null) && sym.isStable && !sym.hasAnnotation(NodeSyncAnnotation) && !sym.isLazy
      }

      // if all elements in the tree are literals or stable (i.e. vals) and not nodes, the computation is really cheap
      // so we do it right away in the $newNode method and stash it in an AC[P]N
      forAll(arg) {
        case _: Literal | _: This                  => true
        case a @ (_: Select | _: Apply | _: Ident) => isStable(a)
        case _                                     => false
      }
    }

    def transformFuncArgumentToNode(fun: Tree, arg: Tree, param: Symbol): Tree = {
      // argument is originally passed by name (as opposed to lambda). The node created is by name however!
      val isByName = param.isByNameParam
      val isProperty = param.hasAnnotation(PropertyNodeLiftAnnotation)
      def ntc = nothingToCompute(arg)
      def forced = Attachment.ForceNodeClass on currentOwner
      // Trying to avoid creating runnable nodes for constant RHSs.
      // For non-property nodes, the main reason that this cannot be done is an explicit @nodeLiftByName.
      val nodeByValue = !isProperty && isByName && !forced && {
        (!nodeLiftByName && !hasNodeLiftByName(param)) || hasNodeLiftByValue(param) || ntc
      }
      def propertyNodeByValue = isProperty && isByName && ntc && !forced && {
        // For property nodes, we need to be careful to create full PropertyNodeSyncs for the case of
        //   @node(tweak = true) def foo(i: Int) = 1
        // since we can tweak foo(2) := 2 so it depends on the arguments.
        val tweakable = currentOwner.hasAnnotation(TweakableAnnotation)
        def nullary = flattensToEmpty(currentOwner.paramss)
        def hasTweakHandler = tweakHandlerFor(currentOwner).exists
        !tweakable || (nullary && !hasTweakHandler)
      }
      if (nodeByValue) {
        val tpe = appliedType(AlreadyCompletedNode, arg.tpe :: Nil)
        localTyper.typedPos(arg.pos) { New(TypeTree(tpe), (transform(arg) :: Nil) :: Nil) }
      } else if (propertyNodeByValue) {
        localTyper.typedPos(arg.pos) {
          gen.mkMethodCall(
            NodeConstructors_newConstantPropertyNode,
            List(param.tpe.typeArgs.head), // PropertyNode is invariant so we need to be precise
            /* v = */ transform(arg) ::
              /* entity = */ gen.mkAttributedThis(currentOwner.owner) ::
              /* propertyInfo = */ mkSelectPropertyInfoFromModule(currentOwner) :: Nil
          )
        }
      } else {
        if (fun.symbol.hasAnnotation(TweakOperatorAnnotation) && ntc)
          alarm(OptimusNonErrorMessages.UNNECESSARY_TWEAK_BYNAME, fun.pos)
        atPropClass(canAsync = true) { transformFuncArgumentToNodeByName(fun, arg, param) }
      }
    }

    def mkAbstractFunctionWithINodeClsAndSubClass(
        pos: Position,
        vparams: List[ValDef],
        nodeClassDef: ClassDef
    ): Tree = {
      val vpss = vparams :: Nil
      // Create logic equivalent to Function(mkNodeCtorArgs(vpss), mkNewNode(TypeTree(nodeClass.tpe), vpss))
      val fun = localTyper
        .typedPos(pos) { Function(mkNodeCtorArgs(vpss), mkNewNode(TypeTree(nodeClassDef.symbol.tpe), vpss)) }
        .asInstanceOf[Function]

      val parents = addSerializable(abstractFunctionType(fun.tpe.typeArgs.init, fun.tpe.typeArgs.last), NodeClsID.tpe)

      val anonClass = currentOwner newAnonymousFunctionClass (pos, 0L) addAnnotation serialVersionUIDAnnotation
      anonClass setInfo ClassInfoType(parents, newScope, anonClass)

      val targs = fun.tpe.typeArgs
      val (formals, restpe) = (targs.init, targs.last)

      val applyMethodDef = {
        val methSym = anonClass.newMethod(nme.apply, pos, Flags.FINAL)
        val paramSyms = map2(formals, fun.vparams) { (tp, param) =>
          methSym.newSyntheticValueParam(tp, param.name)
        }
        methSym setInfoAndEnter MethodType(paramSyms, restpe)

        fun.vparams foreach (_.symbol.owner = methSym)
        fun.body.changeOwner(fun.symbol, methSym)

        val body = localTyper.typedPos(fun.pos)(fun.body)
        val methDef = DefDef(methSym, List(fun.vparams), body)

        // Have to repack the type to avoid mismatches when existentials
        // appear in the result - see SI-4869.
        methDef.tpt setType localTyper.packedType(body, methSym)
        methDef
      }

      val add2cls = List(nodeClassDef, applyMethodDef, mkNodeClsIdGetter(anonClass))

      nodeClassDef.changeOwner(nodeClassDef.symbol.owner, anonClass)

      localTyper.typedPos(fun.pos) {
        Block(
          List(ClassDef(anonClass, NoMods, List(Nil), add2cls, fun.pos)),
          Typed(New(anonClass.tpe), TypeTree(fun.tpe)))
      }
    }

    def transformFuncArgumentToNodeByName(fun: Tree, arg: Tree, param: Symbol): Tree = {
      // TODO (OPTIMUS-0000): there are too many branches based on isByName in this method. consider refactoring

      val isByName =
        param.isByNameParam // argument is originally passed by name (as opposed to lamda). The node created is always by name regardless

      val argf = if (!isByName) asFunction(arg) else null
      val (tpe, vparams) =
        if (isByName)
          (param.tpe.typeArgs.head, Nil)
        else {
          val targs = arg.tpe.typeArgs
          (targs.last, argf.vparams)
        }

      class CanAsyncTraverser extends Traverser {
        def nodeGuard(c: String)(t: => Unit): Unit = {
          val old = failOnNode
          failOnNode = s"cannot async transform method with asynchronous call under $c, generating synchronous code"
          t
          failOnNode = old
        }

        var failOnNode: String = _
        var cause: String = _
        var position: Position = NoPosition
        var nodeCalls: Int = _

        override def traverse(tree: Tree): Unit = tree match {
          case _ if cause ne null                   =>
          case _: Function | _: ImplDef | _: DefDef =>
          case _: Return =>
            cause = "cannot async transform method with return, generating synchronous code"
            position = tree.pos
          case _: Select | _: Apply if isNodeOrSyncLift(tree.symbol) && (failOnNode ne null) =>
            cause = failOnNode
            position = tree.pos
          case Apply(fn, args) =>
            traverse(fn)
            foreachParamsAndArgs(fn.tpe.params, args) { (param, arg) =>
              // don't look into nodelifted args
              if (!hasNodeLiftAnno(param))
                traverse(arg)
            }
          case _: Select if isNodeOrSyncLift(tree.symbol) =>
            nodeCalls += 1
            super.traverse(tree)
          case _: Try =>
            nodeGuard("try/catch") {
              super.traverse(tree)
            }
          case CaseDef(pat, guard, body) =>
            traverse(pat)
            nodeGuard("pattern guard") { traverse(guard) }
            traverse(body)
          case _ => super.traverse(tree)
        }
      }

      def transformNodeBody(body: Tree, funcMethod: Symbol, nodeParam: Symbol, ecParam: Symbol) = atOwner(funcMethod) {
        val tr = new CanAsyncTraverser
        if (curInfo.canAsync)
          tr.traverse(body)

        val r = if (tr.cause ne null) {
          alarm(OptimusNonErrorMessages.CANNOT_ASYNC, tr.position, tr.cause)
          atPropClass(canAsync = false) {
            transform(body)
          }
        } else
          transform(body)
        if (curInfo.needAsync) {
          def isSimple = (tr.nodeCalls == 1) && {
            body match {
              case _: Select | _: Apply if isNodeOrSyncLift(body.symbol) =>
                true
              case _ => false
            }
          }

          if (isSimple)
            curInfo.isSimple = true

          val applyMethod: DefDef =
            q"""override def apply(tr$$async: $Node[${definitions.AnyRefTpe}]): ${definitions.UnitTpe} = {$r}"""
          val nodeDebugPath: Option[(String, Symbol)] = currentOwner.ownersIterator.collectFirst {
            case x if x.hasAnnotation(NodeDebugAnnotation) =>
              val path = x.getAnnotation(NodeDebugAnnotation).get.args.head.asInstanceOf[Literal].value.stringValue
              (path, x)
          }
          val config = collection.immutable.Map.newBuilder[String, AnyRef]
          config += ("allowExceptionsToPropagate" -> "true")
          val codeMotion = new SimpleCodeMotion(currentOwner.pos)
          nodeDebugPath match {
            case Some((path, _)) =>
              import java.nio.file._
              val dir = Paths.get(path)
              Files.createDirectories(dir)
              val currentPos = currentOwner.pos
              val currentId = currentOwner.id
              def name(prefix: String, extension: String) = s"$prefix-line${currentPos.line}-$currentId.$extension"
              def write(name: String, s: String): Unit =
                Files.write(dir.resolve(name), s.getBytes(StandardCharsets.UTF_8))
              def writeDot(tree: Tree, dot: String): Unit = {
                write(name("state-machine", "dot"), dot)
                write(name("input", "scala"), show(tree))
              }
              def writePostAnf(block: Block): block.type = {
                write(name("post-anf", "scala"), show(block))
                block
              }
              config += ("stateDiagram" -> ((_: Symbol, tree: Tree) => Some((dot: String) => writeDot(tree, dot))))
              config += ("postAnfTransform" -> ((block: Block) => writePostAnf(codeMotion(block))))
            case None =>
              config += ("postAnfTransform" -> codeMotion)
          }

          val applyMethodMarked =
            async.markForAsyncTransform(currentOwner, applyMethod, FSM_await, config.result())
          // IntelliJ's debugger has special support for async-generated classes based on the class name:
          // https://github.com/JetBrains/intellij-scala/blob/e30b66a277553ea90b6b0d86b6ce22739248fe64/scala/scala-impl/src/org/jetbrains/plugins/scala/debugger/ScalaSyntheticProvider.scala#L112
          // I'm not sure how much this adds to IntelliJ's support for Node, but let's leave it as tpnme.stateMachine
          // The async phase itself doesn't care what the class called.
          val typedBlock = localTyper.typedPos(r.pos)(q"""
           final class ${tpnme.stateMachine} extends $NodeStateMachineClass(
            $nodeParam.asInstanceOf[$CompletableNode[${definitions.AnyRefTpe}]],
            $ecParam) {
             $applyMethodMarked
           }
           new ${tpnme.stateMachine}()
           ()
         """)

          typedBlock
        } else
          r
      }

      val addNodeClsID = param.hasAnnotation(WithNodeClassIDAnnotation)

      // create the node class
      val clsDef = new NodeClassBuilder(
        curInfo,
        currentOwner,
        enclSourceMethod,
        fun,
        arg,
        param,
        tpe,
        vparams,
        isByName = isByName,
        // note that for non-byName (i.e. lambdas which return nodes), we implement the NodeClsID logic on the lambda
        // (see mkAbstractFunctionWithINodeClsAndSubClass) not the node, because it's the lambda we will want to check
        // for equality at runtime e.g. in a lambda-based TweakNode or NodeFunctionX, and we don't care whether the
        // produced nodes are equal or not because we run them and then throw them away)
        addNodeClsID = addNodeClsID && isByName,
        argf,
        transformNodeBody
      ).mkNodeClass
      val clsDefTyped = {
        interestingTree = clsDef
        val r = localTyper.typedPos(fun.pos)(clsDef).asInstanceOf[ClassDef]
        interestingTree = null
        r
      }

      val nodeClass = clsDefTyped.symbol
      val blockDef = if (isByName) {
        val obj2ret = localTyper.typedPos(fun.pos) { New(TypeTree(nodeClass.tpe), Nil :: Nil) }
        Block(clsDefTyped :: Nil, obj2ret)
      } else if (addNodeClsID) {
        mkAbstractFunctionWithINodeClsAndSubClass(fun.pos, vparams, clsDefTyped)
      } else {
        Block(
          clsDefTyped :: Nil,
          Function(mkNodeCtorArgs(vparams :: Nil), mkNewNode(TypeTree(clsDefTyped.symbol.tpe), vparams :: Nil)))
      }

      try { localTyper.typedPos(fun.pos) { blockDef } }
      catch {
        case ex: Throwable =>
          alarm(OptimusErrors.LOCAL_TYPER_ERROR, fun.pos, nodeClass.info, blockDef.toString, ex.toString); throw ex
      }
      blockDef
    }

    def transformAsyncArgs(
        fun: Tree,
        paramsFromFunSym: List[Symbol],
        paramsFromFunType: List[Symbol],
        args: List[Tree]): List[Tree] = {
      val closuresEnterGraphFn = isClosuresEnterGraph(fun.symbol)
      def transformAsyncArg(arg: Tree, params: (Symbol, Symbol)) = {
        val (paramFromSym, paramFromType) = params
        // Expected async parameter, which must be lifted.
        val closuresEnterGraph = closuresEnterGraphFn || isClosuresEnterGraph(paramFromSym)
        if (hasNodeLiftAnno(paramFromType)) {
          transformFuncArgumentToNode(fun, arg, paramFromType)
        }
        // A by-name parameter.  The argument is presumed to be evaluated inside the function, so any async
        // calls will be errors iff this function is not marked @closuresEnterGraph - irrespective of whether
        // it's being called as an argument to a function that was so-marked.
        else if (paramFromType.isByNameParam) atPropClass(canAsync = false) {
          val last = entersGraph
          entersGraph = closuresEnterGraph || (isScalaTest && entersGraph)
          val ret = transform(arg)
          entersGraph = last
          ret
        }
        // If we're passing a closure to a method that is expecting one, but is not cEG, then any async calls
        // in the argument are cause for alarm.  We check the param from the method symbol, rather than from
        // the method type, because we care about how it was declared.  E.g. Some[Int => Int](x) should not
        // hit this clause.
        else if (!closuresEnterGraph && isClosureParamAndArg(paramFromSym, arg)) {
          val last = inClosureEnteringGraphPosition
          inClosureEnteringGraphPosition = false
          val ret = transform(arg)
          inClosureEnteringGraphPosition = last
          ret
        }
        // If closuresEnterGraph is specified, then async calls are permitted within the arg unless permission
        // is subsequently withdrawn by one of the clauses above.
        else if (closuresEnterGraph) {
          val last = inClosureEnteringGraphPosition
          inClosureEnteringGraphPosition = true
          val ret = transform(arg)
          inClosureEnteringGraphPosition = last
          ret
        }
        // Otherwise, preserve whatever permissibility is currently in place.
        else
          transform(arg)
      }

      def adjust(p: List[Symbol]) = adjustForRepeatedParameters(p, args)
      map2Conserve(args, adjust(paramsFromFunSym).zip(adjust(paramsFromFunType)))(transformAsyncArg)
    }

    object SelectListIdent {
      def unapply(tree: Tree): Option[(List[Select], Ident)] = tree match {
        case sel @ Select(SelectListIdent(rest, ident), _) => Some(sel :: rest, ident)
        case sel @ Select(ident: Ident, _)                 => Some(sel :: Nil, ident)
        case _                                             => None
      }
    }

    def mkFullyQualifiedIdent(id: Ident): Select = {
      val clsThis = gen.mkAttributedThis(id.symbol.owner)
      assert(clsThis.tpe ne null)
      Select(clsThis, id.symbol).setPos(id.pos).setType(SingleType(clsThis.tpe, id.symbol))
    }

    def transformSafe(tree: Tree): Tree = tree match {
      case cd @ ClassDef(_, _, _, Template(parents, _, _)) if parents.nonEmpty =>
        atAsyncOffScope(tree) {
          // ScalaTest suites can enter the graph all over the place and we allow it (just like we allow junit @Test),
          // including in any nested constructs
          val lastIsScalaTest = isScalaTest
          val lastEntersGraph = entersGraph
          isScalaTest |= ScalaTestSuiteClass.exists && cd.symbol.isSubClass(ScalaTestSuiteClass)
          entersGraph = isScalaTest
          val r = super.transform(tree)
          entersGraph = lastEntersGraph
          isScalaTest = lastIsScalaTest
          r
        }

      // Rewrite case class applies on valdefs moved to node class to make their names and types fully-qualified
      // See AsyncTests.callToNestedCaseClassApplyInCombinatorTest for more details
      case Select(n @ New(tt: TypeTree), nme.CONSTRUCTOR) =>
        val result = tt.tpe.map {
          case TypeRef(_, sym, args) if liftedSyms(sym) => TypeRef(sym.owner.thisType, sym, args)
          case SingleType(_, sym) if liftedSyms(sym)    => SingleType(sym.owner.thisType, sym)
          case tp                                       => tp
        }
        if (result eq tt.tpe) tree
        else {
          val _new = treeCopy.New(n, TypeTree(result)).setType(result)
          treeCopy.Select(tree, _new, nme.CONSTRUCTOR).setType(MethodType(tree.tpe.params, result))
        }
      case id @ Ident(_) if liftedSyms(id.symbol) =>
        mkFullyQualifiedIdent(id)
      // we have made transform for default parameter, so skip transformation here
      case vd: ValDef if vd.mods.hasFlag(Flags.DEFAULTPARAM) =>
        vd

      // scope is wrapped in apar { ... }
      case Apply(fun, arg :: Nil) if fun.symbol == PluginDebugScopeSym =>
        arg.updateAttachment(PluginDebugScope)
        val wasDebug = pluginDebug
        pluginDebug = true
        val xt = super.transform(tree)
        pluginDebug = wasDebug
        xt
      // scope is wrapped in withAdvancedCodeMotion { ... }
      case Apply(fun, arg :: Nil) if fun.symbol == AggressiveParScopeSym =>
        // Ensure that the scope is a proper block, so we can detect it in codemotion
        val block =
          if (arg.isInstanceOf[Block]) arg
          else {
            val tmpName = unit.freshTermName("inBlock")
            val b = q"{ val $tmpName = $arg; $tmpName }"
            localTyper.typedPos(arg.pos)(b)
          }
        val wasAggressive = aggressivePar
        aggressivePar = true
        val xt = transform(block)
        aggressivePar = wasAggressive
        xt

      case _: DefDef | ModuleDef(_, _, _) | Function(_, _) | Template(_, _, _) =>
        val prevSourceMethod = enclSourceMethod
        if (tree.isInstanceOf[DefDef]) enclSourceMethod = tree.symbol
        try
          atAsyncOffScope(tree) {
            tree match {
              case md: ModuleDef if !(tree.symbol.moduleClass isSubClass DelayedInitClass) =>
                // member vals of objects must not call onto graph, even SI calls.
                // They are singletons per classloader and may be used across different runtime envs, dsi impls, etc.
                // Object vals in e.g. OptimusApps are "ok" as they're really local vals of the delayed closure
                ModuleCtorStatsTraverser.traverseTrees(md.impl.body)
                // we also apply the same thing to the <init> function
                md.impl.body.foreach {
                  // most def defs are (correctly) ignored by the AsyncCallSiteVisitor... but we do want to check for
                  // calls onto graph from <init> which is just as bad as calls onto graph from member vals because
                  // those calls might extract data from the graph and put it in vals of a super class!
                  case DefDef(_, nme.CONSTRUCTOR, _, _, _, constructorBody) =>
                    ModuleCtorSuperClassArgsTraverser.traverse(constructorBody)
                  case _ =>
                }
              case _ =>
            }
            val lastEntersGraph = entersGraph
            // @Test, @xFunc and @handle imply @enterGraph
            // @nodeSync does as well, as the caller will handle the rewrite, or be warned
            val symbol = tree.symbol
            def isExcel = hasXFuncAnnotation && symbol.hasAnnotation(xFuncAnnotation)
            def isJunit = hasJunitAnnotations && (
              symbol.hasAnnotation(JunitTestAnnotation) ||
                symbol.hasAnnotation(JunitBeforeAnnotation) ||
                symbol.hasAnnotation(JunitBeforeClassAnnotation) ||
                symbol.hasAnnotation(JunitAfterAnnotation) ||
                symbol.hasAnnotation(JunitAfterClassAnnotation)
            )
            def isAppEntry = PartialFunction.cond(tree) { case mimpl: Template =>
              mimpl.symbol.owner isSubClass DelayedInitClass
            }
            entersGraph = isScalaTest ||
              inClosureEnteringGraphPosition ||
              symbol.hasAnnotation(EntersGraphAnnotation) ||
              symbol.hasAnnotation(handleAnnotation) ||
              symbol.hasAnnotation(NodeSyncAnnotation) ||
              isExcel ||
              isJunit ||
              isAppEntry
            val r = super.transform(tree)
            entersGraph = lastEntersGraph
            r
          }
        finally enclSourceMethod = prevSourceMethod
      case _: Apply | _: TypeApply | _: Select if curInfo.isLoom =>
        tree
      case _: Apply | _: TypeApply | _: Select if !isSyncValAccess(tree) && isNodeOrSyncLift(tree.symbol) =>
        mkAsyncCall(tree)
      case Apply(fun, _) if curInfo.canAsync && (fun.symbol == FSM_await) =>
        curInfo.needAsync = true
        super.transform(tree)
      case Apply(fun, args) if !fun.tpe.isError =>
        maybeWithNodeLiftByName(tree) {
          val paramsFromTpe = fun.tpe.params
          // In the case where there was only one param group for the original method symbol, we can be sure
          // that it corresponds to the args and the params from tpe.  Pass them to transformAsyncArgs, so it
          // can use this information to generate more precise errors on illegal async closures.
          val paramsFromSym = if (fun.symbol.paramss.size == 1) fun.symbol.paramss.head else paramsFromTpe
          treeCopy.Apply(tree, transform(fun), transformAsyncArgs(fun, paramsFromSym, paramsFromTpe, args))
        }
      case block @ Block(stats, _) =>
        if (aggressivePar) {
          block.updateAttachment(AggressivePar)
          // ANF currently strips attachments from blocks. the solution is probably to attach
          // to the child stats themselves.
          for (stat <- stats)
            stat.updateAttachment(AggressivePar)
        }
        if (pluginDebug) {
          block.updateAttachment(PluginDebugScope)
          for (stat <- stats)
            stat.updateAttachment(PluginDebugScope)
        }

        super.transform(tree)
      case _ => super.transform(tree)
    }

    /**
     * Avoid reporting sync stacks on access of an embeddable val that contains an @entity field (even if not @stored)
     * from a (sync) constructor of an @stored @entity (see OPTIMUS-59347)
     *
     * If NoSyncStack is being constructed then we must have val embeddable: Embeddable in memory and therefore the
     * nonStored: NonStored is in memory too (because embeddable -> entity refs are not lazy [1]). So on construction
     * there’s no sync stack, just two plain old in-memory dereferences.
     *
     * Entity vals (including non-constructor vals) are stored in the DAL, so when val nonStored: NonStored is pickled
     * we’ll snapshot the eref of the NonStored. Then on load of NoSyncStack, embeddable.nonStored is not being
     * re-executed, we’re just making a LazyPickledReference wrapping the nonStored eref.
     *
     * So while accessing val nonStored: NonStored from a sync context is a 17001, nonStored itself isn’t and should not
     * be flagged.
     *
     * The plugin should avoid flagging access to an entity’s own fields from other fields on the same entity instance
     * as 17001, since it’s always going to be a plain old dereference, not an LPR resolve.
     *
     * [1] See long term plan to make stored entity references from embeddables lazy (OPTIMUS-55129) and related
     * temporary workaround (OPTIMUS-55600)
     *
     * See optimus.scalac.tests.StoredEntityConstructorSyncStackTests
     */
    private def isSyncValAccess(tree: Tree): Boolean = tree match {
      case Select(th: This, _) =>
        // this is really just a check that we are not in a def at all, we can't truly check we're in constructor yet
        // because the val initialisation hasn't yet moved into the <init> method
        val inConstructor = enclSourceMethod == NoSymbol
        val selectSymbol = tree.symbol
        val owner = selectSymbol.owner
        // need it to be This for the same symbol that owns us (not some enclosing this)
        val ownerThis = th.symbol == owner
        // need the Select's symbol to be a non-tweakable val (owned by our owner), or if this has already been
        // transformed to a def, check for valAccessor annotation
        inConstructor && ownerThis && (!isTweakable(tree) && (selectSymbol.isVal || isValAccessor(tree.symbol)))
      case _ => false
    }

    /**
     * The currently-enclosing method, according to the incoming unit. It's non-trivial to grab the method we want by
     * using currentOwner/localTyper.context, because we're making new methods and so on as we go along, so it's easier
     * just to mark them as we enter them.
     */
    private[this] var enclSourceMethod: Symbol = NoSymbol

    override def transform(tree: Tree): Tree = {
      try transformSafe(tree)
      catch {
        case ex: Throwable =>
          ex.printStackTrace() // should we remove such kind of stack trace? [Unattributed comment]
          // Assume that if it's OK to print a stack trace rather than use reporter, then it's ok to print other things.
          // These have been very useful for debugging compiler crashes:
          if (curTree ne null) {
            System.err.println(s"Internal current ${curTree.pos}: $curTree")
            System.err.println(showRaw(curTree))
          }
          System.err.println(s"Our current ${tree.pos} $tree")
          System.err.println(showRaw(tree))
          if (interestingTree ne null) {
            System.err.println("Possibly interesting: " + interestingTree)
            System.err.println(showRaw(interestingTree))
          }
          alarm(
            OptimusErrors.ASYNC_TRANSFORM_ERROR,
            if (curTree ne null) curTree.pos else tree.pos,
            unit.source.file.path,
            ex.toString + ":" + ex.getStackTrace.take(10).mkString(",")
          )
          EmptyTree
      }
    }
  }

  class SimpleCodeMotion(applyPos: Position) extends Transformer with (Block => Block) {
    def apply(block: Block): Block = {
      // if (applyPos.source.file.name.contains("EventSerializer.scala"))
      //  getClass
      transform(block).asInstanceOf[Block]
    }

    private var advancedCodeMotionBlock = false
    private var debugScope = false

    import optimus.tools.scalacplugins.entity.reporter._
    import scala.collection.mutable.ArrayBuffer
    import global._
    lazy val miscFlagsClass: Symbol = rootMirror.getClassIfDefined("optimus.platform.annotations.miscFlags")
    lazy val impureClass: Symbol = rootMirror.getClassIfDefined("optimus.platform.impure")
    lazy val sequentialClass: Symbol = rootMirror.getClassIfDefined("optimus.platform.annotations.sequential")

    private case class AwaitInfo(block: ArrayBuffer[Tree], deferred: ArrayBuffer[Tree], var aliases: List[Symbol])

    private val sym2awaiter = mutable.LinkedHashMap.empty[Symbol, AwaitInfo]
    private var curBlock: ArrayBuffer[Tree] = _

    private def getAndClearAccumulatedBlock(): List[Tree] = {
      if (curBlock.isEmpty) Nil
      else {
        val list = curBlock.toList
        curBlock.clear()
        list
      }
    }

    private def considerNonRT(tree: Tree) =
      tree.symbol.annotations.exists { a: Annotation =>
        ((a.tree.tpe.typeSymbol == miscFlagsClass) &&
          a.tree.children.tail.headOption.exists {
            _ match {
              case Literal(Constant(i: Int)) if (i & MiscFlags.NOT_RT) != 0 => true
              case _                                                        => false
            }
          }) ||
        a.tree.tpe.typeSymbol == impureClass || a.tree.tpe.typeSymbol == sequentialClass
      }

    object SplitWait {
      trait Awaiter
      object Awaiter extends Awaiter
      trait NewNode
      object NewNode extends NewNode
      def unapply(tree: Tree): Option[List[ValDef]] = tree match {
        case vd @ ValDef(mods, name, tpt, ap @ Apply(fun, awaitable :: Nil))
            if fun.hasSymbolField & fun.symbol == FSM_await &&
              name.toString.startsWith("await$") && !considerNonRT(awaitable) =>
          val suffix = name.toString.substring("await$".length)
          val nodeExpr = atOwner(vd.symbol)(transform(awaitable))

          // Crete new val to enqueue rhs, e.g.
          // val node$123 = Foo.bar$queued
          val initialNodeSym = vd.symbol.owner.newTermSymbol(TermName("node$" + suffix), tree.pos)
          val nodeSym = initialNodeSym.setInfo(nodeExpr.tpe)
          val vdNode = ValDef(nodeSym, nodeExpr.changeOwner(vd.symbol, nodeSym)).setType(NoType)
          vdNode.updateAttachment(NewNode)
          // Create new await that just reads our temporary variable
          // val await$1 = FSM.await(node$1)
          val vdNewAwait =
            treeCopy.ValDef(vd, mods, name, tpt, treeCopy.Apply(ap, fun, gen.mkAttributedIdent(nodeSym) :: Nil))
          vdNewAwait.updateAttachment(Awaiter)
          Some(vdNode :: vdNewAwait :: Nil)
        case _ => None
      }
    }

    private def getLaunchersAndDeferAwaiters(stats: List[Tree]): (List[Tree], List[Tree]) = {
      val iter = stats.iterator

      // shared mutable collection used in all AwaitInfos created for the current stats - intended?
      val deferred = new ArrayBuffer[Tree]
      val statsAfterLastWait = ListBuffer.empty[Tree]

      def transformStat(tree: Tree): List[Tree] = {
        val launchers: List[Tree] = tree match {
          case SplitWait(vdNode :: vdNewAwait :: Nil) =>
            // Defer the awaiting
            deferred += vdNewAwait
            // Remember what we're waiting for.
            sym2awaiter(vdNewAwait.symbol) = AwaitInfo(curBlock, deferred, vdNewAwait.symbol :: Nil)
            // Place the new enqueuer into the stats
            curBlock ++= statsAfterLastWait
            curBlock += vdNode
            statsAfterLastWait.clear()
            Nil
          // scala/async creates a lot of aliases like val x$1 = await$1.  These need to
          // be deferred as well, so that await is actually available.
          case ValDef(_, _, _, Unadapt(id @ Ident(_))) if sym2awaiter.contains(id.symbol) =>
            val info = sym2awaiter(id.symbol)
            if (info.block eq curBlock) {
              info.deferred += tree
              info.aliases ::= tree.symbol
              sym2awaiter(tree.symbol) = info
              Nil
            } else atOwner(tree.symbol)(transform(tree)) :: Nil
          case _ =>
            transform(tree) :: Nil
        }
        statsAfterLastWait ++= launchers
        getAndClearAccumulatedBlock()
      }

      (iter.flatMap(transformStat).toList, statsAfterLastWait.toList)
    }

    object Unadapt {
      @tailrec
      def unapply(t: Tree): Option[Tree] = t match {
        case Apply(fun, tree :: Nil) if fun.symbol.name == nme.unbox && currentRun.runDefinitions.isUnbox(fun.symbol) =>
          unapply(tree)
        case treeInfo.StripCast(t1) =>
          if (t1 eq t) Some(t) else unapply(t1)
      }
    }

    override def transform(tree: Tree): Tree = {
      def flushDeferredAwaitersToBlock(awaitInfo: AwaitInfo): Unit = {
        awaitInfo.aliases.foreach(sym2awaiter -= _)
        awaitInfo.block ++= awaitInfo.deferred
        awaitInfo.deferred.clear()
        if (debugScope)
          alarm(OptimusNonErrorMessages.CODE_MOTION, tree.pos, s"$awaitInfo")
      }

      // If curBlockOnly is false, flush all awaits accumulated so far, in all blocks up to the current one,
      // to avoid violating code invariants.
      // If curBlockOnly is true, we only flush the awaits in the current block
      def flushAllDeferredAwaitersToBlock(curBlockOnly: Boolean): Unit = {
        val s2a = sym2awaiter.toList // copy
        s2a.foreach { case (sym, awaiterInfo) =>
          // Items may be removed from sym2awaiter by flushDeferredAwaitersToBlock below, so check if it's still there
          if (sym2awaiter.contains(sym)) {
            val isCurBlock = awaiterInfo.block eq curBlock
            if (curBlockOnly && isCurBlock)
              awaiterInfo.deferred foreach { t =>
                // If we've lost the position somehow, make a decent guess, and throw in some extra info
                val (pos, extra) =
                  if (t.pos == NoPosition)
                    (applyPos, s"(${t.symbol}: ${t.symbol.typeSignature})")
                  else (t.pos, "")
                alarm(OptimusNonErrorMessages.DEAD_NODE_CALL, pos, extra)
              }
            if (!curBlockOnly || isCurBlock)
              flushDeferredAwaitersToBlock(awaiterInfo)
          }
        }
      }

      def isLocal(sym: Symbol): Boolean = {
        // symbol.isLocal method is deprecated, but the new APIs doesn't fit our requirement
        // so we use the internal implementation here directly to avoid the deprecated warning
        sym.owner.isTerm
      }

      def sort(stats: Seq[Tree]): Seq[Tree] = {

        var pairs = 0
        val statsWithAwaitPairs: Seq[(Tree, Int)] = stats
          .flatMap {
            case SplitWait(pair) =>
              pairs += 1
              pair
            case s => s :: Nil
          }
          .zipWithIndex
          .toIndexedSeq

        if (pairs <= 1)
          stats
        else {

          val sym2i: Map[Symbol, Int] = statsWithAwaitPairs.collect {
            case (s, i) if s.hasSymbolField => (s.symbol, i)
          }.toMap

          val ordered = mutable.ArrayBuffer.empty[Tree]
          val barriers = mutable.HashSet.empty[Int]
          val assigners = mutable.HashMap.empty[Int, Set[Int]]

          class StatTraverser(stat: Tree, i: Int, ideps: mutable.HashSet[Int]) extends Traverser {
            var isBarrier = false
            // Every stat depends on all barriers preceding it
            override def traverse(tree: Tree): Unit = tree match {
              case Throw(expr) =>
                isBarrier = true
                traverse(expr)
              case Assign(lhs, rhs) =>
                // Record that this stat is a dependency of anyone who depends on the assignee.
                // It's not quite transitive, since we can't make the assignee depend on a stat that follows it!
                if (sym2i.contains(lhs.symbol)) {
                  val assignee = sym2i(lhs.symbol)
                  val newAssigners = assigners.get(assignee).fold(Set(i))(_ + i)
                  assigners += assignee -> newAssigners
                }
                traverse(rhs)
              case elem if elem != stat && elem.hasSymbolField && sym2i.contains(elem.symbol) =>
                val i = sym2i(elem.symbol)
                // This stat depends on any symbol we refer to
                ideps += i
                // and any assigners of that symbol.
                if (assigners.contains(i))
                  ideps ++= assigners(i)
                super.traverse(elem)
              case elem =>
                super.traverse(elem)
            }
            def traverse(): Unit = traverse(stat)
          }

          lazy /* forward ref */ val statsWithDeps = statsWithAwaitPairs.map { case (stat, i) =>
            var isBarrier = false
            val ideps = mutable.HashSet.empty[Int] ++ barriers
            if (stat.isInstanceOf[LabelDef])
              isBarrier = true
            else {
              val traverser = new StatTraverser(stat, i, ideps)
              traverser.traverse()
              isBarrier = traverser.isBarrier
            }
            if (isBarrier) {
              // Every barrier depends on all nodes above it
              barriers += i
              ideps ++= (0 until i)
            }
            val isAwait = stat.hasAttachment[SplitWait.Awaiter]
            val isLaunch = stat.hasAttachment[SplitWait.NewNode]
            Stat(i, ideps.toSeq.sorted, isAwait, isLaunch)
          }.toArray

          case class Stat(
              i: Int,
              ideps: Seq[Int],
              isAwait: Boolean,
              isLaunch: Boolean,
              // added list of ordered stats in a previous traversals
              var emitted: Boolean = false,
              // to be added to list after this traversal
              var emitting: Boolean = false,
              // has un-emitted dependencies
              var hasDeps: Boolean = false,
              // seen during this traversal on a path with launches; i.e. known to have enqueuings depending on it
              var hasLaunch: Boolean = false,
              // seen already on this traversal
              var encountered: Boolean = false) {

            // note "has unemitted dependences" == "has await dependencies" because if they weren't await dependencies
            // the stat would have been emitted already
            def hasAwait: Boolean = !emitted && (isAwait || hasDeps)
            def stat: Tree = statsWithAwaitPairs(i)._1
            def deps: Seq[Stat] = ideps.map(statsWithDeps).filterNot(_.emitted)

            def emit(): Unit = if (!emitted) {
              emitted = true
              emitting = false
              ordered += stat
            }

            def clearForTraversal(): Unit = {
              encountered = false
              hasDeps = false
            }

            def descend(onLaunchPath: Boolean): Stat = {
              if (!emitted && !emitting) {
                // If we previously encountered this node off any launch path but are one one now, we'll retraverse its dependencies
                if (!encountered || (onLaunchPath && !hasLaunch)) {
                  // After traversal, anything without awaits will have been emitted
                  hasLaunch = isLaunch || onLaunchPath
                  deps.foreach { s =>
                    val waiting = s.descend(hasLaunch).hasAwait
                    hasDeps ||= waiting
                  }
                  encountered = true
                }
                // All dependencies satisfied and not an await, so go ahead and emit it.
                if (!isAwait && !hasDeps)
                  emit()
                // If this is a wait, and some node launch depends on it (and was therefore not emitted during this
                // traversal), mark it for emission right after the traversal.
                else if (isAwait && !hasDeps && onLaunchPath)
                  emitting = true
              }
              this
            }
          }

          val roots = {
            val allDeps = statsWithDeps.iterator.flatMap(_.deps.map(_.i)).toSet
            statsWithDeps.filterNot(stat => allDeps.contains(stat.i))
          }

          var prevSize = 0
          while (prevSize < statsWithDeps.length) {
            roots.foreach(_.descend(false))
            // Launch everything that was blocking a launch
            statsWithDeps.foreach { s => if (s.emitting) s.emit() }
            // If we're making no progress, we must have awaits that are not blocking a node launch, so sweep them
            // in now.
            if (ordered.size == prevSize)
              statsWithDeps.foreach { s => if (!s.hasDeps && !s.emitted) s.emit() }
            // If we're still making no progress, then something has gone horribly wrong.
            if (ordered.size == prevSize)
              throw new AssertionError("Unable to make progress on stat sorting!!")
            statsWithDeps.foreach(_.clearForTraversal())
            prevSize = ordered.size
          }

          assert(ordered.size == statsWithDeps.length)

          ordered.toSeq
        }

      }

      def codeMotionLog(advanced: Boolean, range: Position): Unit = {
        val advancedTag = if (advanced) "(advanced)" else "(simple)"
        alarm(OptimusNonErrorMessages.CODE_MOTION, range, s"successful reorder $advancedTag")
      }

      sym2awaiter.get(tree.symbol).foreach(flushDeferredAwaitersToBlock)
      tree match {
        case b @ Block(stats, expr) =>
          debugScope ||= (b.hasAttachment[PluginDebugScope] || b.stats.exists(_.hasAttachment[PluginDebugScope]))

          val wasAggressive = advancedCodeMotionBlock
          // The ANF transform strips our attachment, so we communicate by adding it to all stats in the hope that
          // some clue will still be around when codemotion is invoked.
          advancedCodeMotionBlock ||= (b.hasAttachment[AggressivePar] || b.stats.exists(_.hasAttachment[AggressivePar]))
          val languageOption = plugin.hasLanguageOption(tree.pos.source, LanguageOption.autopar)

          val newBlock =
            if (advancedCodeMotionBlock || languageOption) {
              val sorted = sort(stats)
              val newStats = sorted.map(transform).toList
              val newExpr = transform(expr)
              val ret = treeCopy.Block(tree, newStats, newExpr)
              if (sorted ne stats) {
                codeMotionLog(advanced = true, sorted.head.pos)
              }
              ret
            } else {
              val ob = curBlock
              curBlock = ArrayBuffer.empty[Tree]
              val (launchers, statsAfterLastWait) = getLaunchersAndDeferAwaiters(stats)
              val newExpr = transform(expr)
              flushAllDeferredAwaitersToBlock(curBlockOnly = true)
              val awaiters = getAndClearAccumulatedBlock()
              val sorted = launchers ::: awaiters ::: statsAfterLastWait
              val nb = treeCopy.Block(tree, sorted, newExpr)
              if (awaiters.nonEmpty) {
                codeMotionLog(advanced = false, sorted.head.pos)
              }
              curBlock = ob
              nb
            }
          advancedCodeMotionBlock = wasAggressive
          newBlock
        /*
         * This should be a rare case, but we can't transform inside of ClassDefs or DefDefs, but they can close
         * over existing symbols so flush so the initialization occurs in proper order.  This is inspired by
         * at test in AsyncQueryTest, which will fail if we don't flush and we run code like this:
         *   from(entities).extendTyped(r => { val x = r.asyncMethod; val q = new { val twice = x * 2 }; q })
         * in this case we capture x when we instantiate q, so the assignment needs to occur before the new
         */
        case _: ClassDef | _: DefDef =>
          flushAllDeferredAwaitersToBlock(false)
          tree
        case Apply(f, _) if f.symbol == NoSymbol =>
          // I assume these are newly-generated local LabelDefs
          tree
        case Apply(f, _) if isLocal(f.symbol) =>
          flushAllDeferredAwaitersToBlock(false)
          super.transform(tree)
        case New(f) if isLocal(f.symbol) =>
          flushAllDeferredAwaitersToBlock(false)
          super.transform(tree)
        case _ => super.transform(tree)
      }
    }
  }
}
