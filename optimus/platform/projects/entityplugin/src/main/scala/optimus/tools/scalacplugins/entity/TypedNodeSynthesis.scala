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

import optimus.tools.scalacplugins.entity.Attachment.NodeFieldType

import scala.tools.nsc.Global
import scala.tools.nsc.symtab.Flags._
import CollectionUtils._
import optimus.tools.scalacplugins.entity.staged.scalaVersionRange

import scala.reflect.internal.util.{ListOfNil, TriState}

trait TypedNodeSynthesis { this: GenerateNodeMethodsComponent =>

  val global: Global
  import global._

  private[this] object SimpleTyper {
    def typed(tree: Tree): Unit = if (tree.tpe eq null) tree match {
      case TypeDef(_, _, tparams, rhs) =>
        tparams.foreach(typed)
        typed(rhs)
        tree.setType(NoType)
      case TypeBoundsTree(lo, hi) =>
        typed(lo)
        typed(hi)
        tree.setType(TypeBounds(lo.tpe, hi.tpe))
      case _ =>
      // do nothing
    }
  }

  private[this] val nodeMethodsPhase = perRunCaches.newGeneric {
    currentRun.phaseNamed(OptimusPhases.GENERATE_NODE_METHODS.phaseName)
  }

  // create = $newNode, get = $queued
  // we always have a get/$queued method, but raw deferred nodes don't get/need one
  // deferred property nodes do, because we may want to tweak them!
  private[this] case class NodeMethods(get: Symbol, create: Symbol)

  // a map from node method to the node methods we generated in the info transform.
  // this is somewhat easier than using at attachment, and is largely honest because
  // this mapping doesn't escape this phase
  private[this] val generatedMethods = perRunCaches.newAnyRefMap[Symbol, NodeMethods]()

  private[this] case class NodeTree(tree: ValOrDefDef, nodeInfo: Attachment.Node) {
    val sym: TermSymbol = tree.symbol.asTerm
    val name: TermName = sym.name
    val owner: TypeSymbol = sym.owner.asType
    val restpe: Type = sym.info.finalResultType

    val NodeMethods(queuedNodeSym, createNodeSym) =
      exitingPhase(nodeMethodsPhase()) { sym.owner.info; generatedMethods(sym) }

    val tparams: List[TypeDef] = typeParamsOf(tree)
    val vparamss: List[List[ValDef]] = vParamssOf(tree)
    val rhs: Tree = tree.rhs
  }

  def mkGetNodeSyms(
      nodeSym: Symbol,
      nodeInfo: Attachment.Node
  ): List[Symbol] = {
    val queuedNode = // name$queued
      mkQueuedNodeSym(nodeSym, nodeInfo)

    val createNode =
      if ((nodeSym.isDeferred || nodeSym.isSuperAccessor) && nodeInfo.raw)
        // Original @node method is abstract (or a superaccessor...), so don't create name$newNode
        // Note that we always need one for property nodes (so we can tweak them)... it can be deferred
        NoSymbol
      else mkCreateNodeSym(nodeSym, nodeInfo)

    generatedMethods(nodeSym) = NodeMethods(queuedNode, createNode)

    nodeSym.resetFlag(ACCESSOR)

    (createNode :: queuedNode :: Nil).filterNot(_ == NoSymbol)
  }

  def mkGetNodeDefs(tree: ValOrDefDef, nodeInfo: Attachment.Node): List[ValOrDefDef] = {
    val node = NodeTree(tree, nodeInfo)

    val createNodeDef = if (node.createNodeSym ne NoSymbol) Some(typedCreateNodeDef(node)) else None

    // convert RHS of original node method to call name$newNode.get or name$newNode.lookupAndGet
    val rewrittenNodeDef = rewriteNodeDef(node)

    val queuedNodeDef = typedQueuedNodeDef(node) // name$queued

    List(rewrittenNodeDef, queuedNodeDef) ++ createNodeDef
  }

  // name$newNode
  private[this] def mkCreateNodeSym(nodeSym: Symbol, nodeInfo: Attachment.Node): Symbol = {
    val createNodeName = mkCreateNodeName(nodeSym.name)

    // deliberately call owner.newMethod here (rather than newMethod(owner, ...)), since we don't want to pick
    // up the flags and privateWithin of sym
    val createNodeSym = nodeSym.owner
      .newMethod(createNodeName, nodeSym.pos, clonedMethodFlags(nodeSym.rawflags))
      .setPrivateWithin(nodeSym.privateWithin)

    createNodeSym.addAnnotation {
      if (nodeSym.isGetter)
        ValNodeCreatorAnnotation
      else if (nodeInfo.rt == TriState.False)
        DefAsyncCreatorAnnotation
      else
        DefNodeCreatorAnnotation
    }
    if (nodeInfo.tweakable == TriState.True) createNodeSym.addAnnotation(TweakableAnnotation)
    if (!nodeInfo.async) createNodeSym.addAnnotation(AsyncOffAnnotation)
    if (nodeSym.hasAnnotation(AsyncAlwaysUniqueAnnotation)) createNodeSym.addAnnotation(AsyncAlwaysUniqueAnnotation)
    if (nodeSym.hasAnnotation(JobAnnotation)) createNodeSym.addAnnotation(DefJobCreatorAnnotation)
    nodeSym.getAnnotation(NodeDebugAnnotation).foreach(ann => createNodeSym.addAnnotation(ann))
    if (ForceNodeClassAnnotations.exists(nodeSym.hasAnnotation) || hasExposeArgTypes(nodeSym)) {
      Attachment.ForceNodeClass put createNodeSym
    }

    val createNodeType =
      methodType(nodeSym.info, if (nodeInfo.raw) Node else CovariantPropertyNode, createNodeSym) match {
        case NullaryMethodType(restpe) =>
          // manually semi-uncurry: there's no way to get scalac to invokespecial without an Apply node being involved
          MethodType(Nil, restpe)
        case other => other
      }
    createNodeSym.setInfo(createNodeType)
    resetOverrideIfNecessary(createNodeSym)

    if (scalaVersionRange("2.13:"): @staged) {
      // ensure all `match` trees are desugared, including those that could be emitted as switches.
      // the async transform doesn't handle match trees correctly (OPTIMUS-69378).
      createNodeSym.updateAttachment(ForceMatchDesugar)
    }

    if (nodeSym.isSuperAccessor) {
      val referee = nodeSym.asTerm.referenced
      setSuperReference(createNodeSym.asMethod, referee.owner, mkCreateNodeName(referee.name))
    }
    createNodeSym
  }
  private[this] def typedCreateNodeDef(node: NodeTree): DefDef = {
    val resultType = node.sym.info.finalResultType
    val createNodeSym = node.createNodeSym

    val createNodeRhs =
      if (node.rhs.isEmpty || createNodeSym.isDeferred) { // probably redundant but won't hurt
        EmptyTree
      } else if (curInfo.isLoom) {
        addLoomIfMissing(node.sym.owner)
        // The next line needs to be removed when $impl goes away
        if (!node.sym.hasAnnotation(AsyncAnnotation)) addIfMissing(node.sym, NodeAnnotation)
        q"null".setType(createNodeSym.info.finalResultType)
      } else if (node.sym.hasAttachment[NodeFieldType.GetterDef] || node.sym.hasAttachment[NodeFieldType.BareVal]) {
        val cls = node.owner
        val encInfo = cls.info
        // unexpandedName is needed here because node.sym may be private to an enclosing (non-package) class, and such
        // symbols have their names mangled by superaccessors (see sketchiness comment in genStoredPropertyLoad).
        // However, valaccessors has a forceful warning against
        val nodeName = nodeNameFromCreateNodeName(nme.unexpandedName(node.name)) // val foo
        val nodeSymbol = node.sym // encInfo.decl(nodeName)
        val backingGetterName = mkImplName(nodeName) // def foo$impl
        val backingName = encInfo.decl(backingGetterName)
        val isBlocking = nodeSymbol.attachments.get[Attachment.Stored].exists(_.lazyLoaded)
        genStoredPropertyLoad(nodeSymbol, backingName, isBlocking)
      } else {
        val nodifyArg = moveTree(node.rhs, node, createNodeSym, createNodeSym)
        val nodify = if (node.nodeInfo.raw) NodifySelect else NodifyPropertySelect
        val nodifyTyped = TypeApply(nodify, TypeTree(resultType) :: Nil)
        // convert nodify type `A => (Property)Node[A]` to applied type `Foo => (Property)Node[Foo]`
        nodifyTyped.setType(nodify.tpe.resultType.instantiateTypeParams(nodify.tpe.typeParams, resultType :: Nil))

        Apply(nodifyTyped, nodifyArg :: Nil).setType(createNodeSym.info.finalResultType)
      }

    // The "non sensible equals" checks in the standard compiler do _not_ run
    // for synthetic methods. But we need to set the x$newNode method as synthetic
    // to avoid running into "class inherits conflicting members x$node" in certain
    // inheritance patterns.
    //
    // Here, we reuse code in the refchecks transform to check the code in the context on the
    // non-synthetic node.sym method.
    //
    // This issue does not effect the Loom build, which doesn't wrap the user code in x$newNode.
    val miniRefchecks = new global.refChecks.RefCheckTransformer(currentUnit) {
      override def transform(tree: Tree): Tree = {
        tree match {
          case CaseDef(pat, guard, body) =>
            transform(guard)
            transform(body)
          case treeInfo.Application(fun, targs, argss) =>
            checkSensible(tree.pos, fun, argss)
            tree.transform(this)
          case _ =>
            tree.transform(this)
        }
        tree
      }
    }
    miniRefchecks.transformStats(List(node.tree), node.sym)

    newTypedDefDef(createNodeSym, createNodeRhs)
  }

  // Attempt to move symbols from the old method (the untransformed node method) to the
  // new method (the $newNode method). Inspired by scala.tools.nsc.ExtensionMethods.
  // Note: it says "attempt", because this doesn't work, revisit when this is fixed:
  // https://github.com/scala/bug/issues/11383
  private[this] def moveTree(orig: Tree, oldMethod: NodeTree, newMethod: Symbol, newContainer: Symbol): Tree = {
    val newTree = orig
      .substituteSymbols(oldMethod.tparams.map(_.symbol), newMethod.typeParams)
      .substituteSymbols(oldMethod.vparamss.flatten.map(_.symbol), newMethod.paramLists.flatten)
      .changeOwner(oldMethod.sym, newContainer)

    if (newTree.tpe <:< oldMethod.restpe)
      newTree
    else
      gen.mkCastPreservingAnnotations(newTree, oldMethod.restpe) // scala/bug#7818 e.g. mismatched existential skolems
  }

  private[this] def rewriteNodeDef(node: NodeTree): ValOrDefDef = {
    if (curInfo.isLoom) return node.tree
    val newRhs =
      if ((node.createNodeSym eq NoSymbol) || node.createNodeSym.isDeferred) EmptyTree
      else {
        // change rhs of node method
        val get =
          if (node.sym.hasAnnotation(JobAnnotation)) // [JOB_EXPERIMENTAL]
            if (node.nodeInfo.raw) defGetJob else defLookupAndGetJob
          else if (node.nodeInfo.raw) defGet
          else if (node.sym.hasAnnotation(ScenarioIndependentAnnotation)) defLookupAndGetSI
          else defLookupAndGet
        Select(callCreateNode(node.sym, node.createNodeSym), get).setType(node.rhs.tpe)
      }

    node.sym.resetFlag(ACCESSOR)
    // Always rewrite to a DefDef, even if we were originally dealing with a ValDef
    newTypedDefDef(node.sym, newRhs)
  }

  // name$queued
  private[this] def mkQueuedNodeSym(nodeSym: Symbol, nodeInfo: Attachment.Node): Symbol = {
    val queuedNodeName = mkGetNodeName(nodeSym.name)
    val queuedNodeSym = nodeSym.owner
      .newMethod(queuedNodeName, nodeSym.pos, clonedMethodFlags(nodeSym.rawflags))
      .setPrivateWithin(nodeSym.privateWithin)

    if (nodeInfo.rt == TriState.False) setMiscFlags(queuedNodeSym, MiscFlags.NOT_RT)

    val queuedNodeType = methodType(nodeSym.info, NodeFuture, queuedNodeSym)
    queuedNodeSym.setInfo(queuedNodeType)
    resetOverrideIfNecessary(queuedNodeSym)

    if (nodeSym.isSuperAccessor) {
      val referee = nodeSym.asTerm.referenced
      setSuperReference(queuedNodeSym.asMethod, referee.owner, mkGetNodeName(referee.name))
    }
    queuedNodeSym
  }
  private[this] def typedQueuedNodeDef(node: NodeTree): DefDef = {
    val queuedNodeSym = node.queuedNodeSym

    val queuedNodeRhs =
      if ((node.createNodeSym eq NoSymbol) || node.createNodeSym.isDeferred) EmptyTree
      else if (curInfo.isLoom) {
        q"null".setType(queuedNodeSym.info.finalResultType)
      } else {
        val enqueue =
          if (node.sym.hasAnnotation(JobAnnotation)) // [JOB_EXPERIMENTAL]
            if (node.nodeInfo.raw) defEnqueueJob else defLookupAndEnqueueJob
          else if (node.nodeInfo.raw) defEnqueue
          else defLookupAndEnqueue
        gen.mkAttributedSelect(callCreateNode(queuedNodeSym, node.createNodeSym), enqueue)
      }

    newTypedDefDef(queuedNodeSym, queuedNodeRhs)
  }

  /**
   * Sets the "referenced" symbol of superAccessor to the corresponding method (superName) in the superOwner with the
   * matching type signature. This would usually be done by scalac's superaccessor's phase, except that it doesn't know
   * about $queued / $newNode methods (because we only just generated them here), so we need to do it. Much later on,
   * scalac's mixins phase will implement these methods on any implementation classes as a simple call to the referenced
   * method.
   */
  private[this] def setSuperReference(superAccessor: MethodSymbol, superOwner: Symbol, superName: TermName): Unit = {
    // phase travel needed, to ensure that superOwner has the $newNode/$queued method we're targeting
    // decl not member is safe here, because the relevant method was codefined with its original superaccessor
    val referee = exitingPhase(nodeMethodsPhase()) { superOwner.info.decl(superName) }
    // This method will *always* exist unless something went horribly wrong with node method generation in superOwner
    // (i.e. there are no user coding errors which would cause it to be missing, only compiler bugs) therefore we
    // don't have any pretty user-facing Optimus error message for this
    def fail =
      internalErrorAbort(
        superAccessor.pos,
        s"unable to find superaccessor target: $superName on $superOwner (for ${superAccessor.fullLocationString}")
    superAccessor.referenced = if (referee.isOverloaded) {
      val matched = referee.alternatives.find(_.tpe.matches(superAccessor.tpe))
      matched getOrElse fail
    } else referee orElse fail
  }

  private[this] def callCreateNode(fromSym: Symbol, createNodeSym: Symbol) = {
    val fun = gen.mkAttributedRef(createNodeSym)
    val args = mmap(fromSym.info.paramss) { param =>
      gen.mkAttributedIdent(param).applyIf[Tree](definitions.isRepeated(param)) { ref =>
        /* Do the secret handshake to turn `dingo` into `dingo : _*`, but typedly.
         * This is because `woozle$newNode` will have the same (modulo Node-wrapping) type as the original `woozle`,
         * including any varargs params that `woozle` may have, so when calling it with `Ident(param)` (which has type
         * `scala.<repeated>[Folly]` or somesuch) we need to re-splat the varargs. We could instead give the node
         * creator method a pre-uncurried type here, but then we'd have to munge the args to turn any by-name parameter
         * `f` into `f _`, which is just as burdensome, so best to keep this here and let uncurry do the uncurrying.
         */
        val tptTpe = param.tpe.baseType(definitions.SeqClass).typeArgs.head
        Typed(ref, Ident(tpnme.WILDCARD_STAR) setType tptTpe) setType tptTpe
      }
    }

    // Use tpeHK (rather than tpe) here because we want to get `A` rather than `A[_]` for higher-kinded type params
    val tparams = fromSym.info.typeParams.map(_.tpeHK)
    /* $newNode is public and will be overridden whenever its source method is. If the source method or its $queued
     * counterpart is called with invokespecial (because it's a super invocation, for instance) we need to also invoke
     * $newNode specially, or we'll do virtual dispatch again, defeating the purpose. It's in fact always safe to use
     * invokespecial, because if virtual dispatch was intended, the calling method will already be resolved virtually.
     * See the comment in mkCreateNodeSym as to why we don't pass an empty argss.
     */
    mkTypedApply(fun, if (args.isEmpty) ListOfNil else args, tparams).updateAttachment(UseInvokeSpecial)
  }

  private[this] def newTypedDefDef(sym: Symbol, rhs: Tree): DefDef = {
    val dd = newDefDef(sym, rhs)().setType(NoType)
    dd.tparams.foreach(SimpleTyper.typed)
    dd.vparamss.foreach(_.foreach(_.setType(NoType)))
    dd
  }

  def methodType(orig: Type, resultWrapper: Symbol, newMethodSym: Symbol): Type = {
    import definitions._

    def wrapResultType(tp: Type, decomposeFunctions: Boolean): Type = tp match {
      case PolyType(typeParams, resultType) =>
        PolyType(typeParams, wrapResultType(resultType, decomposeFunctions))
      case MethodType(params, resultType) =>
        MethodType(params.map(param), wrapResultType(resultType, decomposeFunctions))
      case NullaryMethodType(resultType) =>
        NullaryMethodType(wrapResultType(resultType, decomposeFunctions))
      case TypeRef(_, ByNameParamClass, arg :: Nil) =>
        wrapResultType(arg, decomposeFunctions)
      case tr: TypeRef if decomposeFunctions && FunctionClass.contains(tr.typeSymbol) =>
        FunctionClass.specificType(tr.typeArgs.init, wrapResultType(tr.typeArgs.last, decomposeFunctions) :: Nil)
      case _ =>
        appliedType(resultWrapper, tp).dealias
    }
    def param(sym: Symbol): Symbol =
      if (hasNodeLiftAnno(sym))
        sym.cloneSymbol.setInfo(wrapResultType(sym.tpe, decomposeFunctions = true))
      else sym
    if (newMethodSym != NoSymbol)
      wrapResultType(orig, decomposeFunctions = false).cloneInfo(owner = newMethodSym)
    else
      wrapResultType(orig, decomposeFunctions = false)
  }

  /** Adjust the flags for a node method to the flags for a synthetic method made by this phase. */
  final def clonedMethodFlags(oldFlags: Long): Long =
    (oldFlags & ~(ACCESSOR | PARAMACCESSOR)) | SYNTHETIC

  def genStoredPropertyLoad(propGetter: Symbol, stored: Symbol, isBlocking: Boolean): Tree = {
    if (isBlocking) {
      gen.mkAttributedSelect(gen.mkAttributedThis(propGetter.enclClass), stored)
    } else {
      val infoHolder = infoHolderForSym(propGetter.owner)
      val module = infoHolder.module
      val propInfo = {
        // Sketchy! Say you've written
        //     @entity class O { @entity class I(private[O] val wat: Int) }
        // Obviously we want I's companion to have a `def wat: PropertyInfo` or somesuch. But, by the time we get here,
        // our underlying property `wat` isn't named that anymore! Superaccessors has mangled it to something complicated
        // like `optimus$platform$example$O$I$$wat`.
        val name = mkPropertyInfoName(infoHolder, propGetter.unexpandedName)
        module.info.decls.find(sym => sym.unexpandedName == name && sym.isGetter).getOrElse {
          abort(s"Internal error: No PropertyInfo found on $module named $name")
        }
      }

      // cannot assert propGetter.tpe <:< stored.tpe because it would fail on generic type situation
      val rhs =
        if (propGetter.tpe != stored.tpe)
          gen.mkCast(gen.mkAttributedSelect(gen.mkAttributedThis(propGetter.enclClass), stored), propGetter.tpe)
        else
          gen.mkAttributedSelect(gen.mkAttributedThis(propGetter.enclClass), stored)

      val ownerIsEntity = isEntity(propGetter.owner)
      if (ownerIsEntity && propGetter.owner.hasAnnotation(StoredAnnotation)) {
        PluginSupport
          .observedValueNode(
            rhs,
            gen.mkAttributedThis(propGetter.owner),
            gen.mkAttributedSelect(gen.mkAttributedIdent(module), propInfo))
          .setType(appliedType(PropertyNode, rhs.tpe.resultType :: Nil))
      } else {
        val nodeTypeConstructor = if (ownerIsEntity) {
          AlreadyCompletedPropertyNode.tpe
        } else {
          // we're a lazily loaded property on an @event, for example
          AlreadyCompletedNode.tpe
        }
        val nodeType = appliedType(nodeTypeConstructor, propGetter.tpe.resultType :: Nil)

        val args = if (ownerIsEntity) {
          List(
            rhs,
            gen.mkAttributedThis(propGetter.owner),
            gen.mkAttributedSelect(gen.mkAttributedIdent(module), propInfo))
        } else {
          List(rhs)
        }
        Apply(
          gen.mkAttributedSelect(
            New(TypeTree(nodeType)).setType(nodeType),
            nodeType.decl(nme.CONSTRUCTOR)
          ),
          args
        ).setType(nodeType)
      }
    }
  }
}
