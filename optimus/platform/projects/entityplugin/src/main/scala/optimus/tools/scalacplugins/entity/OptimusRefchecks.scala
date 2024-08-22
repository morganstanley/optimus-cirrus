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

import optimus.entityplugin.config.StaticConfig
import optimus.tools.scalacplugins.entity.Attachment.Stored
import optimus.tools.scalacplugins.entity.reporter.OptimusAlarmBuilder1
import optimus.tools.scalacplugins.entity.reporter.OptimusAlarmBuilder2
import optimus.tools.scalacplugins.entity.reporter.OptimusErrors
import optimus.tools.scalacplugins.entity.reporter.OptimusNonErrorMessages

import scala.annotation.tailrec
import scala.tools.nsc._
import scala.tools.nsc.symtab.Flags._

object OptimusRefchecksComponent {
  val unusedAlsoSetInheritanceSuffix =
    "it does not override an also set in a parent class containing the corresponding tweakable node"
  val unusedAlsoSetSuffix = "no corresponding tweakable node"
}

class OptimusRefchecksComponent(val plugin: EntityPlugin, val phaseInfo: OptimusPhaseInfo)
    extends EntityPluginComponent
    with TypedUtils
    with WithOptimusPhase
    with refchecks.IteratorUsage
    with refchecks.ContextualChecks {
  import global._

  override def newPhase(prev: Phase): Phase = new EntityPluginStdPhase(prev) {
    override def apply0(unit: CompilationUnit): Unit = {
      val refchecks = new OptimusRefchecks(unit)
      refchecks.traverse(unit.body)

      // TODO (OPTIMUS-56313): Remove the if-statement so that we extend our iterator check to all compilation
      // units, not just those that contain the name "iterator".
      if (refchecks.containsIteratorUsage) new OptimusIteratorLinter().traverse(unit.body)
      new ContextualChecker().traverse(unit.body)
    }
  }

  object phaseDefns {
    import definitions._

    val MapClass = rootMirror.getRequiredClass("scala.collection.immutable.Map")

    val forbiddenSymbols: Map[Name, Option[Type]] = PropertyNode.info.members
      .filter(mem => mem.isMethod && flattensToEmpty(mem.info.paramss))
      .map(i => i.name -> (if (i.isOverridableMember) Some(i.info.finalResultType) else None))
      .toMap

    val validEventOrEntityTraits = Set[Symbol](
      ObjectClass,
      AnyClass,
      StorableClass,
      EntityClass,
      BusinessEventClass,
      EmbeddableCompanionBaseClass,
      EmbeddableTraitCompanionBaseClass,
      KeyedEntityCompanionBaseCls
    )
  }

  class OptimusRefchecks(unit: CompilationUnit) extends Traverser {
    import global._
    import global.definitions._
    import phaseDefns._
    import staged.scalaVersionRange

    private def toleratedStoredObjects: Set[String] = alarms.AlarmSuppressions._22906
    var containsIteratorUsage: Boolean = false

    object validateTweakableVariance extends ValidateVariance(OptimusErrors.TWEAKNODE_WITH_COVARIANT_TYPE)
    object validateCopyableVariance extends ValidateVariance(OptimusErrors.COPYABLE_WITH_COVARIANT_TYPE)
    class ValidateVariance(malfeasance: OptimusAlarmBuilder2) {
      def issueVarianceErrorImpl(base: Symbol, sym: Symbol, required: Variance): Unit = {
        if (required == Variance.Contravariant && sym.isCovariant)
          alarm(malfeasance, pos, base.info, sym.nameString)
      }
      private[this] var pos: Position = NoPosition
      val real: VarianceValidator = if (scalaVersionRange("2.13:"): @staged) {
        new VarianceValidator {
          // normally this issues a scalac error "covariant type A occurs in contravariant position..."
          // but we hijack this method (thankfully neither private nor final!) to issue our alarm instead
          override def issueVarianceError(base: Symbol, sym: Symbol, required: Variance, tpe: Type): Unit =
            issueVarianceErrorImpl(base, sym, required)
        }
      } else {
        new VarianceValidator {
          override def issueVarianceError(base: Symbol, sym: Symbol, required: Variance): Unit =
            issueVarianceErrorImpl(base, sym, required)
        }
      }

      def apply(sym: Symbol, pos: Position): Unit = {
        this.pos = pos
        // pretend we wrote `def woozle(val test: ${sym.info}) {}` and check that parameter only
        // `newSyntheticValueParam` sets the `PARAM` flag which ensures that covariant types don't appear in negative
        // position within `sym.info`, which is what we're trying to check
        real(ValDef(sym.owner.newSyntheticValueParam(sym.info), EmptyTree))
      }
    }

    // For checking use of XXX.now as tweak body
    val badTweakBodySyms = {
      val At = rootMirror.getModuleIfDefined("optimus.platform.At")
      val Instant = rootMirror.getRequiredModule("java.time.Instant")
      val ZonedDateTime = rootMirror.getRequiredModule("java.time.ZonedDateTime")
      Set(At, Instant, ZonedDateTime) filter { _ != NoSymbol } map {
        definitions.getMember(_, newTermName("now"))
      }
    }

    def isTweakCtor(fun: Tree): Boolean = {
      val fSym = fun.symbol
      TweakTargetColonEquals.alternatives.contains(fSym) && fSym.paramss.head.head.isByNameParam
    }

    val inAutoGenCreationMethodsStatus = new StateHolder(false)
    def inAutoGenCreationMethods = inAutoGenCreationMethodsStatus.currentValue
    def atAutoGenCreationMethods(isInAutoGenCreationMethods: Boolean)(f: => Unit) =
      inAutoGenCreationMethodsStatus.enterNewContext(isInAutoGenCreationMethods)(f)

    def checkTree(tree: Tree, params: Set[Symbol]) = {
      object ParamAccChecker extends Traverser {
        override def traverse(tree: Tree): Unit = {
          tree match {
            case s: Select if (params.contains(s.symbol)) =>
              alarm(OptimusNonErrorMessages.TRANSIENT_CTOR_PARAM_USE, s.pos)
            case t => super.traverse(t)
          }
        }
      }
      ParamAccChecker.traverse(tree)
    }

    def checkParamAccessors(tree: ClassDef): Unit =
      if (!tree.mods.isTrait && tree.symbol.hasAnnotation(StoredAnnotation)) {
        def isStored(inpSymbol: Symbol) = {
          val accessed = if (inpSymbol.isGetter) inpSymbol.accessed else inpSymbol

          assert(accessed != NoSymbol || inpSymbol.isDeferred, inpSymbol.pos.toString)

          !inpSymbol.isDeferred &&
          !accessed.owner.isModule &&
          (accessed hasAnnotation StoredAnnotation)
        }

        val stats = tree.impl.body
        val paramAccs: Set[Symbol] = stats.collect {
          case vd: ValDef
              if (vd.mods.isParamAccessor) &&
                (!(vd.symbol.hasAnnotation(NodeAnnotation) || vd.symbol.hasAnnotation(StoredAnnotation))
                  || vd.symbol.hasAnnotation(TransientAttr)) =>
            vd.symbol
        }.toSet

        if (!paramAccs.isEmpty) {
          // scan all the non-private defs and transient val nodes
          stats foreach {
            case synth: ValOrDefDef if synth.symbol.isSynthetic => // skip
            case vd: ValDef =>
              if (!isStored(vd.symbol) && !paramAccs.contains(vd.symbol)) checkTree(vd, paramAccs)
            case dd: DefDef => if (!dd.symbol.isConstructor) checkTree(dd, paramAccs)
            case t          => checkTree(t, paramAccs)
          }
        }
      }

    @tailrec
    private def checkPathIsEmbeddable(symToMatch: Symbol, select: Select, expectThis: Boolean = true): Boolean = {
      select.qualifier match {
        case th: This if expectThis => th.symbol == symToMatch && isStableOrValAccessor(select.symbol)
        // to check the function body for @indexed definition like embeddables.map(x => x.a.b.c.SomeEntityField)
        case i: Ident if !expectThis => i.symbol == symToMatch
        case s @ Select(_, _) =>
          val tpe = s.tpe.bounds.hi
          val annotated = tpe.typeSymbol.hasAnnotation(EmbeddableAnnotation)
          annotated && isStableOrValAccessor(s.symbol) && checkPathIsEmbeddable(symToMatch, s, expectThis)
        case _ => false
      }
    }

    private def isStableOrValAccessor(sym: Symbol): Boolean =
      sym.isStable || sym.hasAnnotation(ValAccessorAnnotation)

    def checkKeyOrIndex(dd: DefDef) = {
      def isStableNode(sel: Select): Boolean = {
        val ss = sel.symbol
        !isTweakable(sel) && // if @node(tweak = true), it's not stable
        (ss.isFinal || ss.owner.isFinal || isNonTweakable(
          sel) || // if 'the field is final' or 'the owner is final' or 'is marked @node(tweak = false)'
          // or the owner of the field is sealed, and all it's overridden version are non-tweakable node (the owner must be sealed or final as well)
          (ss.owner.isSealed && (ss.owner.knownDirectSubclasses forall { cls =>
            val overridenMethod = cls.info.decl(ss.name)
            // if overridden method is not @node(tweak = false), it must be sealed or final (so that no subclass can skip the check and override with @node(tweak = true)
            (!overridenMethod.hasAnnotation(TweakableAnnotation) && (cls.isSealed || cls.isFinal)) ||
            overridenMethod.hasAnnotation(
              NonTweakableAnnotation
            ) // if overridden with @node(tweak = false), we are safe, no need to sealed or final
          })))
      }

      def isValidC2PType(tpe: Type) = {
        (tpe.typeSymbol.isSubClass(ImmutableSetClass) || tpe.typeSymbol.isSubClass(CovariantSetClass)) && {
          isTypeRefStoredEntity(tpe)
        }
      }

      def isValidEmbeddableCollToMapOver(tpe: Type) = {
        isValidIndexCollectionType(tpe.typeSymbol) && isTypeRefEmbeddable(tpe)
      }

      def isTypeRefArgMatched(tpe: Type, symbolCheck: Symbol => Boolean) = {
        tpe.dealias match {
          case TypeRef(_, _, arg :: Nil) =>
            val argSym = arg.typeSymbol
            if (argSym.isAbstract) symbolCheck(arg.bounds.hi.typeSymbol)
            else symbolCheck(argSym)
          case _ =>
            false
        }
      }

      @inline def isValidIndexCollectionType(typeSym: Symbol) = {
        typeSym.isSubClass(definitions.IterableClass) || typeSym.isSubClass(definitions.ArrayClass)
      }

      @inline def isTypeRefStoredEntity(tpe: Type) = {
        isTypeRefArgMatched(tpe, isStoredEntity)
      }

      @inline def isTypeRefEmbeddable(tpe: Type) = {
        isTypeRefArgMatched(tpe, isEmbeddable)
      }

      val defSym = dd.symbol
      val isIndex = defSym.hasAnnotation(IndexedAnnotation)
      val isKey = defSym.hasAnnotation(KeyAnnotation)
      val isStoredC2P = defSym.hasAnnotation(C2PAnnotation)
      val isKeyOrIndex = isIndex || isKey
      dd match {
        case vodd: DefDef if isStoredC2P && !isValidC2PType(vodd.symbol.tpe.resultType) =>
          alarm(OptimusErrors.CHILD_PARENT_MUST_BE_SET, dd.pos)
        case DefDef(_, _, _, _, _, sel @ Select(qual, name))
            if isKeyOrIndex &&
              // for 2.12, if we have valAccessor, it comes from val definition, which is fine for index
              !defSym.hasAnnotation(ValAccessorAnnotation) =>
          val isEmbeddablePath = checkPathIsEmbeddable(defSym.owner, sel)
          if (!isEmbeddablePath)
            alarm(OptimusErrors.INDEX_MUST_EMBEDDABLE_PATH, dd.pos)
          else if (!sel.symbol.isParamAccessor && !isEntityOrEvent(sel.symbol.owner))
            alarm(OptimusErrors.INDEX_MUST_REFER_CTOR_PARAM, dd.pos)
        // for @indexed def and rhs is embeddables.map(_.someStoredEntityField)
        case DefDef(
              _,
              _,
              _,
              _,
              _,
              treeInfo.Applied(
                (s1 @ Select(s2 @ Select(_: This | _: Super, _), _)),
                _,
                (Function(p :: Nil, body @ Select(_, _)) :: Nil) :: _))
            if isKeyOrIndex && s1.name == nme.map && s1.symbol.owner.isNonBottomSubClass(TraversableLikeClass) =>
          val mapOnEmbeddables = isValidEmbeddableCollToMapOver(s2.symbol.tpe.resultType)
          val mapToEntityField = isEntity(body.tpe.typeSymbol)
          val embeddablePath = checkPathIsEmbeddable(p.symbol, body, expectThis = false)
          if (!mapOnEmbeddables || !isStableOrValAccessor(s2.symbol) || !mapToEntityField || !embeddablePath)
            alarm(OptimusErrors.INDEX_SEQ_DEF_MAP_ON_ENTITY, dd.pos, dd.rhs)
        // for @indexed def and rhs is Tuple Apply
        case DefDef(_, _, _, _, _, ap @ Apply(fun, args)) if isKeyOrIndex =>
          args foreach {
            case sel @ Select(_: This | _: Super, x) =>
              if (
                !isStableOrValAccessor(sel.symbol) || (sel.symbol.hasAnnotation(NodeAnnotation) && !isStableNode(sel))
              )
                alarm(OptimusErrors.KEY_INDEX_MUST_STABLE2, sel.pos)
            case tree =>
              alarm(OptimusErrors.CANT_GENERATE_INDEX2, tree.pos, showRaw(ap))
          }
        case DefDef(_, _, _, _, _, rhs) if isKeyOrIndex && !defSym.hasAnnotation(ValAccessorAnnotation) =>
          rhs match {
            // singleton use case: @key def foo = ()
            case Literal(Constant(())) =>
            case _                     => alarm(OptimusErrors.CANT_GENERATE_INDEX2, dd.pos, showRaw(dd))
          }
        case _ =>
      }
    }
    private def checkIllegalAnnotation(
        tree: Tree,
        symbol: Symbol,
        alarmBuilder: OptimusAlarmBuilder1,
        name: Symbol): Unit = {
      if (symbol.hasAnnotation(name))
        alarm(alarmBuilder, tree.pos, name.decodedName)
    }
    def checkDefDef(dd: DefDef) = {
      val defSym = dd.symbol
      val overriddenSyms = defSym.asMethod.allOverriddenSymbols
      val isTweakable = defSym.hasAnnotation(TweakableAnnotation)
      val isNonTweakable = defSym.hasAnnotation(NonTweakableAnnotation)
      val hasNodeSync = defSym.hasAnnotation(NodeSyncAnnotation)
      val hasNodeAnnotation = defSym.hasAnnotation(NodeAnnotation)
      val hasElevatedAnnotation = defSym.hasAnnotation(ElevatedAnnotation)
      val hasAsyncAnnotation = defSym.hasAnnotation(AsyncAnnotation)
      val isStored = defSym.hasAttachment[Stored]

      val definesAlsoSet = isTweakHandler(dd.name)
      if (definesAlsoSet) {
        def checkIfTweakForHandlerExists(clazz: Symbol): Boolean = {
          val methods = clazz.info.resultType.decls.toList
          methods.exists(m =>
            m.hasAnnotation(TweakableAnnotation) && dd.name
              .dropRight(names.tweakHandlerSuffix.length)
              .string_==(m.name))
        }
        val existsInCurrentClass = checkIfTweakForHandlerExists(defSym.owner)
        val existsInAncestors = defSym.owner.ancestors.exists(checkIfTweakForHandlerExists)
        if (!existsInCurrentClass)
          if (existsInAncestors && !dd.mods.isOverride)
            alarm(
              OptimusNonErrorMessages.UNUSED_ALSO_SET,
              dd.pos,
              dd.name.toString,
              OptimusRefchecksComponent.unusedAlsoSetInheritanceSuffix)
          else if (!existsInAncestors)
            alarm(
              OptimusNonErrorMessages.UNUSED_ALSO_SET,
              dd.pos,
              dd.name.toString,
              OptimusRefchecksComponent.unusedAlsoSetSuffix)
      }

      if (hasNodeAnnotation) {
        checkIllegalAnnotation(dd, defSym, OptimusErrors.NODE_WITH_WRONG_ANNO, ImpureAnnotation)
        checkIllegalAnnotation(dd, defSym, OptimusErrors.NODE_WITH_WRONG_ANNO, AsyncAnnotation)
        checkIllegalAnnotation(dd, defSym, OptimusErrors.NODE_WITH_WRONG_ANNO, ElevatedAnnotation)
        if (!defSym.hasAnnotation(ValAccessorAnnotation))
          checkIllegalAnnotation(dd, defSym, OptimusErrors.NODE_WITH_WRONG_ANNO, ProjectedAnnotation)
        checkIllegalAnnotation(dd, defSym, OptimusErrors.NODE_WITH_WRONG_ANNO, definitions.TailrecClass)
      } else if (hasElevatedAnnotation) {
        checkIllegalAnnotation(dd, defSym.enclClass, OptimusErrors.ELEVATED_WITH_WRONG_ANNOTATION, EmbeddableAnnotation)
        checkIllegalAnnotation(dd, defSym, OptimusErrors.ELEVATED_WITH_WRONG_ANNOTATION, EntersGraphAnnotation)
      } else if (hasAsyncAnnotation) {
        checkIllegalAnnotation(dd, defSym, OptimusErrors.ASYNC_WITH_WRONG_ANNOTATION, definitions.TailrecClass)
      }

      // If dd is @node only, it must not override any @scenarioIndependent @nodes
      if (hasNodeSync && !hasSIInternalAnnotation(defSym)) {
        for (overridden <- overriddenSyms.find(hasSIAnnotation)) {
          alarm(OptimusErrors.NONSI_OVERRIDE_SI, dd.pos, overridden.owner.name, overridden.name)
        }
      }

      /**
       * We will add @nonTweakable for @node(tweak = false) We will add @tweakable for @node(tweak = true ) we only
       * allow @node(tweak = false) override @node(tweak = false) or @node and @node(tweak = true ) override @node(tweak
       * \= true ) or @node We should keep the tweakablity of nodes in the inheritance chain, this is because:
       *
       * @node(tweak
       *   \= true ) override @node(tweak = false), when treat sub class instance as super class, it's scenario
       *   independent, which is wrong
       * @node(tweak
       *   \= false) override @node(tweak = true ), when treat sub class instance as super class, it's tweakable, which
       *   is wrong
       * @node
       *   can't override @node(tweak = true/false), because later inheritance class can override it with @node(tweak =
       *   true/false)
       */
      // If dd is not a tweakable @node, it must not override any tweakable @nodes.
      if (hasNodeSync && !isTweakable) {
        for (overridden <- overriddenSyms.find { _.hasAnnotation(TweakableAnnotation) }) {
          if (isNonTweakable)
            alarm(OptimusErrors.NONTWEAK_OVERRIDE_TWEAK, dd.pos, overridden.owner.name, overridden.name)
          else alarm(OptimusErrors.DEFAULT_OVERRIDE_TWEAK, dd.pos, overridden.owner.name, overridden.name)
        }
      }

      // If dd is not a non-tweakable @node, it must not override any non-tweakable @nodes
      if (hasNodeSync && !isNonTweakable) {
        for (overridden <- overriddenSyms.find { _.hasAnnotation(NonTweakableAnnotation) }) {
          if (isTweakable) alarm(OptimusErrors.TWEAK_OVERRIDE_NONTWEAK, dd.pos, overridden.owner.name, overridden.name)
          else alarm(OptimusErrors.DEFAULT_OVERRIDE_NONTWEAK, dd.pos, overridden.owner.name, overridden.name)
        }
      }

      // check for def overriding val (even for non-@node on @entity or @event, since in Scala 2.12 scala's own
      // refchecks won't reliably detect this problem)
      if ((hasNodeSync || isEntityOrEvent(defSym.owner)) && !isStableOrValAccessor(defSym))
        for (overridden <- overriddenSyms.find(isStableOrValAccessor(_))) {
          alarm(OptimusErrors.DEF_OVERRIDE_VAL(overridden.owner.name, overridden.name), dd.pos)
        }

      // If dd is @async def, it must not override any @node def (in non-entity types)
      if (defSym.hasAnnotation(AsyncAnnotation)) {
        for (overridden <- overriddenSyms.find { _.hasAnnotation(NodeAnnotation) })
          alarm(OptimusErrors.ASYNC_OVERRIDE_NODE, dd.pos)
      }

      // If dd is override a method which is @backed, it must mark as @backed as well
      if (defSym.isOverride && !defSym.hasAnnotation(BackedAnnotation)) {
        for (overridden <- overriddenSyms.find { _.hasAnnotation(BackedAnnotation) })
          alarm(OptimusErrors.NONBACKED_OVERRIDE_BACKED, dd.pos)
      }

      // If dd is a tweakable @node, and it overrides another tweakable @node, the type of the property
      // must be equivalent.
      // Note that the override may fix type parameters that are free in the base, so we need to use memberType/asSeenFrom.
      // See OverridePolyTypeTweakableVal.scala
      if (isTweakable) {
        for (overridden <- overriddenSyms.find { _.hasAnnotation(TweakableAnnotation) }) {
          val derivedResType = defSym.tpe.resultType
          val baseResType = defSym.owner.tpe.memberType(overridden).resultType
          if (!(derivedResType =:= baseResType))
            alarm(
              OptimusErrors.ILLEGAL_COVARIANT_OVERRIDE,
              dd.pos,
              overridden,
              overridden.owner,
              baseResType,
              derivedResType)
        }

        validateTweakableVariance(defSym, dd.pos)
      }
      if (defSym.hasAnnotation(SoapMethodAnnotation) && !defSym.hasAnnotation(handleAnnotation)) {
        alarm(OptimusErrors.SOAP_MUST_BE_HANDLE, dd.pos)
      }

      if (!defSym.isSynthetic && overriddenSyms.nonEmpty && !dd.name.containsName(nme.CONSTRUCTOR)) {
        // we only check the user written code
        val superAnnos = overriddenSyms.flatMap(_.annotations).map(_.symbol).toSet
        val thisAnnos = defSym.annotations.map(_.symbol).toSet
        val missedAnnos = superAnnos.diff(thisAnnos)
        val addedAnnos = thisAnnos.diff(superAnnos)

        // handle special override combinations

        // common handling for the extra/missed annotation in inheritance
        missedAnnos.foreach {
          case anno if anno == GivenRuntimeEnvAnnotation && !thisAnnos.contains(GivenRuntimeEnvAnnotation) =>
            alarm(
              OptimusErrors.LOSE_ANNO_WHEN_OVERRIDE,
              dd.pos,
              dd.name,
              tpnames.givenRuntimeEnv,
              tpnames.givenRuntimeEnv)

          case anno if anno == NodeAnnotation && !isNode(defSym) =>
            alarm(OptimusErrors.LOSE_ANNO_WHEN_OVERRIDE, dd.pos, dd.name, tpnames.node, tpnames.node)

          case anno if anno == AsyncAnnotation && !isNode(defSym) =>
            // isNode picks up on async too; node overriding async is OK (gain purity)
            // async overriding node has special 22106 error
            alarm(OptimusErrors.LOSE_ANNO_WHEN_OVERRIDE, dd.pos, dd.name, tpnames.async, tpnames.async)
          //        Losing impurity is ok; the reverse would not be, so we do not do the following:
          //          case anno if anno == ImpureAnnotation =>
          //            alarm(OptimusErrors.LOSE_ANNO_WHEN_OVERRIDE, dd.pos, dd.name, tpnames.impure, tpnames.impure)
          case anno if anno == handleAnnotation =>
            alarm(OptimusErrors.LOSE_ANNO_WHEN_OVERRIDE, dd.pos, dd.name, tpnames.handle, tpnames.handle)
          case anno
              if anno == EntersGraphAnnotation && !superAnnos.exists(anno =>
                anno == KeyAnnotation || anno == IndexedAnnotation) =>
            alarm(OptimusErrors.LOSE_ANNO_WHEN_OVERRIDE, dd.pos, dd.name, tpnames.entersGraph, tpnames.entersGraph)
          case _ => // other annotations we want to keep in the inheritance chain
        }
        addedAnnos.foreach {
          case anno if anno == NodeAnnotation && !overriddenSyms.exists(isNode) =>
            alarm(OptimusErrors.ADD_ANNO_WHEN_OVERRIDE, dd.pos, tpnames.node, dd.name, tpnames.node)
          case anno if anno == AsyncAnnotation =>
            alarm(OptimusErrors.ADD_ANNO_WHEN_OVERRIDE, dd.pos, tpnames.async, dd.name, tpnames.async)
          case anno if anno == ImpureAnnotation =>
            alarm(OptimusErrors.ADD_ANNO_WHEN_OVERRIDE, dd.pos, tpnames.impure, dd.name, tpnames.impure)
          case anno if anno == handleAnnotation =>
            alarm(OptimusErrors.ADD_ANNO_WHEN_OVERRIDE, dd.pos, tpnames.handle, dd.name, tpnames.handle)
          case anno
              if anno == EntersGraphAnnotation && !thisAnnos.exists(anno =>
                anno == KeyAnnotation || anno == IndexedAnnotation) =>
            alarm(OptimusErrors.ADD_ANNO_WHEN_OVERRIDE, dd.pos, tpnames.entersGraph, dd.name, tpnames.entersGraph)
          case _ => // other annotations we don't want to add in the inheritance chain
        }
      }

      val hasExposeArgTypesParam = hasExposeArgTypes(defSym)
      if (hasExposeArgTypesParam) {
        val curOwner = defSym.owner
        val members = if (curOwner.hasSelfType) curOwner.selfType.members else curOwner.info.members
        members foreach {
          case s
              if s.name == defSym.name && hasExposeArgTypes(s) && s != defSym && s.pos
                .precedes(defSym.pos) =>
            alarm(OptimusErrors.CREATE_NODE_TRAIT_OVERLOAD_ERROR, dd.pos, s.pos.line)
          case other => // skip the correct ones
        }
      }

      if (((dd.name == nme.apply) || (dd.name == names.uniqueInstance)) && isEntityCompanion(defSym.owner)) {
        val isApply = dd.name == nme.apply
        val badName = if (isApply) names.uniqueInstance else nme.apply
        val badSyms = defSym.owner.info.member(badName).alternatives
        val isBad = dd.rhs.exists {
          case t: RefTree => badSyms.contains(t.symbol)
          case _          => false
        }
        if (isBad) {
          val ownerName = defSym.owner.name
          alarm(OptimusErrors.USER_BAD_ENTITY_CREATER, dd.pos, ownerName, dd.name, ownerName, badName)
        }
      }

      if (defSym.hasAnnotation(ReifiedAnnotation)) {
        if (!defSym.isFinal)
          alarm(OptimusErrors.REIFIED_INVALID_CLASS, dd.pos)
        val reifiedOpt = dd.rhs.find(t => t.hasSymbolWhich(s => s.hasAnnotation(ReifiedAnnotation)))
        reifiedOpt.foreach(t => alarm(OptimusErrors.REIFIED_REF_REIFIED, dd.pos))

        val ownerSym = defSym.owner
        if (!ownerSym.isClass || ownerSym.isModuleClass)
          alarm(OptimusErrors.REIFIED_INVALID_CLASS, dd.pos)
        if (!isEntityOrEvent(ownerSym) && !isEmbeddable(ownerSym))
          alarm(OptimusErrors.REIFIED_INVALID_CLASS, dd.pos)
        if (defSym.hasAnnotation(TweakableAnnotation))
          alarm(OptimusErrors.REIFIED_WITH_TWEAKABLE, dd.pos)
      }
      checkKeyOrIndex(dd)

      // $init methods are synthesized too early to tell if they override something, so fake it here
      if (dd.name endsWith "$init") resetOverrideIfNecessary(defSym)

      if (hasNodeAnnotation || hasExposeArgTypesParam) {
        // checking for forbidden argument names for @node / @createNodeTraitAnnotation defs which would conflict with
        // PropertyNode methods
        if (isEntity(defSym.owner) && !defSym.isDeferred) {
          for (param <- dd.vparamss.flatten) {
            forbiddenSymbols
              .get(param.name)
              .foreach(typOpt =>
                if (typOpt.forall(_.erasure =:= param.symbol.info.erasure)) {
                  alarm(OptimusErrors.FORBIDDEN_PARAM_NAME, dd.pos, dd.name, param.name.toString())
                })
          }
        }
      }

      if (hasNodeAnnotation) {
        if (dd.vparamss.exists(_.exists(_.tpt.tpe.typeSymbol == definitions.ArrayClass))) {
          alarm(OptimusNonErrorMessages.MUTABLE_NODE_PARAMS, dd.pos)
        }
        if (dd.tpt.tpe.typeSymbol == definitions.ArrayClass) {
          alarm(OptimusNonErrorMessages.MUTABLE_NODE_RETTYPES, dd.pos)
        }
      }

      // for now @job methods support only a single parameter, which must be a @stored @entity
      if (defSym.hasAnnotation(JobAnnotation)) { // [[JOB_EXPERIMENTAL]]
        dd.vparamss match {
          case List(List(singleParam)) if isStoredEntity(singleParam.tpt.tpe.typeSymbol) =>
          case _ => alarm(OptimusErrors.JOB_SINGLEPARAM_NONSTOREDENTITY, dd.pos)
        }
      }
    }

    private[this] def checkParents(cd: ImplDef) = {
      val cdSym = cd.symbol
      val isTransient = cdSym.isModule || !cdSym.hasAnnotation(StoredAnnotation)
      val isInline = cdSym.hasAnnotation(ScalaInlineClass)
      cdSym.info.parents foreach { p =>
        val psym = p.typeSymbol
        if (!isTransient && !isInline && isEntity(psym) && !psym.hasAnnotation(StoredAnnotation)) {
          alarm(OptimusErrors.TRANSIENT_INHERIT, cd.pos, cdSym, p)
        }
        if (psym.hasAnnotation(EmbeddableAnnotation))
          alarm(OptimusErrors.ENTITY_EXTEND_EMBEDDABLE, cd.pos, cdSym, psym)
        else if (psym.isCase)
          alarm(OptimusErrors.ENTITY_EXTEND_CASE, cd.pos, cdSym, psym)
      }
    }

    final val denyListedSymbols =
      plugin.settings.warts.flatMap { case "println" =>
        definitions.PredefModule.moduleClass.info.member(newTermName("println")).alternatives
      }.toSet

    def checkValidParent(tree: ImplDef): Unit = {
      val isClass = tree.isInstanceOf[ClassDef]
      val tree_symbol = tree.symbol
      val isSelfEvent = isEvent(tree_symbol)
      val isSelfEntity = isEntity(tree_symbol)
      val baseClasses = tree_symbol.info.parents.map(_.typeSymbol)
      for (superClass <- baseClasses if superClass != null && superClass != tree_symbol) {
        lazy val providedImpls = superClass.tpe.members.filter { member =>
          !(member.isAbstract || member.name.decoded.contains(
            "$default$") || (member.owner.isJavaInterface && member.isConstructor) ||
            validEventOrEntityTraits.contains(member.owner))
        }
        if (!validEventOrEntityTraits.contains(superClass) && providedImpls.nonEmpty) {
          if (isSelfEvent && !isEvent(superClass)) {
            alarm(
              OptimusErrors.EVENT_EXTEND_NONEVENT,
              tree.pos,
              tree_symbol.name,
              superClass,
              providedImpls map {
                _.name
              } mkString (", "))
          } else if (isEventContained(tree_symbol) && isEventContained(superClass)) {
            alarm(
              OptimusErrors.CONTAINED_EVENT_EXTEND_CONTAINED_EVENT,
              tree.pos,
              tree_symbol.name,
              superClass,
              providedImpls map {
                _.name
              } mkString (", "))
          } else if (isSelfEntity && !isEntity(superClass)) {
            if (isClass) {
              if (tree_symbol.hasAnnotation(StoredAnnotation))
                alarm(
                  OptimusErrors.ENTITY_STORED_EXTENDS_NONENTITY_STRONG,
                  tree.pos,
                  tree_symbol.name,
                  superClass,
                  providedImpls map { _.name } mkString (", "))
              else {
                val fields = providedImpls.filter(_.isField)
                if (fields.nonEmpty) {
                  alarm(
                    OptimusErrors.ENTITY_EXTEND_NONENTITY_STRONG,
                    tree.pos,
                    tree_symbol.name,
                    superClass,
                    fields map { _.name } mkString (", "))
                }
              }
            } else
              alarm(
                OptimusErrors.ENTITY_OBJECT_EXTEND_NONENTITY_STRONG,
                tree.pos,
                tree_symbol.name,
                superClass,
                providedImpls map { _.name } mkString (", "))
          }
        }
      }
    }

    // TODO (OPTIMUS-13372): remove old check which is less powerful than the checkValidParent one when we can
    def checkNonEntityParent(tree: ImplDef): Unit = {
      val superClass = tree.symbol.superClass
      if ((superClass ne null) && (superClass ne ObjectClass) && !isEntity(superClass)) {
        alarm(OptimusErrors.ENTITY_EXTEND_NONENTITY, tree.pos, tree.symbol.name, superClass)
      }
    }

    def checkStoredEntityObject(tree: ModuleDef): Unit = {
      val treeSym = tree.symbol
      if ((treeSym ne null) && isStoredEntity(treeSym) && !toleratedStoredObjects.contains(treeSym.fullName))
        alarm(OptimusErrors.ENTITY_OBJECT_WITH_STORED, tree.pos, treeSym.fullName)
    }

    private val validEmbeddableClasses: Set[Symbol] =
      Set(ObjectClass, AnyClass, SerializableClass, rootMirror.requiredClass[java.io.Serializable], Enumeration)
    private val validNonOptimusEmbeddableClasses: Set[String] =
      StaticConfig.stringSet("validNonOptimusEmbeddableClasses")

    def checkInheritedVal(impl: Tree): Unit = {
      def isValidNonOptimusClass(cs: Symbol) = validNonOptimusEmbeddableClasses contains cs.fullName

      val sym = impl.symbol

      def isBanned(s: Symbol): Boolean = {
        val isAbstractVal = !s.isPrivate && !s.isSourceMethod && s.isTerm // non-private, abstract val defintions
        def isVariable =
          s.isVariable || (s.enclClass.isTrait && s.hasAllFlags(ACCESSOR | METHOD) && !s.hasFlag(STABLE))

        // only vars are banned on objects; everything is banned on classes
        (sym.isClass && (isAbstractVal || s.isVal)) || isVariable
      }

      if (sym.tpe.typeSymbol.isConcreteClass) {
        // check @embeddable case class/object
        val parents = sym.ancestors
        val inheritedStorableVals =
          parents.filterNot(cs => cs.isModule || validEmbeddableClasses(cs) || isValidNonOptimusClass(cs)).flatMap {
            classSym =>
              val decls = classSym.info.decls
              decls.filter(isBanned) map (valSym => (classSym, valSym))
          }
        if (inheritedStorableVals.nonEmpty) {
          val missedVals = {
            val storableVals =
              if (sym.primaryConstructor.paramss.isEmpty) Nil
              else
                sym.primaryConstructor.paramss.head map (_.decodedName.trim) // case class need to define all inherited val in ctor
            inheritedStorableVals.filterNot { case (parent, p) =>
              (p.isLazy && p.hasAnnotation(definitions.TransientAttr)) ||
              storableVals.contains(p.decodedName.trim)
            }
          }
          missedVals foreach { case (pSym, valSym) =>
            alarm(OptimusErrors.EMBEDDABLE_INHERITED_VAL_OR_VAL, impl.pos, valSym.name, pSym.name)
          }
        }
      }
    }

    // According to OPTIMUS-13852
    // private val with same name with super class in @stored @entity and @event is not allowed
    private def checkNameShadowingOverrideVar(sym: Symbol): Unit = {
      val members = sym.info.decls
      val ancestors = if (sym.hasSelfType) sym.selfType.baseClasses else sym.ancestors
      // this is a bit touchy because we're running after valaccessors, so the vals in question are no longer simple
      // getter-method/private-local-backing-field pairs that we've come to expect but rather an agglomeration of
      // getters and `$impl`s and `$backing`s (q.v. the comment on RedirectAccessorsComponent).
      members foreach {
        case s if s.hasAnnotation(StoredAnnotation) && isValAccessor(s) =>
          // Now s is a val field, after typer, val field always be private, so we need check modifier of its getter method
          if (
            ancestors.exists { base =>
              val o = base.info.decl(s.name.dropLocal)
              (o ne s) && o.alternatives.exists(o =>
                (o.owner ne s.owner) && isValAccessor(o) && (o.flags & PrivateLocal) == PRIVATE)
            }
          )
            alarm(OptimusErrors.IMPLICIT_OVERRIDE_ERROR, s.pos, s.annotationsString)
        case _ => // skip the correct ones
      }
    }

    private def entityEventString(sym: Symbol) = if (isEvent(sym)) "@event" else "@entity"

    private def checkHasDefaultUnpickleable(tree: Tree): Unit = {
      val sym = tree.symbol
      if (sym.isAbstract) {
        val companionType = sym.companionModule.tpe
        if (companionType.typeSymbol.isSubClass(HasDefaultUnpickleableClass)) {
          val tpe = companionType.baseType(HasDefaultUnpickleableClass)
          val args = tpe.typeArgs
          require(args.size == 1)
          if (args.head.typeSymbol != sym)
            alarm(OptimusErrors.EMBEDDABLE_HAS_DEFAULT_UNPICKLEABLE_VALUE, tree.pos)
        }
      }
    }

    private def doCopyableVarianceCheck(ent: Tree, assocs: List[Tree]): Unit =
      if (ent.hasSymbolField && isEntity(ent.symbol)) { // exclude events?
        // assocs is a list of Tuple2.apply("name", value); we care about the names only
        val names = assocs.map { case Apply(_, (lit @ Literal(Constant(name: String))) :: value :: Nil) =>
          (name, lit.pos)
        }
        val entInfo = ent.symbol.info.resultType // sometimes it's an NMT... shrug
        names.foreach { case (name, pos) =>
          validateCopyableVariance(entInfo.member(TermName(name)), pos)
        }
      }

    private def isCollectionType(treeSym: global.Symbol): Boolean = {
      val typeSym = treeSym.tpe.finalResultType.typeSymbol
      typeSym.isSubClass(definitions.TraversableClass) || typeSym.isSubClass(definitions.ArrayClass)
    }

    override def traverse(tree: Tree): Unit = {
      val treeSym = tree.symbol
      containsIteratorUsage ||= (treeSym != null && treeSym.rawname == names.iterator)

      tree match {
        case Apply(s, _) if denyListedSymbols contains s.symbol =>
          alarm(OptimusErrors.BANED_METHOD, tree.pos, treeSym)
        case Apply(s, _) if isNode(s.symbol) && s.symbol.isOnlyRefinementMember =>
          // it's not that we can't, but that we don't want to.
          // besides the fact that it's slow, the node method generation breaks erasure's ApplyDynamic generation
          // for the other (non-node) methods in the refinement type
          alarm(OptimusErrors.REFLECTIVE_NODE_INVOCATION, s.pos, s.symbol.fullLocationString)
        // The use of checking the name as a string here is not nice, but when trying to resolve the actual type, it is not found because it is that type we are processing
        case cd: ImplDef
            if (isEntity(treeSym) && !treeSym.hasAnnotation(EntityAnnotation)) | (isEvent(treeSym) && !(treeSym
              .hasAnnotation(EventAnnotation) || treeSym.tpe.typeSymbol == BusinessEventImplClass)) =>
          val eString = entityEventString(treeSym)
          alarm(
            OptimusErrors.NONSTORABLE_EXTEND_STORABLE,
            cd.pos,
            treeSym,
            eString,
            eString,
            treeSym.tpe.typeSymbol.name.toString,
            "BusinessEventImpl".equals(treeSym.tpe.typeSymbol.name.toString)
          )
        case id: ImplDef if treeSym.hasAnnotation(SoapObjectAnnotation) && !treeSym.hasAnnotation(EntityAnnotation) =>
          alarm(OptimusErrors.SOAP_OBJ_MUST_ENTITY, id.pos)
        case md: ClassDef if isEntityOrEvent(treeSym) && (treeSym isSubClass definitions.DelayedInitClass) =>
          alarm(OptimusErrors.STORABLE_WITH_DELAYED_INIT, md.pos, entityEventString(treeSym))
        case id: ImplDef if isEntity(treeSym) && treeSym.owner.isMethod =>
          alarm(OptimusErrors.METHOD_LOCAL_ENTITY2, id.pos)
        case _: ValDef if treeSym.hasAnnotation(ReifiedAnnotation) =>
          alarm(OptimusErrors.REIFIED_INVALID_CLASS, tree.pos)
        case _: ValOrDefDef | _: ModuleDef | _: ClassDef
            if (treeSym ne null) && treeSym.hasAnnotation(UpcastingTargetAnnotation) =>
          tree match {
            case md: ModuleDef => checkStoredEntityObject(md)
            case _             =>
          }
          if (
            (!(treeSym.hasAnnotation(EntityAnnotation) || treeSym.hasAnnotation(
              EventAnnotation)) || !treeSym.isConcreteClass)
          ) {
            alarm(OptimusErrors.UPCAST_WITH_WRONG_TYPE, tree.pos)
          }
          super.traverse(tree)

        case cd: ClassDef if isEntityOrEvent(treeSym) =>
          checkValidParent(cd)
          if (isEntity(treeSym)) {
            checkNonEntityParent(cd)
            if (treeSym.hasAnnotation(EmbeddableAnnotation)) alarm(OptimusErrors.ENTITY_WITH_EMBEDDABLE, cd.pos)
            checkParamAccessors(cd)
            if (treeSym.hasAnnotation(StoredAnnotation)) {
              // we have specified a schema version so we have some more checks to apply
              // semantically these should all be applied anyway, but
              // it is harder to apply retrospectively
              if (hasExplicitSlotNumber(treeSym)) {
                checkNoInitialisers(cd, List(treeSym))
              }

              checkNameShadowingOverrideVar(treeSym)
            }
            if (
              !treeSym.owner.isModule && !treeSym.owner.isModuleClass && treeSym.hasAnnotation(StoredAnnotation)
              // we allow @stored inside the REPL wrapper classes since they are top level from the user's perspective
              && !nme.isReplWrapperName(treeSym.owner.name)
            )
              alarm(OptimusErrors.INNER_ENTITY_MUST_TRANSIENT, cd.pos, cd.name)
            else {
              checkParents(cd)
              super.traverse(tree)
            }
          } else if (isEvent(treeSym)) {
            if (!treeSym.owner.isPackageClass)
              alarm(OptimusErrors.INNER_EVENT, cd.pos)
            checkNameShadowingOverrideVar(treeSym)
            if (isEventContained(treeSym))
              treeSym.info.members.foreach { m =>
                if (isKeyOrIndex(m))
                  alarm(OptimusErrors.CONTAINED_EVENT_WITH_KEY_INDEX, treeSym.pos)
              }
            super.traverse(tree)
          }
        case md: ModuleDef if isEntityOrEvent(treeSym) =>
          if (isEntity(treeSym)) {
            checkStoredEntityObject(md)
            checkNonEntityParent(md)
          }
          checkValidParent(md)
          checkParents(md)
          if (treeSym.hasAnnotation(StoredAnnotation))
            checkNameShadowingOverrideVar(treeSym)
          super.traverse(tree)
        // ignore the new operators inside optimus auto generated creation methods: apply, uniqueInstance
        case dd: DefDef if treeSym.hasAnnotation(AutoGenCreationAnnotation) =>
          atAutoGenCreationMethods(true) {
            super.traverse(tree)
          }
        case _: ClassDef | _: ModuleDef if treeSym.hasAnnotation(EmbeddableAnnotation) =>
          checkInheritedVal(tree)
          checkHasDefaultUnpickleable(tree)
          super.traverse(tree)
        case _: ValOrDefDef if (treeSym ne null) && treeSym.hasAnnotation(EventAnnotation) =>
          alarm(OptimusErrors.EVENT_WITH_WRONG_TYPE, tree.pos)
        case _: ValOrDefDef if (treeSym ne null) && treeSym.hasAnnotation(EntityAnnotation) =>
          alarm(OptimusErrors.ENTITY_WITH_WRONG_TYPE, tree.pos)
        case _: ValOrDefDef if (treeSym ne null) && treeSym.hasAnnotation(EmbeddableAnnotation) =>
          alarm(OptimusErrors.EMBEDDABLE_WITH_WRONG_TYPE, tree.pos)
        case _: ValOrDefDef
            if (treeSym ne null) && treeSym.hasAnnotation(IndexedAnnotation) &&
              !(treeSym.asMethod.returnType =:= definitions.NothingTpe) &&
              treeSym.asMethod.returnType.baseClasses.exists { bc =>
                bc == definitions.IterableClass ||
                bc == definitions.ArrayClass
              } &&
              treeSym.asMethod.returnType.typeArgs.exists { ta =>
                ta.baseClasses.exists { bc =>
                  bc == definitions.IterableClass || bc == definitions.ArrayClass
                }
              } =>
          alarm(OptimusErrors.INDEX_ON_NESTED_COLLECTION, tree.pos)
        case _: ValOrDefDef
            if (treeSym ne null) && hasUniqueOption(treeSym.getAnnotation(IndexedAnnotation)).getOrElse(
              false) && isCollectionType(treeSym) =>
          alarm(OptimusErrors.NO_UNIQUE_INDEXED_COLLECTION, tree.pos, treeSym.name)

        case _: ValOrDefDef if (treeSym ne null) && treeSym.hasAnnotation(StoredAnnotation) && ! {
              val owner = treeSym.owner
              (owner.hasAnnotation(StoredAnnotation) && owner.hasAnnotation(EntityAnnotation)) ||
              owner.hasAnnotation(EventAnnotation)
            } && !treeSym.isSynthetic => // skip valaccessors artifacts
          alarm(OptimusErrors.STORED_MEMBER_ON_NON_STORED_OWNER, tree.pos)
        case _: DefDef
            if treeSym.isLocalToBlock && (treeSym.hasAnnotation(NodeAnnotation) || treeSym.hasAnnotation(
              AsyncAnnotation)) =>
          alarm(
            if (treeSym hasAnnotation NodeAnnotation) OptimusErrors.LOCAL_NODE else OptimusErrors.LOCAL_ASYNC,
            tree.pos)
        case _: DefDef
            if (hasXFuncAnnotation) && !treeSym.hasAnnotation(NodeAnnotation) && treeSym
              .getAnnotation(xFuncAnnotation)
              .exists { case xf =>
                xf.javaArgs.get(names.xFuncAnnotationFieldNameAutoRefresh) match {
                  case None                                   => false
                  case Some(LiteralAnnotArg(Constant(false))) => false
                  case Some(LiteralAnnotArg(Constant(true)))  => true
                  case Some(x) =>
                    global.reporter.error(tree.pos, s"unexpected $x ")
                    false
                }
              } =>
          alarm(OptimusErrors.XFUNC_AUTO_REFRESH_ON_NON_NODE, tree.pos)
        // TODO (OPTIMUS-12546): ban @handle @xFunc
        /* commented out until we confirm the transition path
     case _: DefDef if (xFuncAnnotation != NoSymbol) && treeSym.hasAnnotation(handleAnnotation) && treeSym.hasAnnotation(xFuncAnnotation) =>
       alarm(OptimusErrors.XFUNC_WITH_HANDLE, tree.pos)
         */
        case _: DefDef if treeSym.hasAnnotation(NodeSyncAnnotation) && {
              val clazz = treeSym.enclClass
              clazz.isDerivedValueClass || (clazz.isTrait && clazz.info.parents.nonEmpty && clazz.info.firstParent.typeSymbol == AnyClass)
            } =>
          alarm(OptimusErrors.NOT_SUPPORTED_NODE, tree.pos)
        case _: ValOrDefDef if treeSym.hasAnnotation(KeyAnnotation) =>
          val klass = treeSym.enclClass
          if (klass != treeSym.owner)
            alarm(OptimusErrors.UNKNOWN_KEY_USAGE, tree.pos)
          val existingOpt = klass.parentSymbols.flatMap(_.info.members.find(_.hasAnnotation(KeyAnnotation)))
          existingOpt foreach { existing =>
            val name = existing.name
            val owner = existing.owner.name
            alarm(OptimusErrors.ENTITY_WITH_MULTI_DEFAULT_KEY, tree.pos, name, owner)
          }
          tree match {
            case dd: DefDef =>
              checkDefDef(dd)
              super.traverse(tree)
            case _ =>
              super.traverse(tree)
          }

        // PropertyInfo synthetic objects. Note that we *don't* run checkDefDef() for these!
        case vdd: ValOrDefDef
            if treeSym.isSynthetic &&
              treeSym.owner.isModuleClass &&
              treeSym.tpe.typeSymbol.isNonBottomSubClass(PropertyInfoBase) =>
          val overriden = treeSym.nextOverriddenSymbol
          if (overriden != NoSymbol) {
            alarm(
              OptimusErrors.ENTITY_PROPERTY_INFO_ACCIDENTAL_OVERRIDE,
              tree.pos,
              treeSym.owner,
              treeSym.nextOverriddenSymbol.owner,
              treeSym
            )
          }
          super.traverse(tree)

        case dd: DefDef =>
          checkDefDef(dd)
          super.traverse(tree)

        case Apply(fun, args) if isTweakCtor(fun) && args.nonEmpty && badTweakBodySyms.contains(args.head.symbol) =>
          alarm(OptimusNonErrorMessages.ILLEGAL_TWEAK_BODY, args.head.pos, args.head)

        case Apply(fun, args) =>
          // looking for something that was once a call like
          //   optimus.platform.package.EntityCopyOps[E](<ent>).copyUnique.applyDynamicNamed("apply")(<assocs>)
          tree.attachments.get(MacroExpansionAttachment).foreach {
            case analyzer.MacroExpansionAttachment(expandee, _) if expandee.symbol.owner == CopyUniqueCls =>
              expandee match {
                case Apply(
                      Apply(Select(Select(Apply(entityCopyOps, ent :: Nil), copyUnique), applyDynamicNamed), apply),
                      assocs) =>
                  doCopyableVarianceCheck(ent, assocs)
                case _ =>
                  internalErrorAbort(
                    tree.pos,
                    s"unexpected shape of copyUnique expandee:\n$expandee\n${printRaw(expandee)}")
              }
            case _ =>
          }
          super.traverse(tree)

        case ret: Return if isNode(ret.symbol) =>
          alarm(OptimusErrors.RETURN_IN_NODE, ret.pos, ret.symbol)
          super.traverse(ret)
        case Block(stats, expr) =>
          // looking for something that was once a call like
          //   optimus.platform.DAL.modify(<ent>).applyDynamicNamed("apply")(<assocs>)
          tree.attachments.get(MacroExpansionAttachment).foreach {
            case analyzer.MacroExpansionAttachment(expandee, _) if expandee.symbol.owner == DALModifyMod.moduleClass =>
              expandee match {
                case Apply(Apply(Select(Apply(dalModify, ent :: Nil), applyDynamicNamed), apply), assocs) =>
                  doCopyableVarianceCheck(ent, assocs)
                case _ =>
                  internalErrorAbort(
                    tree.pos,
                    s"unexpected shape of DAL.modify expandee:\n$expandee\n${printRaw(expandee)}")
              }
            case _ =>
          }
          super.traverse(tree)

        // TODO (OPTIMUS-13336): Move the type check inside the isEntity method, so Nothing is not Entity (currently it will return true)
        case Select(New(tpt), nme.CONSTRUCTOR)
            if !inAutoGenCreationMethods && tpt.tpe != definitions.NothingTpe && isEntityOrEvent(tpt.tpe.typeSymbol) =>
          val methodName = tpt.tpe match {
            case TypeRef(pre, sym, args) =>
              val typeParameters = if (args.size > 0) s"[${args.mkString(",")}]" else ""
              s"${sym.nameString}.${names.uniqueInstance}${typeParameters}"
            case other => s"${tpt}.${names.uniqueInstance}"
          }
          if (isEntity(tpt.tpe.typeSymbol))
            alarm(OptimusErrors.NEW_ENTITY, tree.pos, methodName)
          else if (isEvent(tpt.tpe.typeSymbol))
            alarm(OptimusErrors.NEW_EVENT, tree.pos, methodName)
          super.traverse(tree)

        case _ =>
          super.traverse(tree)
      }
    }
  }

  def checkNoInitialisers(rootCd: ClassDef, path: List[Symbol]): Unit = {
    val classSym = path.head
    for (member <- classSym.info.members if member.nameString endsWith suffixes.INITIALIZER) {
      val valName = member.nameString.substring(0, member.nameString.length - suffixes.INITIALIZER.length)
      val pathString =
        if (path.length <= 1) ""
        else {
          path map { _.tpe } mkString ("", " extends ", "")
        }
      alarm(OptimusErrors.USE_TRANSFORMER, rootCd.pos, classSym.nameString, valName, pathString)
    }
    classSym.info.parents foreach { p =>
      checkNoInitialisers(rootCd, p.typeSymbol :: path)
    }
  }

  private def hasExplicitSlotNumber(treeSym: Symbol): Boolean = {
    val metaData = treeSym.getAnnotation(EntityMetaDataAnnotation).getOrElse {
      internalErrorAbort(treeSym.pos, s"Optimus Internal Error: @EntityMetaDataAnnotation not found!")
    }
    val explictSlotNumber = entityMetaDataAnnotationNames.explicitSlotNumber
    metaData.assocs
      .collectFirst { case (`explictSlotNumber`, LiteralAnnotArg(c)) => c.booleanValue }
      .getOrElse(false)
  }

  val MacroExpansionAttachment = reflect.classTag[analyzer.MacroExpansionAttachment]
}
