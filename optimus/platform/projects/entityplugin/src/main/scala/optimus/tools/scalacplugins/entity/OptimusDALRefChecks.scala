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

import scala.annotation.tailrec
import scala.tools.nsc._

object OptimusDALRefChecks

class OptimusDALRefChecks(val plugin: EntityPlugin, val phaseInfo: OptimusPhaseInfo)
    extends EntityPluginComponent
    with TypedUtils
    with WithOptimusPhase {
  import global._

  override def newPhase(prev: Phase): Phase = new EntityPluginStdPhase(prev) {
    override def apply0(unit: CompilationUnit): Unit = {
      val refchecks = new OptimusDALRefChecks(unit)
      refchecks.traverse(unit.body)
    }
  }

  class OptimusDALRefChecks(unit: CompilationUnit) extends Traverser {
    import global._
    import definitions._

    val MapClass = rootMirror.getRequiredClass("scala.collection.immutable.Map")

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

    def checkAccDefDef(dd: DefDef) = {
      val defSym = dd.symbol
      if (defSym.hasAnnotation(AccelerateInfoAnnotation)) {
        dd.rhs match {
          case sel @ Select(qual, name) =>
            val isEmbeddablePath = checkPathIsEmbeddable(defSym.owner, sel)
            if (!isEmbeddablePath)
              alarm(OptimusErrors.PROJECTED_DEF_INVALID_RHS, dd.pos)
            else if (!sel.symbol.isParamAccessor && !isEntityOrEvent(sel.symbol.owner))
              alarm(OptimusErrors.PROJECTED_DEF_MUST_REFER_CTOR_PARAM, dd.pos)
            else {
              val hasTweak = sel.exists(_ match {
                case s @ Select(This(_), _) => s.symbol.hasAnnotation(TweakableAnnotation)
                case _                      => false
              })
              if (hasTweak)
                alarm(OptimusErrors.PROJECTED_DEF_MUST_REFER_NONTWEAKABLE, dd.pos)
            }
          case Apply(fun, args) if isKeyOrIndex(defSym) =>
            args foreach {
              case sel @ Select(_: This | _: Super, x) =>
              case tree =>
                alarm(OptimusErrors.PROJECTED_DEF_INVALID_RHS, tree.pos)
            }
          case _ =>
            alarm(OptimusErrors.PROJECTED_DEF_INVALID_RHS, dd.pos)
        }
      }
    }

    val accSupportedTypeSet: Set[Symbol] = Set(
      definitions.DoubleClass,
      definitions.FloatClass,
      definitions.IntClass,
      definitions.ShortClass,
      definitions.LongClass,
      definitions.BooleanClass,
      typeOf[BigDecimal].typeSymbol,
      typeOf[String].typeSymbol,
      rootMirror.getRequiredClass("java.time.ZonedDateTime").asType,
      rootMirror.getRequiredClass("java.time.LocalDate").asType,
      rootMirror.getRequiredClass("java.time.ZoneId").asType,
      rootMirror.getRequiredClass("java.time.Period").asType,
      rootMirror.getRequiredClass("java.time.Instant").asType,
      rootMirror.getRequiredClass("java.time.Duration").asType,
      rootMirror.getRequiredClass("java.time.YearMonth").asType
    )

    /**
     * property whose type should not be collection and which is stored without tweakable or nonstored but with key or
     * index could be projected
     */
    private def checkAccelerated(symbol: Symbol, isStored: Boolean, alarmNonStored: => Unit): Boolean = {
      def getUnderlyingType(tpe: Type): Type =
        if (tpe.typeSymbol isSubClass definitions.OptionClass) getUnderlyingType(tpe.typeArgs.head) else tpe

      def isSupportedIndex(tpe: Type): Boolean = {
        val underType = getUnderlyingType(tpe)
        if (underType.typeSymbol isSubClass MapClass) false
        else if (
          underType.baseClasses.exists(_.fullName == "optimus.platform.ImmutableArray") && underType.typeArgs.forall(
            tp => tp.typeSymbol == definitions.ByteClass)
        ) true // we allow @indexed for ImmutableArray[Byte], so we also allow this for @projected(indexed = true)
        else if (underType.typeSymbol isSubClass definitions.TraversableClass) {
          underType.typeArgs.forall(tp => {
            accSupportedTypeSet.contains(tp.typeSymbol) || tp.typeSymbol.hasAnnotation(EntityMetaDataAnnotation)
          })
        } else true
      }

      def isCompoundIndex(): Boolean = {
        val accInfoTree = symbol.getAnnotation(AccelerateInfoAnnotation).get.original
        accInfoTree match {
          case Apply(_, args) =>
            args.exists {
              case NamedArgTree(Ident(accelerateInfoAnnotationNames.path), Literal(path)) =>
                path.stringValue.isEmpty
              case _ => false
            }
          case _ => false
        }
      }

      def isProjectIndexed(): Boolean = {
        val projectedTree = symbol.getAnnotation(ProjectedAnnotation).get.original
        projectedTree match {
          case Apply(_, args) =>
            args.exists {
              case NamedArgTree(Ident(n: TermName), Literal(indexed)) if (names.indexed == n) =>
                indexed.booleanValue
              case _ => false
            }
          case _ => false
        }
      }

      def isNestedOptionKnowable(tpe: Type): Boolean = {
        if (tpe.typeSymbol.isSubClass(definitions.OptionClass) || (tpe.typeSymbol eq KnowableClass)) {
          val arg = tpe.typeArgs.head
          arg.typeSymbol.isSubClass(definitions.OptionClass) || arg.typeSymbol.isSubClass(KnowableClass)
        } else false
      }

      val accelerated = symbol.hasAnnotation(ProjectedAnnotation)
      if (accelerated) {
        val typeToCheck = if (symbol.isMethod) symbol.asMethod.returnType else symbol.asTerm.tpe
        typeToCheck.normalize match {
          case t if isNestedOptionKnowable(t) =>
            alarm(OptimusErrors.PROJECTED_FIELD_UNSUPPORTED, symbol.pos)

          // Don't support create DB index on @stored(childToParent=true), ImmutableArray[Byte], Seq[Byte], Map[_, _],
          // and Seq[Seq[_]], as well as Option[_] with above type
          // For Collection type index, we only support Collection of primitive type, String, DateTime and Entity.
          case t if isProjectIndexed() && (symbol.hasAnnotation(C2PAnnotation) || !isSupportedIndex(t)) =>
            alarm(OptimusErrors.PROJECTED_INDEXED_UNSUPPORTED_PROPERTY, symbol.pos)
          case t =>
            if (
              !isStored && !symbol.hasAnnotation(IndexedAnnotation) && !symbol.hasAnnotation(KeyAnnotation)
              && !symbol.hasAnnotation(AccelerateInfoAnnotation)
            ) {
              alarmNonStored
            } else if (!isStored && symbol.hasAnnotation(AccelerateInfoAnnotation) && isCompoundIndex()) {
              if (t.typeArgs.exists(isNestedOptionKnowable _))
                alarm(OptimusErrors.PROJECTED_FIELD_UNSUPPORTED, symbol.pos)
            }
        }
      }
      accelerated
    }

    /**
     * we will do some check to guarantee user follow rules of how to use projected feature. We do this check at compile
     * time since we want to find the violation as early as possible. But we still leave some rules checked at class
     * registration phase and query phase since those rules are complex or are related to type which cannot get at
     * AdjustAST phase.
     *
     * rules to check: 1 projected entity/event must have @projected fields, vice versa 2 projected event must be final.
     * 3 projected entity must be trait/concrete class. Its slot number must be 0. 4 only collection of allowed types
     * (primitive, string, etc) can be marked as @projected(indexed=true)
     */
    private def checkProjectedAnnotationForEntity(treeSym: Symbol, isStored: Boolean): Unit = {
      val projected = isEntityProjected(treeSym)
      if (projected) {
        val anno = treeSym.getAnnotation(EntityMetaDataAnnotation).get
        val slotNumber = anno.assocs.toMap.get(entityMetaDataAnnotationNames.slotNumber) match {
          case Some(LiteralAnnotArg(l)) => l.intValue
          case _                        => 0
        }
        if (!isStored)
          alarm(OptimusErrors.PROJECTED_NOTSTORED_ENTITY_EVENT, treeSym.pos)
        if (!treeSym.isTrait && !treeSym.isConcreteClass)
          alarm(OptimusErrors.PROJECTED_INVALID_ENTITY, treeSym.pos)
        if (slotNumber != 0)
          alarm(OptimusErrors.PROJECTED_SLOT_MISSMATCH, treeSym.pos)
      }

      checkProjectedAnnotationForStorable(projected, treeSym)
    }

    private def checkProjectedAnnotationForEvent(treeSym: Symbol): Unit = {
      val projected = isEventProjected(treeSym)
      if (projected && !treeSym.isFinal) // we do not support inheritance for event yet
        alarm(OptimusErrors.PROJECTED_NOTFINAL_EVENT, treeSym.pos)
      checkProjectedAnnotationForStorable(projected, treeSym)
    }

    private def checkProjectedAnnotationForStorable(isProjected: Boolean, treeSym: Symbol): Unit = {
      val storedProperties = treeSym.info.decls.filter(_.hasAnnotation(StoredAnnotation)).map(_.decodedName.trim).toSet
      val projectedProperties = treeSym.info.decls.collect {
        case s
            if !s.isSynthetic && checkAccelerated(
              s,
              storedProperties.contains(s.decodedName.trim),
              alarm(OptimusErrors.PROJECTED_NONSTORED_PROPERTY, s.pos)) =>
          s
      }

      if (projectedProperties.isEmpty && isProjected)
        alarm(OptimusErrors.PROJECTED_ANNOTATION_MISSING, treeSym.pos)
      else if (projectedProperties.nonEmpty && !isProjected)
        alarm(OptimusErrors.PROJECTED_NOTSET, treeSym.pos)
    }

    private def checkProjectedAnnotationForEmbeddable(treeSym: Symbol): Unit = {
      val projected = isEmbeddableProjected(treeSym)
      if (projected && (!treeSym.isCaseClass || !treeSym.isFinal))
        alarm(OptimusErrors.PROJECTED_INVALID_EMBEDDABLE, treeSym.pos)

      val ctorArgs = treeSym.info.decls
        .collectFirst {
          case ctor: MethodSymbol if ctor.isPrimaryConstructor => ctor.paramLists.flatten.map(_.decodedName.trim).toSet
        }
        .getOrElse(Set.empty)
      val projectedProperties = treeSym.info.decls.collect {
        case s
            if !s.isSynthetic && checkAccelerated(
              s,
              ctorArgs.contains(s.decodedName.trim),
              alarm(OptimusErrors.PROJECTED_NONCTOR_PROPERTY, s.pos)) =>
          s
      }

      if (projectedProperties.nonEmpty && !projected)
        alarm(OptimusErrors.PROJECTED_NOTSET, treeSym.pos)
    }

    private def checkMonoTemporalAnnotationForEntity(treeSym: Symbol, isStored: Boolean): Unit = {
      val monoTemporal = isEntityMonoTemporal(treeSym)
      if (monoTemporal) {
        if (!isStored)
          alarm(OptimusErrors.INVALID_MONOTEMPORAL_NOTSTORED_ENTITY, treeSym.pos)

        // bi-temporal base cannot have @indexed(unique=true)
        val bitemporalBaseClasses = treeSym.baseClasses.filter(e => isStoredEntity(e) && !isEntityMonoTemporal(e))
        for (baseSym <- bitemporalBaseClasses) {
          val uniqueIndexes =
            baseSym.info.decls.filter(s => !s.isSynthetic && isUniqueIndex(s)).map(_.decodedName.trim).toSet
          if (uniqueIndexes.nonEmpty)
            alarm(
              OptimusErrors.INVALID_MONOTEMPORAL_INHERIT_BITEMPORAL_WITH_UNIQUE_INDEX,
              treeSym.pos,
              uniqueIndexes.mkString("'", "', '", "'"),
              baseSym.fullName)
        }
      } else if (isStored) {
        // bi-temporal entity cannot have mono-temporal base types
        val monoTemporalBaseClasses =
          treeSym.baseClasses.filter(e => isStoredEntity(e) && isEntityMonoTemporal(e)).map(_.fullName)
        if (monoTemporalBaseClasses.nonEmpty)
          alarm(
            OptimusErrors.INVALID_BITEMPORAL_INHERIT_MONOTEMPORAL,
            treeSym.pos,
            monoTemporalBaseClasses.sorted.mkString(", "))
      }
    }

    /**
     * we will do some checks to guarantee user follow rules of how to use fullTextSearch feature. We do this check at
     * compile time since we want to find the violation as early as possible.
     *
     * rules to check: 1 fullTextSearch annotated entity must have @fullTextSearch fields, vice versa. 2 fullTextSearch
     * entity must be trait/concrete class. 3 Type check
     */
    private def checkFullTextSearchAnnotationEntity(treeSym: Symbol, isStored: Boolean): Unit = {
      val fts = isEntityFullTextSearchable(treeSym)
      if (fts) {
        if (!isStored)
          alarm(
            OptimusErrors.FULLTEXTSEARCH_INVALID_USAGE,
            treeSym.pos,
            "Are you missing @entity @stored(fullTextSearch=true) on the class?")
        if (!treeSym.isTrait && !treeSym.isConcreteClass)
          alarm(
            OptimusErrors.FULLTEXTSEARCH_INVALID_USAGE,
            treeSym.pos,
            "can only be used with a concrete class or a trait")
      }

      checkFullTextSearchAnnotationValidity(fts, treeSym)
    }

    private def checkFullTextSearchAnnotationValidity(isFts: Boolean, treeSym: Symbol): Unit = {
      val ftsProperties = treeSym.info.decls.collect {
        case s
            if isFts && !s.hasAnnotation(ValAccessorAnnotation) && s
              .hasAnnotation(FullTextSearchAnnotation) && s.isMethod =>
          if (!treeSym.isAbstract) // to get around evaluating $init$
            alarm(
              OptimusErrors.FULLTEXTSEARCH_INVALID_USAGE,
              treeSym.pos,
              "def is not currently supported, please use val only")
        case s if isFts && s.hasAnnotation(IndexedAnnotation) && s.hasAnnotation(FullTextSearchAnnotation) =>
          alarm(OptimusErrors.FULLTEXTSEARCH_INVALID_USAGE, treeSym.pos, "do not use @indexed on the same field")
        case s if isFts && s.hasAnnotation(ProjectedAnnotation) =>
          alarm(OptimusErrors.FULLTEXTSEARCH_INVALID_USAGE, treeSym.pos, "do not use @projected on any fields")
        case s if !s.isSynthetic && checkFullTextSearchType(s) => s
      }
      if ((ftsProperties.isEmpty && isFts) || (ftsProperties.nonEmpty && !isFts))
        alarm(
          OptimusErrors.FULLTEXTSEARCH_INVALID_USAGE,
          treeSym.pos,
          "if @entity @stored(fullTextSearch=true) defined, there must be at least one @fullTextSearch property (vv)"
        )

    }

    private def checkFullTextSearchType(symbol: Symbol): Boolean = {

      def isNestedOptionKnowable(tpe: Type): Boolean = {
        if (tpe.typeSymbol.isSubClass(definitions.OptionClass) || (tpe.typeSymbol eq KnowableClass)) {
          val arg = tpe.typeArgs.head
          arg.typeSymbol.isSubClass(definitions.OptionClass) || arg.typeSymbol.isSubClass(KnowableClass)
        } else false
      }

      val supportedTypes = Set(
        definitions.StringClass,
        definitions.DoubleClass,
        definitions.FloatClass,
        definitions.IntClass,
        definitions.ShortClass,
        definitions.LongClass,
        typeOf[BigDecimal].typeSymbol,
        typeOf[String].typeSymbol,
        rootMirror.getRequiredClass("java.time.ZonedDateTime").asType
      )

      def isSupportedColl(tpe: Type) =
        tpe.typeSymbol.isSubClass(definitions.ListClass) || tpe.typeSymbol.isSubClass(
          definitions.SeqClass) || tpe.typeSymbol.isSubClass(definitions.ArrayClass) || tpe.typeSymbol.isSubClass(
          definitions.IterableClass)

      def isTypeSupported(tpe: Type): Boolean = {
        if (isSupportedColl(tpe)) {
          val collArg = tpe.typeArgs.head
          supportedTypes.contains(collArg.typeSymbol)
        } else if (tpe.typeSymbol.isSubClass(definitions.OptionClass)) {
          val optArg = tpe.typeArgs.head
          if (isSupportedColl(optArg)) {
            val collArg = optArg.typeArgs.head
            supportedTypes.contains(collArg.typeSymbol)
          } else supportedTypes.contains(optArg.typeSymbol)
        } else supportedTypes.contains(tpe.typeSymbol)
      }

      val fts = symbol.hasAnnotation(FullTextSearchAnnotation)
      if (fts) {
        val typeToCheck = if (symbol.isMethod) symbol.asMethod.returnType else symbol.asTerm.tpe
        typeToCheck.normalize match {
          case t if isNestedOptionKnowable(t) =>
            alarm(
              OptimusErrors.FULLTEXTSEARCH_INVALID_USAGE,
              symbol.pos,
              "fields can't be nested Option/Knowable type(e.g. Option[Option[_]] or Knowable[Option[_]])")
          case t if isTypeSupported(t) => // do nothing
          case _ =>
            alarm(
              OptimusErrors.FULLTEXTSEARCH_INVALID_USAGE,
              symbol.pos,
              "Only (Option of) String/String collections/ZonedDateTime supported currently")
        }
      }
      fts
    }

    override def traverse(tree: Tree) = {
      val treeSym = tree.symbol

      tree match {
        case cd: ClassDef if isEntityOrEvent(treeSym) =>
          if (isEntity(treeSym)) {
            checkFullTextSearchAnnotationEntity(treeSym, treeSym.hasAnnotation(StoredAnnotation))
            checkProjectedAnnotationForEntity(treeSym, treeSym.hasAnnotation(StoredAnnotation))
            checkMonoTemporalAnnotationForEntity(treeSym, treeSym.hasAnnotation(StoredAnnotation))
          }
          if (isEvent(treeSym)) {
            checkProjectedAnnotationForEvent(treeSym)
            if (isFullTextSearch(cd.symbol))
              alarm(OptimusErrors.FULLTEXTSEARCH_INVALID_USAGE, cd.pos, "cannot be used with @event")
          }
          super.traverse(tree)
        case _: ValOrDefDef if treeSym.hasAnnotation(FullTextSearchAnnotation) => {
          checkFullTextSearchType(treeSym)
          if (
            !treeSym.owner.isMethod && !(treeSym.owner.isClass && !treeSym.owner.isModuleClass && isEntityOrEvent(
              treeSym.owner))
          )
            alarm(OptimusErrors.FULLTEXTSEARCH_INVALID_USAGE, tree.pos, "cannot be used with @event")
          super.traverse(tree)
        }
        case _: ValOrDefDef
            if treeSym.hasAnnotation(ProjectedAnnotation) && !treeSym.owner.isMethod
              && !(treeSym.owner.isClass && !treeSym.owner.isModuleClass && (isEntityOrEvent(
                treeSym.owner) || isEmbeddable(treeSym.owner))) =>
          alarm(OptimusErrors.PROJECTED_INVALID_CLASS, tree.pos)
        case _: ValOrDefDef | _: ModuleDef | _: ClassDef
            if (treeSym ne null) && treeSym.hasAnnotation(UpcastingTargetAnnotation) =>
          if (
            !(!(treeSym.hasAnnotation(EntityAnnotation) || treeSym.hasAnnotation(
              EventAnnotation)) || !treeSym.isConcreteClass)
          ) {
            checkProjectedAnnotationForEntity(treeSym, treeSym.hasAnnotation(StoredAnnotation))
          }
          super.traverse(tree)
        case md: ModuleDef if isEntity(treeSym) =>
          if (isEntityProjected(treeSym))
            alarm(OptimusErrors.PROJECTED_INVALID_CLASS, tree.pos)
          if (isEntityMonoTemporal(treeSym))
            alarm(OptimusErrors.INVALID_MONOTEMPORAL_OBJECT, tree.pos)
          super.traverse(tree)
        case _: ClassDef | _: ModuleDef if treeSym.hasAnnotation(EmbeddableAnnotation) =>
          checkProjectedAnnotationForEmbeddable(treeSym)
          super.traverse(tree)
        case dd: DefDef =>
          checkAccDefDef(dd)
          super.traverse(tree)
        case _ =>
          super.traverse(tree)
      }
    }
  }
}
