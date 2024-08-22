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

import scala.tools.nsc._
import scala.tools.nsc.transform._
import optimus.tools.scalacplugins.entity.reporter.OptimusErrors
import optimus.tools.scalacplugins.entity.reporter.OptimusNonErrorMessages
import optimus.tools.scalacplugins.entity.staged.scalaVersionRange

import scala.collection.mutable.ListBuffer
import scala.reflect.internal.Flags
import scala.PartialFunction.cond

/**
 * This (slightly yet tersely misnamed) phase creates EntityInfos and EventInfos for entities and events, respectively.
 *
 *   - Every module which is the companion of an event or entity class gets val `info ` = new {Entity,Event}Info(...)
 *     def info = `info `
 *   - Every module which is itself an entity also gets val `$info ` = new {Entity,Event}Info(...) def $info = `$info `
 *   - Every event or entity class gets val $info = companion.info
 *
 * Additionally, for every property that overrides a superclass property, calls to PropertyInfo#matchOn_= are inserted,
 * linking subclass properties with their overridden properties.
 */
class EntityInfoComponent(val plugin: EntityPlugin, val phaseInfo: OptimusPhaseInfo)
    extends EntityPluginComponent
    with InfoTransform
    with TypingTransformers
    with TransformGrep
    with TypedUtils
    with ast.TreeDSL
    with WithOptimusPhase {
  import global._, Flags._

  def newTransformer0(unit: CompilationUnit) = new EntityInfoTransformer(unit)

  case class OriginalOwner(sym: Symbol)

  case class AllPossibleParents(head: Symbol, overrides: List[Symbol], otherSources: List[Symbol])

  private def shouldRewriteParents(sym: Symbol) = {
    val superClass = sym.superClass
    // TODO (OPTIMUS-13297): if we move the BusinessEventImpl class to core project, we can remove the type check here
    !sym.isTrait && isEntityOrEvent(sym) && (superClass.isTrait || superClass == definitions.ObjectClass)
  }

  def rewriteParents(sym: Symbol, parents: List[Type]): List[Type] = {
    def rewrite(sc: Symbol): List[Type] = sc.tpe :: parents.tail
    if (shouldRewriteParents(sym)) {
      if (isEntity(sym)) rewrite(EntityImplClass)
      else if (isEvent(sym)) rewrite(BusinessEventImplClass)
      else parents
    } else parents
  }

  def needsEntityInfos(sym: Symbol): Boolean = isUserDefinedEntity(sym) || isEvent(sym) || isStorableCompanion(sym)

  val infoFlags: Long = SYNTHETIC | PRIVATE | LOCAL

  def isMissingName(decls: Scope, name: TermName, info: Type): Boolean = {
    val entry = decls.lookupEntry(name)
    (entry eq null) || !(entry.sym.tpe.resultType =:= info)
  }

  // Starting at the top-level module recurse through symbol info decls trying to find all nested entities
  // For each entity found enter an entity info sym into the new top-level scope, and put an attachment on
  // the symbol to say which entity it belongs to
  def enterNestedInfos(
      topLevelSym: Symbol,
      decls: Scope,
      sym: Symbol,
      prefix: String = "",
      shouldRecurse: Boolean = true): Unit = {
    def join(prefix: String, name: Name): String = prefix + (if (prefix.isEmpty) "" else "_") + name

    def recurse(sym: Symbol): Unit =
      // we don't recurse into REPL Wrappers here because we visit them as if they are effectively top level in #transformInfo
      if (shouldRecurse && !nme.isReplWrapperName(sym.name))
        sym.info.decls.foreach(s => enterNestedInfos(topLevelSym, decls, s, join(prefix, s.name)))

    def infoSym(name: TermName, info: Type, originalOwner: OriginalOwner): Symbol = {
      val infoSym =
        topLevelSym.newValue(name.localName, sym.pos, infoFlags).setInfo(info).updateAttachment(originalOwner)
      if (!isEntity(topLevelSym)) infoSym.addAnnotation(definitions.TransientAttr)
      infoSym
    }

    if (sym.isModuleOrModuleClass) {
      if (isStorableCompanion(sym)) {
        decls.enter {
          infoSym(newTermName(join(prefix, names.info)), infoTypeForClass(sym), OriginalOwner(sym.companionClass))
        }
      }
      if (isEntityOrEvent(sym)) {
        decls.enter {
          infoSym(newTermName(join(prefix, names.$info)), infoTypeForModule(sym), OriginalOwner(sym))
        }
      }
      recurse(sym)
    } else if (sym.isClass && !sym.isPackageClass) {
      recurse(sym)
    }
  }

  def infoTypeForClass(sym: Symbol): Type = (if (isEvent(sym.companionClass)) EventInfoCls else ClassEntityInfoCls).tpe
  // $info in a class can be overridden in a module; can't refine farther than EntityInfo
  def infoTypeForModule(sym: Symbol): Type = (if (isEvent(sym.companionClass)) EventInfoCls else EntityInfo).tpe

  def enterInfoDefsForSym(decls: Scope, sym: Symbol): Unit = {
    if (isStorableCompanion(sym)) {
      val desiredType = infoTypeForClass(sym)
      // do not add if it already exists (ie: was defined manually)
      if (isMissingName(decls, names.info, desiredType)) {
        decls.enter {
          sym
            .newMethod(names.info, sym.pos, SYNTHETIC | STABLE | ACCESSOR)
            .setInfo(NullaryMethodType(desiredType))
        }
      }
    }
    if (isEntityOrEvent(sym)) decls.enter {
      sym.newMethod(names.$info, sym.pos, SYNTHETIC | OVERRIDE).setInfo(NullaryMethodType(infoTypeForModule(sym)))
    }
  }

  // [1] If possible we rewrite symbols which extend Entity to extend EntityImpl (and symbols which extend
  // BusinessEvent to extends BusinessEventImpl) because subclassing is more efficient than implementing a trait at
  // the JVM level
  override def transformInfo(sym: Symbol, tpe: Type): Type = {
    if (sym.isClass && !sym.isPackageClass && !sym.isBottomClass) {
      tpe match {
        case PolyType(tparams, res) =>
          val transformed = transformInfo(sym, res)
          if (transformed eq res) tpe
          else PolyType(tparams, transformed)

        // We generate entity info always on a top level module for even for nested classes (so that we have just one
        // entity info, rather than one per outer class instance)
        case ClassInfoType(parents0, decls0, _) if sym.isModuleClass && isEffectivelyTopLevel(sym) =>
          val decls1 = decls0.cloneScope
          enterNestedInfos(sym, decls1, sym)
          enterNestedInfos(sym, decls1, sym.companionClass)

          // Attachments are added by EntityCompanionTagger for entities defined in methods because we can't discover
          // them by looking at info decls
          sym.attachments.get[InnerEntities].map(_.syms).getOrElse(Nil).foreach { inner =>
            // Get rid of any trailing underscore
            val newPrefix = if (inner.prefix.endsWith("_")) inner.prefix.dropRight(1) else inner.prefix
            // Don't need to recurse because all such defs will be tagged themselves
            enterNestedInfos(sym, decls1, inner.sym, newPrefix, shouldRecurse = false)
          }

          val parents1 = if (needsEntityInfos(sym)) {
            enterInfoDefsForSym(decls1, sym)
            rewriteParents(sym, parents0)
          } else parents0

          // TODO (OPTIMUS-24864): removing this causes breakage, but why?
          if (isStorableCompanion(sym) && !sym.isStatic) List(sym, sym.module) foreach (_ resetFlag PrivateLocal)

          ClassInfoType(parents1, decls1, sym)

        case ClassInfoType(parents0, decls0, _) if needsEntityInfos(sym) =>
          val parents1 = rewriteParents(sym, parents0)

          val decls1 = decls0.cloneScope
          enterInfoDefsForSym(decls1, sym)

          // TODO (OPTIMUS-24864): removing this causes breakage, but why?
          if (isStorableCompanion(sym) && !sym.isStatic) List(sym, sym.module) foreach (_ resetFlag PrivateLocal)

          ClassInfoType(parents1, decls1, sym)

        case s => s
      }
    } else {
      tpe
    }
  }

  /**
   * TODO (OPTIMUS-13299): Has bugs, as object entity extends someEntity is really broken
   */
  class EntityInfoTransformer(unit: CompilationUnit) extends TypingTransformer(unit) {
    import global._, definitions._

    override def transform(tree: Tree): Tree = {
      try { transformSafe(tree) }
      catch {
        case ex: Throwable =>
          ex.printStackTrace()
          alarm(OptimusErrors.TRANSFORM_ERROR4, tree.pos, ex.toString)
          EmptyTree
      }
    }

    // ensure that calls to Symbol#info see the result of the above info transform
    override def transformUnit(unit: CompilationUnit): Unit =
      afterOwnPhase(super.transformUnit(unit))

    // this is a bit suspect; we're basically doing a large computation and returning a tuple
    class InheritedPropertyCreator(sym: Symbol) {
      assert(isEntityOrEvent(sym), s"must be Storable: $sym")

      private def impliedBaseClasses(sym: Symbol): List[global.Symbol] = sym.typeOfThis.baseClasses

      // We want to know as soon as incompatible definitions are combined, that includes self types which are associated with a trait
      // without it actually inheriting it.
      val baseClasses = impliedBaseClasses(sym).filter(isEntityOrEvent)

      val baseModules = baseClasses.map(_.companionModule)
      val thisIsAConcreteClass = sym.isConcreteClass

      // Find the key infos from this module and modules of parent entity classes and traits
      val indexInfos: List[Tree] = {
        val grps = baseModules flatMap { c =>
          c.info.decls filter { m =>
            m.isGetter && isIndexInfo(
              m) && m.name != names.KeyInfo // TODO (OPTIMUS-47219): check to exclude names.keyInfo should be temporary
          }
        } groupBy { _.name }

        val refs: List[Tree] = grps.toList.sortBy(_._1).map { case (name, ms) =>
          val nameToReport = name.stripSuffix(suffixes.KEY_INFO)
          if (ms.size > 1) {
            val deets =
              s"field $nameToReport has conflicting definitions from ${ms map {
                  _.enclClass.name
                } mkString ("(", ", ", ")")}"
            alarm(OptimusErrors.KEY_SHADOWED, sym.pos, sym.name, deets)
          } else {
            val indexedSymbolOwner = ms.head.owner.companionClass
            val impliedParentClasses = impliedBaseClasses(indexedSymbolOwner).filterNot(_.equals(indexedSymbolOwner))
            val priorStorageDefinitions = impliedParentClasses flatMap { c =>
              c.info
                .decl(nameToReport)
                .filter { m =>
                  m.keyString == "val" || m.hasAnnotation(ValAccessorAnnotation)
                }
                .alternatives
            }
            if (priorStorageDefinitions.nonEmpty) {
              alarm(
                OptimusErrors.INDEX_ADDED_TO_EXISTING_STORED_VAL,
                sym.pos,
                sym.name,
                priorStorageDefinitions.head.owner.name)
            }
          }

          localTyper.typedPos(sym.pos.focus) {
            gen.mkAttributedRef(ms.head)
          }
        }

        refs
      }

      if (sym.isSubClass(InlineEntityClass) && !indexInfos.isEmpty)
        alarm(OptimusErrors.INLINE_ENTITY_WITH_INDEX_KEY, sym.pos)

      private def propertyInfoSym(nodeSym: Symbol): Option[Symbol] = {
        val owner = nodeSym.owner
        val infoHolder = infoHolderForSym(owner)
        val pinfoName = mkPropertyInfoName(infoHolder, nodeSym.name)
        // ignore vals which aren't PropertyInfos (e.g. they might be NodeTaskInfo for raw nodes, but those aren't
        // property nodes and shouldn't be included in the entity info)
        infoHolder.module.info
          .decl(pinfoName)
          .suchThat(s => s.isVal && s.tpe.typeSymbol.isSubClass(PropertyInfo))
          .toOption
      }

      private def withAllPossibleParents(nodeSym: Symbol) = {
        propertyInfoSym(nodeSym) match {
          case None => None
          case Some(pinfo) =>
            val overrides = nodeSym.allOverriddenSymbols

            // For a concrete class, we want all other possible sources of this symbol (see OPTIMUS-57047 and the code
            // in inheritance setters below for details.)
            val indirectOverrides = if (thisIsAConcreteClass) {
              val includedInDirects = overrides.toSet
              baseClasses
                .map(cls => nodeSym.overriddenSymbol(cls))
                .filter(_ != NoSymbol)
                // keep all other version of this symbol that aren't either nodeSym or one of the direct overriden symbols
                .filterNot(sym => includedInDirects.contains(sym) || sym == nodeSym)
            } else Nil

            Some(
              AllPossibleParents(
                pinfo,
                overrides.flatMap(propertyInfoSym).filter(_ != NoSymbol),
                indirectOverrides.flatMap(propertyInfoSym).filter(_ != NoSymbol)
              ))
        }
      }

      private val allPropertyInfoSyms = sym.info.members
        .collect {
          case s
              if s.hasAnnotation(NodeSyncAnnotation) ||
                s.hasAnnotation(ValAccessorAnnotation) ||
                hasMiscFlag(s, MiscFlags.NEEDS_PROPERTY_INFO) =>
            withAllPossibleParents(s)
        }
        .collect { case Some(p) => p }
        .toList

      // This class is initialised 'beforeOwnPhase' so this needs to be a def in order to see the entity info syms
      // that we added in the info transform above
      def storableParents: Tree = {
        val prefs = sym.info.parents.iterator
          .map(_.typeSymbol)
          .filter { sym =>
            isUserDefinedEntity(sym) || isEvent(sym)
          }
          .map { sym =>
            val infoHolder = infoHolderForSym(sym)
            val infoName = newTermName(infoHolder.propertyPrefix + names.info)
            val infoLocalName = if (infoHolder.propertyPrefix.isEmpty) infoName else infoName.localName
            gen.mkAttributedRef(
              genQualifiedModuleRef(infoHolder.module).tpe,
              infoHolder.module.info.decl(infoLocalName))
          }
          .toList
        Apply(Select(gen.mkAttributedRef(ListModule), nme.apply), prefs)
      }

      private def filterTweakables(sym: Symbol, topRtype: Type) = {
        val pirtype = sym.tpe.resultType // R of PropertyInfo[R]
        if (TweakablePropertyInfoN.contains(pirtype.typeSymbol)) {
          val rtype = pirtype.memberType(pirtype.member(tpnames.PropType))
          rtype =:= topRtype
        } else
          false // We only care about tweakable properties
      }

      val inheritanceSetters: List[Tree] =
        allPropertyInfoSyms.iterator
          .map { np: AllPossibleParents =>
            val topPIRtype = np.head.tpe.resultType
            if (topPIRtype.typeArgs.isEmpty) List(np.head) // Custom properties don't support inheritance
            else {
              val topRtype = topPIRtype.typeArgs.last // Result type of property in current class
              //
              // TODO (OPTIMUS-57047): The current behaviour is that only the direct overrides are considered when building
              //  the inheritance setters. This is broken! However fixing it is potentially quite dangerous, so here what we
              //  do instead is emit a warning for anything that we *should* but do not consider in the matchOn statements.
              //
              //  Note that this is only an issue for concrete classes. For traits or abstract classes, the error will appear
              //  in their concrete implementations.
              val overrides = np.overrides.filter(filterTweakables(_, topRtype))

              np.otherSources.filter(filterTweakables(_, topRtype)).filterNot(overrides.contains(_)).foreach { pinfo =>
                alarm(
                  OptimusNonErrorMessages.POTENTIALLY_BROKEN_PROPERTY_TWEAKING,
                  sym.pos,
                  pinfo.name,
                  sym.nameString,
                  pinfo.fullNameString,
                  (overrides :+ np.head)
                    .map(_.fullNameString)
                    .mkString("It will ONLY show property tweaks applied to:\n  ", "\n  ", "")
                )
              }

              np.head :: overrides
            }
          }
          .collect {
            case lst if lst.tail.nonEmpty =>
              val array = mkList(lst.tail.map(gen.mkAttributedRef(_)))
              val sel = Apply(Select(gen.mkAttributedRef(lst.head), names.matchOn_eq), List(array))
              localTyper.typedPos(sym.pos) { sel }
          }
          .toList

      val indexInfoList: Tree = mkList(indexInfos)

      val propInfos: List[Tree] =
        allPropertyInfoSyms.iterator.map { s =>
          val sym = s.head
          localTyper.typedPos(sym.pos)(gen.mkAttributedRef(s.head))
        }.toList

      // explicitly ascribe List[PropertyInfo[_]](...) to avoid lub nightmare
      val propList: Tree = {
        Apply(
          TypeApply(
            gen.mkAttributedSelect(gen.mkAttributedRef(ListModule), List_apply),
            TypeTree(appliedType(PropertyInfo.tpeHK, WildcardType :: Nil)) :: Nil),
          propInfos
        )
      }
    }

    def InheritedPropertyCreator(sym: Symbol) = beforeOwnPhase(new InheritedPropertyCreator(sym))

    def transformSafe(tree: Tree): Tree =
      tree match {
        case ModuleDef(_, _, impl) if isEffectivelyTopLevel(tree.symbol) =>
          import CODE._

          val module = tree.symbol
          val newStats = ListBuffer.empty[Tree]
          val infoHolder = infoHolderForSym(module.moduleClass)
          val topLevelModule = infoHolder.module

          // Gen info forwarder defs if we're an entity / entity companion
          if (isStorableCompanion(module)) newStats += genInfoValDef(module, infoHolder, names.info)
          if (isEntity(module)) newStats += genInfoValDef(module, infoHolder, names.$info)

          def isEntityInfoVal(sym: Symbol): Boolean =
            sym.isSynthetic && sym.isVal && sym.tpe.typeSymbol.isSubClass(StorableInfo)

          // Gen defs for any synthetic entity info val symbols
          module.info.decls.filter(isEntityInfoVal).foreach {
            // Gen module info vals (.$info)
            case fldSym if fldSym.name.endsWith(names.$info.localName) =>
              val module = fldSym.getAndRemoveAttachment[OriginalOwner].get.sym
              val infoCreator = InheritedPropertyCreator(module)
              val parents = localTyper.typedPos(topLevelModule.pos)(infoCreator.storableParents)

              newStats += localTyper.typedPos(topLevelModule.pos) {
                val isStored = module hasAnnotation StoredAnnotation
                ValDef(
                  fldSym,
                  New(ModuleEntityInfoCls, gen.mkClassOf(module.tpe_*), LIT(isStored), infoCreator.propList, parents))
              }

              newStats ++= infoCreator.inheritanceSetters

            // Gen class info vals (.info)
            case fldSym if fldSym.name.endsWith(names.info.localName) =>
              val cls = fldSym.getAndRemoveAttachment[OriginalOwner].get.sym
              val infoCreator = InheritedPropertyCreator(cls)

              newStats += localTyper.typedPos(topLevelModule.pos) {
                ValDef(
                  fldSym, {
                    val isStored = cls hasAnnotation StoredAnnotation
                    val klass = gen.mkClassOf(cls.tpe_*)

                    val upcastDomain = cls.getAnnotation(UpcastingTargetAnnotation) map { annot =>
                      val arg = annot.args.head
                      val pos = annot.pos.focus
                      val tree = Apply(definitions.SomeModule, dupTree(arg))
                      localTyper typed { atPos(pos)(tree) }
                    } getOrElse gen.mkAttributedRef(NoneModule)

                    val parents = localTyper.typedPos(topLevelModule.pos)(infoCreator.storableParents)

                    val args = if (isEvent(cls)) {
                      List(klass, infoCreator.propList, parents, infoCreator.indexInfoList, upcastDomain)
                    } else {
                      List(klass, LIT(isStored), infoCreator.propList, parents, infoCreator.indexInfoList, upcastDomain)
                    }

                    val infoCls = if (isEvent(cls)) EventInfoCls else ClassEntityInfoCls

                    New(infoCls, args: _*)
                  }
                )
              }

              newStats ++= infoCreator.inheritanceSetters

            case _ =>
          }

          val newImpl = deriveTemplate(atOwner(module)(super.transform(impl))) { body =>
            reorderStats(body, newStats)
          }
          deriveModuleDef(tree)(_ => newImpl)

        // Gen info forwarder defs if we're an entity / entity companion
        case ModuleDef(_, _, impl) if isStorableCompanion(tree.symbol) || isEntity(tree.symbol) =>
          val module = tree.symbol
          val newStats = ListBuffer.empty[Tree]

          val infoHolder = infoHolderForSym(module.moduleClass)
          if (isStorableCompanion(module)) newStats += genInfoValDef(module, infoHolder, names.info)
          if (isEntity(module)) newStats += genInfoValDef(module, infoHolder, names.$info)

          val newImpl = deriveTemplate(atOwner(module)(super.transform(impl))) { body =>
            reorderStats(body, newStats)
          }
          deriveModuleDef(tree)(_ => newImpl)

        case cd: ClassDef if isEntityOrEvent(cd.symbol) =>
          val forwarderDef = {
            val forwarder = cd.symbol.info.decl(names.$info)
            localTyper.typedPos(cd.pos) {
              DefDef(forwarder, Select(gen.mkAttributedRef(cd.symbol.companionModule), names.info))
            }
          }
          deriveClassDef(cd)(impl => deriveTemplate(atOwner(cd.symbol)(super.transform(impl)))(forwarderDef :: _))

        // Because we may have changed the inheritance in [1] above, we need to force re-resolution of super constructor
        // calls if the superclass has changed
        case ap @ Apply(Select(sup @ Super(qual, tpe), termNames.CONSTRUCTOR), Nil)
            if sup.symbol.info.parents != beforeOwnPhase(sup.symbol.info.parents) =>
          localTyper.typedPos(ap.pos)(Apply(Select(Super(qual, tpe), termNames.CONSTRUCTOR), Nil))

        case _ =>
          super.transform(tree)
      }

    private def reorderStats(body: List[Tree], newStats: ListBuffer[Tree]): List[Tree] = {
      // since the construction of the StorableInfo depends on the PropertyInfos, make sure they're inited first.
      def shouldMoveBeforeInfoCtor(stat: Tree): Boolean = cond(stat) {
        case vd: ValDef if vd.symbol.isSynthetic =>
          vd.symbol.info.typeSymbol isSubClass PropertyInfo
        case vd: ValDef if vd.symbol.info.typeSymbol isSubClass IndexInfo =>
          // yikes! once we generate these in a typer plugin as we do for PropertyInfos, this should have a synthetic check
          true
        case scc if treeInfo.isSuperConstrCall(scc) => true
      }
      val (before, after) = body.partition(shouldMoveBeforeInfoCtor)
      before ::: newStats.prependToList(after)
    }

    private def genInfoValDef(module: Symbol, infoHolder: InfoHolderModule, infoSuffix: TermName): Tree = {
      val accSym = module.info.decl(infoSuffix)
      val infoName = newTermName(infoHolder.propertyPrefix + infoSuffix).localName
      val topLevelModule = infoHolder.module
      val infoSym = topLevelModule.info.decl(infoName)
      localTyper.typedPos(module.pos) {
        DefDef(accSym, gen.mkAttributedRef(genQualifiedModuleRef(topLevelModule).tpe, infoSym))
      }
    }
  }

}
