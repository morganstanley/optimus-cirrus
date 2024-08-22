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
package optimus.tools.scalacplugins.entity.provider

import optimus.tools.scalacplugins.entity._
import optimus.tools.scalacplugins.entity.reporter.OptimusErrors
import optimus.tools.scalacplugins.entity.reporter.OptimusErrors.NON_TOP_LEVEL_JOB_OWNER
import optimus.tools.scalacplugins.entity.reporter.OptimusNonErrorMessages

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.tools.nsc._
import scala.tools.nsc.ast.TreeDSL
import scala.tools.nsc.plugins.PluginComponent
import scala.tools.nsc.symtab.Flags
import scala.tools.nsc.transform.Transform
import scala.tools.nsc.transform.TypingTransformers

/**
 * PropertyInfoProvider is responsible for creating fields on the companion object of an entity/event to enable features
 * like property-level tweaks and distribution. They're also used internally by optimus for scheduling. These fields
 * return object which are subtypes of PropertyInfo; the exact type of the field depends on the member of the entity
 * they represent. For example, given an entity:
 * {{{
 *  @entity class Foo {
 *    @node def bar: Int = 1
 *  }
 * }}}
 * a method will be generated on the companion:
 * {{{
 *  object Foo {
 *    val bar: PropertyInfo0[Foo, Int] = ...
 *  }
 * }}}
 * Note that objects themselves can also be entities - in that case the PropertyInfo field will be created on the object
 * itself, but with a name suffixed by "_info" to prevent name collisions. <p/> Terminology: <ul> <li>ee* => Entity or
 * Event class/module <li>member* => Relevant member (def or val) of entity/event <li>propInfo* => Relevant property
 * info val of companion object </ul>
 */
class PropertyInfoProvider[A <: Global](pluginData: EntityPluginData, val global: A)
    extends PluginProvider[A]
    with BoundsResolvers {
  provider =>

  private lazy val UntypedUtils = new UntypedUtils with NodeSynthesis {
    override val global: provider.global.type = provider.global
    override val phaseInfo: OptimusPhaseInfo = OptimusPhaseInfo.NoPhase
    override val pluginData: PluginData = provider.pluginData

    // @async(exposeArgTypes = true)/@node(exposeArgTypes = true), @elevated, and @job methods are not properties but still get NodeTaskInfo/ElevatedPropertyInfos generated
    def needsExecutionInfo(memberDef: global.ValOrDefDef): Boolean = {
      hasExposeArgTypes(memberDef.mods) ||
      hasAnnotation(memberDef.mods, tpnames.elevated) ||
      hasAnnotation(memberDef.mods, tpnames.job) // [JOB_EXPERIMENTAL]
    }
  }

  private lazy val TypedUtils = new TypedUtils {
    override val global: provider.global.type = provider.global
    override val phaseInfo: OptimusPhaseInfo = OptimusPhaseInfo.NoPhase
    override val pluginData: PluginData = provider.pluginData
    override def entitySettings: EntitySettings = provider.pluginData.settings
  }

  private lazy val Trees = new TreeDSL {
    override val global: provider.global.type = provider.global
  }

  import global._

  override def macroPlugins: List[analyzer.MacroPlugin] = EntityCompanionTagger :: Nil

  override def analyzerPlugins: List[analyzer.AnalyzerPlugin] = PropertyInfoForwarderGenerator :: Nil

  override def components: List[PluginComponent] =
    new PropertyInfoComponent(pluginData, OptimusPhases.PROPERTY_INFO) :: Nil

  private case class EntityOrEventInfo(
      eeDef: ImplDef,
      optimusProperties: Iterable[ValOrDefDef],
      propertyPrefix: String = "",
      onModule: Boolean = false,
      added: Boolean)
      extends TypedUtils.PropertyInfoWrapping
  private case class EntityOrEventInfos(entries: Seq[EntityOrEventInfo])

  private case class PropertyInfoDetails(memberSym: TermSymbol, baseFlags: Long)

  sealed trait PropertyInfoForwarderDetails {
    def properties: Iterable[ValOrDefDef]
    def prefix: String
    def topLevelModule: Symbol
    def onModule: Boolean
  }
  private case class PropertyInfoForwarderClassDetails(
      properties: Iterable[ValOrDefDef],
      prefix: String,
      topLevelModule: Symbol
  ) extends PropertyInfoForwarderDetails {
    val onModule: Boolean = false
  }
  private case class PropertyInfoForwarderModuleDetails(
      properties: Iterable[ValOrDefDef],
      prefix: String,
      topLevelModule: Symbol
  ) extends PropertyInfoForwarderDetails {
    val onModule: Boolean = true
  }

  // Step 1 (in or before typer): When we encounter an entity or event class/object, create a companion object if it
  // doesn't exist and create the property infos on the top-level companion
  private object EntityCompanionTagger extends analyzer.MacroPlugin {
    import UntypedUtils._
    import TypedUtils.{InnerEntities, InnerEntity}

    // TODO(OPTIMUS-26258): Don't retain all namers during property info generation
    private var companionNamers: Map[Symbol, analyzer.Namer] = Map.empty

    override def isActive(): Boolean = !isPastTyper

    override def pluginsEnterSym(namer: analyzer.Namer, tree: Tree): Boolean = {
      val ctxOwner = namer.context.owner
      tryProcessCompanionNamer(namer, ctxOwner)

      tree match {
        // [A] Some kind of nested class that has fields that need property infos. Note that we don't treat classes
        // in the (class-based) REPL wrapper as nested since from the user's perspective they are top level.
        // Also note that we can't just say !isEffectivelyTopLevel(id.sym) because id doesn't have a sym yet.
        case id: ImplDef
            if !ctxOwner.isPackageClass && !nme.isReplWrapperName(ctxOwner.name) && ownsPropertyInfos(id) =>
          val enclosingTypes = enclosingClassesOrModules(namer.context)

          namer.standardEnterSym(id)

          // Ensure the companion object exists
          val companionClass = (id match {
            case cd: ClassDef  => namer.ensureCompanionObject(cd)
            case md: ModuleDef => md.symbol
          }).moduleClass

          // Ensure the top level companion object exists
          val topLevelContext = enclosingTypes.head
          val topLevelModuleSym = topLevelContext.tree match {
            case cd: ClassDef =>
              cd.symbol.companion
              val packageNamer = namer.enclosingNamerWithScope(topLevelContext.outer.scope)
              val companion = packageNamer.ensureCompanionObject(cd)
              companion
            case md: ModuleDef =>
              md.symbol
          }

          // Create a prefix of the form Inner1_Inner2_...InnerN_ for inner classes 1 .. N
          val prefix = (enclosingTypes.tail.map(_.tree.asInstanceOf[ImplDef].name) :+ id.name).mkString("", "_", "_")
          val symbol = if (id.symbol.isModule) id.symbol.moduleClass else id.symbol

          // If entity defined in method then add attachment to top level module so entity info transform can find it
          if (insideMethod(id.symbol)) {
            val existingInners = topLevelModuleSym.moduleClass.attachments.get[InnerEntities].map(_.syms).getOrElse(Nil)
            if (!existingInners.exists(inner => inner.sym == id.symbol)) {
              topLevelModuleSym.moduleClass.updateAttachment(
                InnerEntities(InnerEntity(id.symbol, prefix) :: existingInners))
            }
          }

          updateAttachments(
            namer,
            topLevelModuleSym,
            id,
            symbol,
            propertyPrefix = prefix,
            onModule = id.symbol.isModule,
            Some(companionClass))
          true

        // [B] Top-level ClassDef that has fields that need property infos
        case cd: ClassDef if ownsPropertyInfos(cd) =>
          namer.standardEnterSym(cd)
          // There's a bug in scalac at present which means that causes companion object visibility to mirror class
          // visibility, even if the companion object has been explicitly defined but is lower in the source file.
          // Fix in scala: https://github.com/scala/scala/pull/7461.
          val moduleSym = namer.ensureCompanionObject(cd)
          updateAttachments(namer, moduleSym, cd, cd.symbol)
          true

        // [C] Top-level ModuleDef that has fields that need property infos
        case md: ModuleDef if ownsPropertyInfos(md) =>
          namer.standardEnterSym(md)
          updateAttachments(namer, md.symbol, md, md.symbol.moduleClass, onModule = true)
          true

        case _ => false
      }
    }

    @tailrec
    private def insideMethod(sym: Symbol): Boolean = {
      if (sym.owner.isPackageClass) false
      else if (!sym.owner.isClass && !sym.owner.isModule) true
      else insideMethod(sym.owner)
    }

    private def tryProcessCompanionNamer(namer: analyzer.Namer, ctxOwner: Symbol): Unit = {
      namer.context.tree match {
        case _: Template if ctxOwner.isModuleClass =>
          // [1] Update the map of top level companions to corresponding Namers
          if (!companionNamers.contains(ctxOwner)) {
            companionNamers = companionNamers.updated(ctxOwner, namer)
          }
          // [2] Create any outstanding property infos on the current companion, and update the EntityOrEventInfos to reflect this
          ctxOwner.attachments.get[EntityOrEventInfos].foreach { eeInfos =>
            val mappedInfos = createPropertyInfos(namer, eeInfos.entries, ctxOwner)
            ctxOwner.updateAttachment(EntityOrEventInfos(mappedInfos))
          }
        case _ =>
      }
    }

    // Enumerate parent ImplDefs that own property infos, skipping Import, Template, and ValOrDefDef trees
    private def enclosingClassesOrModules(context: analyzer.Context): List[analyzer.Context] = {
      def enclosingClassesOrModulesIter(context: analyzer.Context): List[analyzer.Context] = context.tree match {
        case id: ImplDef if id.symbol.isTopLevel => context :: Nil
        case _: ImplDef                          => context :: enclosingClassesOrModulesIter(context.outer)
        case _                                   => enclosingClassesOrModulesIter(context.outer)
      }

      enclosingClassesOrModulesIter(context).reverse
    }

    private def ownsPropertyInfos(id: ImplDef): Boolean =
      isEntityOrEvent(id) || isEntityCompanion(id) || needsExecutionInfos(id)

    private def isEntityOrEvent(id: ImplDef): Boolean =
      hasAnnotation(id.mods, tpnames.entity) || hasAnnotation(id.mods, tpnames.event)

    private def isEntityCompanion(id: ImplDef): Boolean =
      id.impl.parents.exists {
        case AppliedTypeTree(Select(_, nme), _) =>
          (nme eq tpnames.EntityCompanionBase) || (nme eq tpnames.EventCompanionBase)
        case _ => false
      }

    private def needsExecutionInfos(id: ImplDef): Boolean = {
      id.impl.body.exists {
        case dd: DefDef => needsExecutionInfo(dd)
        case _          => false
      }
    }

    private def updateAttachments(
        namer: analyzer.Namer,
        topLevelModule: Symbol,
        id: ImplDef,
        idSym: Symbol,
        propertyPrefix: String = "",
        onModule: Boolean = false,
        companionClass: Option[Symbol] = None
    ): Unit = {
      val topLevelModuleCls = topLevelModule.moduleClass
      val properties = optimusProperties(id)

      // Add attachments to generate forwarder defs for nested entities
      addForwarderInfos(properties, onModule, id, companionClass, propertyPrefix, topLevelModuleCls)

      val existingInners = topLevelModuleCls.attachments.get[EntityOrEventInfos].map(_.entries).getOrElse(Nil)
      companionNamers.get(topLevelModuleCls) match {
        // [A] If we do have the Namer for the top-level companion that we want to create the property infos on, and
        // there are attachments on the symbol that indicate there are property infos to be added, then add them
        case Some(moduleNamer) if !existingInners.exists(ee => ee.eeDef == id && ee.added) =>
          val eeInfo = EntityOrEventInfo(id, properties, propertyPrefix, onModule, added = true)
          createPropertyInfos(moduleNamer, eeInfo, topLevelModuleCls)
          val mappedInners = createPropertyInfos(moduleNamer, existingInners.filter(_.eeDef ne id), topLevelModuleCls)
          topLevelModuleCls.updateAttachment(EntityOrEventInfos(eeInfo +: mappedInners))

        // [B] If we don't have the Namer for the top-level companion that we want to create the property infos on, then
        // add an attachment to the symbol that indicates that it should be added when the Namer is available
        case None if !existingInners.exists(_.eeDef eq id) =>
          val eeInfo = EntityOrEventInfo(id, properties, propertyPrefix, onModule, added = false)
          topLevelModuleCls.updateAttachment(EntityOrEventInfos(eeInfo +: existingInners))
        case _ =>
      }
    }

    private def createPropertyInfos(
        namer: analyzer.Namer,
        eeInfos: Seq[EntityOrEventInfo],
        module: Symbol
    ): Seq[EntityOrEventInfo] = {
      // Enumerate the EntityOrEventInfos on the companion creating the property infos if they haven't been already
      // and update the existing EntityOrEventInfos to reflect this
      eeInfos.map(eeInfo => {
        if (!eeInfo.added) {
          createPropertyInfos(namer, eeInfo, module.asClass)
          eeInfo.copy(added = true)
        } else eeInfo
      })
    }

    private def optimusProperties(eeDef: ImplDef): Iterable[ValOrDefDef] = {
      val isEvent = hasAnnotation(eeDef.mods, tpnames.event)
      val isEntity = hasAnnotation(eeDef.mods, tpnames.entity)
      val isStoredEntity = hasAnnotation(eeDef.mods, tpnames.stored)
      eeDef.impl.body
        .collect {
          case memberDef: ValOrDefDef if isOptimusProperty(isEntity, isEvent, isStoredEntity, memberDef) =>
            memberDef
        }
        .groupBy(_.name)
        .values
        .map { propsForTheSameName =>
          val first = propsForTheSameName.head
          propsForTheSameName.tail.foreach { dupProp =>
            val error =
              if (hasNodeAnno(dupProp.mods)) OptimusErrors.NODE_OVERLOAD_ERROR
              else OptimusErrors.CREATE_NODE_TRAIT_OVERLOAD_ERROR
            alarm(error, dupProp.pos, first.pos.line)
          }
          first
        }
        .toSeq
        .sortBy(_.name) // ensure stable ordering of companion methods
    }

    private def addForwarderInfos(
        properties: Iterable[ValOrDefDef],
        onModule: Boolean,
        eeDef: ImplDef,
        companionClass: Option[Symbol],
        prefix: String,
        topLevelModule: Symbol): Unit = {
      // We only want to generate forwarders if the entity is nested
      val eeSym = eeDef.symbol
      if (!TypedUtils.isEffectivelyTopLevel(eeSym)) {
        if (onModule) {
          val moduleClass = eeSym.moduleClass
          moduleClass.updateAttachment(PropertyInfoForwarderModuleDetails(properties, prefix, topLevelModule))
        } else {
          companionClass.foreach(moduleClass => {
            moduleClass.updateAttachment(PropertyInfoForwarderClassDetails(properties, prefix, topLevelModule))
          })
        }
      }
    }

    private def createPropertyInfos(namer: analyzer.Namer, info: EntityOrEventInfo, topLevelModule: Symbol): Unit = {
      val eeDef = info.eeDef
      info.optimusProperties.foreach { memberDef =>
        val propInfoName = mkPropertyInfoName(info, memberDef.name)
        val propInfoSym =
          topLevelModule.newValue(
            propInfoName,
            topLevelModule.pos.focus,
            Flags.ACCESSOR | Flags.STABLE | Flags.SYNTHETIC | Flags.FINAL)

        val eeCls = if (eeDef.symbol.isModule) eeDef.symbol.moduleClass else eeDef.symbol

        // Lazily compute the result type to allow for cycles between the entity class
        // and a user-written companion
        propInfoSym.setInfo(new PropertyInfoTypeCompleter(eeCls, memberDef, topLevelModule))

        // Add the propInfoSym as a member of the companion object
        namer.enterInScope(propInfoSym)
      }
    }

    private def isOptimusProperty(
        isEntity: Boolean,
        isEvent: Boolean,
        isStoredEntity: Boolean,
        memberDef: ValOrDefDef) = {
      !memberDef.mods.isSynthetic && (
        // all nodes on entities get property infos
        (isEntity && hasAnnotation(memberDef.mods, tpnames.node)) ||
          // certain methods need non-property execution infos
          needsExecutionInfo(memberDef) ||
          // stored properties get property infos (mainly for pickling reasons)
          isStoredProperty(isEvent, isStoredEntity, memberDef)
      )
    }

    private def isStoredProperty(isEvent: Boolean, isStoredEntity: Boolean, memberDef: ValOrDefDef) = memberDef match {
      case _: DefDef => false
      case vd: ValDef =>
        (isEvent || isStoredEntity || hasAnnotation(vd.mods, tpnames.stored)) &&
        !vd.mods.hasFlag(Flags.PARAM) &&
        !vd.mods.isPrivateLocal &&
        !hasAnnotation(vd.mods, tpnames.transient) &&
        !(isEvent && memberDef.name == names.validTime) // ignore the generated validTime field on events
    }

  }

  private object PropertyInfoForwarderGenerator extends analyzer.AnalyzerPlugin {
    import TypedUtils._

    override def isActive(): Boolean = !isPastTyper

    private val forwarderFlags = Flags.STABLE | Flags.SYNTHETIC | Flags.FINAL

    override def pluginsTypeSig(tpe: Type, typer: analyzer.Typer, defTree: Tree, pt: Type): Type = defTree match {
      case _: Template =>
        val moduleClass = typer.context.owner
        if (moduleClass.isModuleClass) {
          moduleClass.attachments
            .get[PropertyInfoForwarderClassDetails]
            .foreach(genForwarders(typer.namer, moduleClass, _))
          moduleClass.attachments
            .get[PropertyInfoForwarderModuleDetails]
            .foreach(genForwarders(typer.namer, moduleClass, _))
        }
        tpe
      case _ => tpe
    }

    private def genForwarders(
        namer: analyzer.Namer,
        moduleClass: Symbol,
        details: PropertyInfoForwarderDetails): Unit = {
      details.properties.foreach { vd =>
        val infoName = mkPropertyInfoName(details.prefix, vd.name, details.onModule)
        val forwarderName = mkPropertyInfoForwarderName(vd.name, details.onModule)
        val forwarderSym = moduleClass.newMethod(forwarderName, moduleClass.pos.focus, forwarderFlags)
        // We create a dummy def pointing to the property info so that the type completer can infer the type from it
        val forwarderDef = newDefDef(
          forwarderSym,
          Select(Ident(details.topLevelModule.module), infoName)
        )(
          Modifiers(forwarderSym.flags),
          forwarderName,
          tparams = Nil,
          vparamss = Nil,
          TypeTree()
        )
        // class A {
        //   @entity trait X { @node def foo: Int = 0 }
        //   @entity object X extends X
        // }
        // The PropertyInfo `val foo` is generated in A, object X gets a forwarder method to that PropertyInfo.
        // If the forwarder's result type is inferred, `Namer.methodSig.resTpFromOverride` uses `Int` as the expected
        // type and fails with the error `found: PropertyInfo, expected: Int` error.
        // The custom LazyType sets an explicit return type at completion time. OptimusRefchecks then issues
        // the friendlier `ENTITY_PROPERTY_INFO_ACCIDENTAL_OVERRIDE` error.
        // We can't set the explicit return type in `forwarderDef` as this forces too many types and leads to cycles.
        object PropertyInfoForwarderTypeCompleter extends LazyType {
          override def complete(sym: Symbol): Unit = {
            import forwarderDef._
            val resTp = TypeTree(details.topLevelModule.module.info.decl(infoName).tpe)
            namer
              .monoTypeCompleter(treeCopy.DefDef(forwarderDef, mods, name, tparams, vparamss, resTp, rhs))
              .complete(sym)
          }
        }
        forwarderSym.setInfo(PropertyInfoForwarderTypeCompleter)
        namer.enterInScope(forwarderSym)
      }
    }
  }

  // Step 2 (in typer): When the type of the PropertyInfo is needed, resolve it based on the
  // properties (types, annotations etc) of the relevant member of the entity/event.
  private class PropertyInfoTypeCompleter(
      eeSym: Symbol,
      memberDef: ValOrDefDef,
      moduleSym: Symbol
  ) extends analyzer.TypeCompleterBase[EmptyTree.type](EmptyTree) {

    override def completeImpl(propInfoSym: Symbol): Unit = {
      eeSym.initialize // force typing of eeSym to ensure memberDef has a symbol

      // If we've got a getter (because we're a val that's been turned into a private[this] val plus an accessor),
      // then the getter will hold the annotations we care about, so use that instead of memberDef.symbol (which
      // represents the private val).
      val memberSym: Symbol = if (memberDef.symbol.hasGetter) memberDef.symbol.getter else memberDef.symbol
      val (propInfoType, details) = PropertyInfoTyper.infoType(eeSym, memberSym.asTerm, moduleSym)
      propInfoSym.setInfo(propInfoType)

      // Attach the necessary info to create the body of the method after typer
      propInfoSym.updateAttachment(details)
    }
  }

  private object PropertyInfoTyper {
    import PropertyFlags._
    import TypedUtils._

    def infoType(eeSym: Symbol, memberSym: TermSymbol, moduleSym: Symbol): (Type, PropertyInfoDetails) = {
      val isEntity = eeSym.hasAnnotation(EntityAnnotation)
      val isEvent = eeSym.hasAnnotation(EventAnnotation)
      val isStoredEntity = isEntity && eeSym.hasAnnotation(StoredAnnotation)

      memberSym.initialize // force typing of memberSym to ensure annotation checks below work

      val storedMember = isStoredMember(memberSym, isEvent, isStoredEntity)
      val isKey = memberSym.hasAnnotation(KeyAnnotation)
      val indexedAnnotation = memberSym.getAnnotation(IndexedAnnotation)
      val isIndexed = indexedAnnotation.isDefined
      val isUniqueIndexed = hasUniqueOption(indexedAnnotation).getOrElse(false)
      val isQueryableIndexed = isIndexed && hasQueryableOption(indexedAnnotation).getOrElse(true)
      val isQueryableByRefOnly = hasQueryableByRefOption(indexedAnnotation).getOrElse(false)
      val isChildToParent = memberSym.hasAnnotation(C2PAnnotation)

      val nodeAnnotation = memberSym.getAnnotation(NodeAnnotation)
      val isNode = nodeAnnotation.isDefined
      val isAsync = memberSym.hasAnnotation(AsyncAnnotation)
      val isJob = memberSym.hasAnnotation(JobAnnotation)
      val isRecursive = memberSym.hasAnnotation(RecursiveAnnotation)
      val isTweakable = hasTweakableOption(nodeAnnotation).getOrElse(false)
      val isByInstanceOnly = memberSym.hasAnnotation(ByInstanceOnlyAnnotation)
      val hasTweakHandler = memberSym.isMethod && eeSym.info.decls.containsName(mkTweakHandlerName(memberSym.name))

      // Don't allow property-level tweaks on nodes with tweak handlers, or on parameterised classes where the node
      // return type is an abstract type parameter
      val isPropertyLevelTweakable =
        isTweakable && !isByInstanceOnly && !hasTweakHandler && !memberSym.tpe.finalResultType.typeSymbol.isAbstractType

      val boundsResolver = boundsResolverFor(eeSym, moduleSym)

      // Convert unbound abstract types to existentials (e.g. `Foo[A <: String]` becomes
      // `Foo[A] forSome { type A <: String }`). Also remove type aliases etc. - see BoundsResolver
      // for details.
      val boundedEeType = boundsResolver.covariantBound(eeSym.tpe_*)

      //                                  params      res
      def unwrapMethod(tp: Type): Option[(List[Type], Type)] = tp match {
        case mt if mt.isDependentMethodType =>
          // we don't handle dependent methods other than by generating GenPropertyInfo as fallback
          None
        case PolyType(_, restpe) =>
          unwrapMethod(restpe) // ignore tparams (boundsResolver will handle the leftover skolems)
        case MethodType(params, restpe) =>
          unwrapMethod(restpe) map { case (params1, restpe) =>
            (params.map(p => boundsResolver.contravariantBound(p.info)) ::: params1, restpe)
          }
        case NullaryMethodType(restpe) =>
          unwrapMethod(restpe)
        case restpe => // anything but poly/method/nmt; should only be value types
          Some(Nil -> boundsResolver.covariantBound(restpe))
      }

      var propInfoType = unwrapMethod(memberSym.info) match {
        case Some((_, NoType)) => NoType // fails below, but we can also get NoType from specificType
        case Some((parameterTypes, rt)) =>
          def applied(infoSym: ClassSymbol) = appliedType(infoSym, boundedEeType :: rt :: parameterTypes)
          if (isEntity || isEvent) {
            if (isIndexed && !isQueryableIndexed) applied(BaseIndexPropertyInfo)
            else if (isKey || isUniqueIndexed) {
              if (isEvent) applied(EventUniqueIndexPropertyInfo)
              else applied(UniqueIndexPropertyInfo)
            } else if (isIndexed) {
              if (isEvent) applied(EventIndexPropertyInfo)
              else if (isQueryableByRefOnly) applied(IndexPropertyInfoForErefFilter)
              else applied(IndexPropertyInfo)
            } else if (isNode) {
              if (isPropertyLevelTweakable) {
                if (isChildToParent) C2PTwkPropertyInfoClass.specificType(boundedEeType, parameterTypes, rt)
                else TwkPropertyInfoClass.specificType(boundedEeType, parameterTypes, rt)
              } else if (isChildToParent) C2PPropertyInfoClass.specificType(boundedEeType, parameterTypes, rt)
              else PropertyInfoClass.specificType(boundedEeType, parameterTypes, rt)
            } else {
              if (isChildToParent) applied(C2PReallyNontweakablePropertyInfo)
              else if (isAsync) AsyncNodeTaskInfo.tpe
              else applied(ReallyNontweakablePropertyInfo)
            }
          } else if (memberSym.hasAnnotation(ElevatedAnnotation)) ElevatedPropertyInfo.tpe
          else if (memberSym.hasAnnotation(JobAnnotation)) NonEntityJobPropertyInfo.tpe
          // is a "raw node" or async not on an entity or event
          else AsyncNodeTaskInfo.tpe

        case _ =>
          if (isEntity || isEvent) {
            // We can't (or don't want to) determine a specific PropertyInfo type here. This is probably because
            // we've encountered a complex member type (eg. a dependent method type)
            alarm(OptimusNonErrorMessages.GENPROPERTYINFO_FALLBACK, memberSym.pos.focus, memberSym, eeSym)
            GenPropertyInfo.tpeHK
          } else if (memberSym.hasAnnotation(ElevatedAnnotation)) ElevatedPropertyInfo.tpe
          // is a "raw node" or async not on an entity or event
          else AsyncNodeTaskInfo.tpe
      }

      if (propInfoType eq NoType) {
        alarm(OptimusErrors.NODE_ARITY_LIMIT, memberSym.pos)
        // use GenPropertyInfo so as not to crash
        propInfoType = GenPropertyInfo.tpe_*
      }

      var baseFlags = 0L
      if (isEntity) {
        if (memberSym.isVal || memberSym.isAccessor) baseFlags |= DONT_CACHE
        if (isTweakable) baseFlags |= TWEAKABLE
        if (hasTweakHandler) baseFlags |= TWEAKHANDLER
        if (!isNode) baseFlags |= NOTNODE
      } else {
        baseFlags |= NOTNODE
        if (!isEvent) baseFlags |= DONT_CACHE
      }

      if (storedMember) baseFlags |= STORED
      if (isKey) baseFlags |= KEY
      if (isIndexed) baseFlags |= INDEXED
      if (isJob) baseFlags |= IS_JOB
      if (isRecursive) baseFlags |= IS_RECURSIVE

      (propInfoType, PropertyInfoDetails(memberSym, baseFlags))
    }

    private def isStoredMember(memberSym: TermSymbol, isEvent: Boolean, isStoredEntity: Boolean) =
      (memberSym.isVal || memberSym.isAccessor) &&
        (isEvent || isStoredEntity || memberSym.hasAnnotation(StoredAnnotation)) &&
        !memberSym.isParameter && !memberSym.isPrivateLocal && !memberSym.hasAnnotation(definitions.TransientAttr)

  }

  // Step 3 (in "optimus_propertyinfo" phase, post-typer): Implement the PropertyInfo val by rewriting the companion
  // object based on the attachments we added in steps 1 and 3.
  private class PropertyInfoComponent(val pluginData: PluginData, val phaseInfo: OptimusPhaseInfo)
      extends PluginComponent
      with WithOptimusPhase
      with Transform
      with TransformGrep
      with TypingTransformers {

    override val global: provider.global.type = provider.global

    protected def newTransformer0(unit: CompilationUnit): Transformer = new PropertyInfoTransformer(unit)

    class PropertyInfoTransformer(unit: CompilationUnit) extends TypingTransformer(unit) {
      import Trees.CODE._
      import TypedUtils._

      override def transform(tree: Tree): Tree = tree match {
        case template: Template =>
          val klass = template.symbol.owner
          val newStats = ListBuffer.empty[Tree]
          if (klass.isModuleClass) {
            if (klass.hasAttachment[EntityOrEventInfos]) {
              val propInfoTrees = for {
                sym <- klass.tpe.decls
                pid <- {
                  // force typing of the symbol if it hasn't already happened, so that we can be sure it will have
                  // a PropertyInfoDetails if appropriate
                  sym.initialize
                  sym.attachments.get[PropertyInfoDetails]
                }
              } yield {
                val propInfoType = sym.info match {
                  case ExistentialType(_, u) => u
                  case t                     => t
                }
                val primaryArgs = constructorArguments(pid, propInfoType)
                val parents = propInfoType match {
                  case RefinedType(clasz :: traits, _) =>
                    Apply(TypeTree(clasz), primaryArgs) :: traits.map(TypeTree(_))
                  case _ => Apply(TypeTree(propInfoType), primaryArgs) :: Nil
                }
                val newPropInfo = gen.mkNew(parents, noSelfType, Nil, pid.memberSym.pos, pid.memberSym.pos)
                localTyper.typedPos(sym.pos.focus)(newValDef(sym, newPropInfo)())
              }
              newStats ++= propInfoTrees
            }

            // If we've got attachments to create forwarder defs then create them:

            if (klass.hasAttachment[PropertyInfoForwarderClassDetails]) {
              val details = klass.attachments.get[PropertyInfoForwarderClassDetails].get
              details.properties.foreach { vd =>
                val infoName = mkPropertyInfoName(details.prefix, vd.name, onModule = false)
                val infoSym = details.topLevelModule.info.decl(infoName)
                val forwarderName = mkPropertyInfoForwarderName(vd.name, onModule = false)
                val forwarderSym = klass.info.decl(forwarderName)
                // If we have an @node def apply on an entity class then its forwarder will have the same name as the
                // apply methods on the companion object
                forwarderSym.alternatives.find(_.tpe.typeSymbol.isSubClass(NodeTaskInfo)).foreach { sym =>
                  newStats += localTyper.typedPos(klass.pos.focus) {
                    DefDef(sym, gen.mkAttributedRef(infoSym))
                  }
                }
              }
            }

            if (klass.hasAttachment[PropertyInfoForwarderModuleDetails]) {
              val details = klass.attachments.get[PropertyInfoForwarderModuleDetails].get
              details.properties.foreach { vd =>
                val infoName = mkPropertyInfoName(details.prefix, vd.name, onModule = true)
                val infoSym = details.topLevelModule.info.decl(infoName)
                val forwarderName = mkPropertyInfoForwarderName(vd.name, onModule = true)
                newStats += localTyper.typedPos(klass.pos.focus) {
                  DefDef(klass.info.decl(forwarderName), gen.mkAttributedRef(infoSym))
                }
              }
            }
          }
          super.transform(deriveTemplate(template)(body => newStats.toList ::: body))
        case _ => super.transform(tree)
      }

      private def constructorArguments(pid: PropertyInfoDetails, tp: Type): List[Tree] = {
        import PropertyFlags._

        // delay resolution of the flags which aren't needed in typer until the classifier has run, since that
        // will add internal annotations which we can use here (eg. @scenarioIndependentInternal)
        var flags = pid.baseFlags
        if (pid.memberSym.hasAnnotation(ScenarioIndependentInternalAnnotation)) flags |= SCENARIOINDEPENDENT
        if (pid.memberSym.hasAnnotation(ScenarioIndependentRhsAnnotation)) flags |= SCENARIOINDEPENDENT
        if (pid.memberSym.hasAnnotation(GivenRuntimeEnvAnnotation)) flags |= GIVEN_RUNTIME_ENV
        if (pid.memberSym.isDeferred) flags |= IS_ABSTRACT

        // for @elevated properties, we copy the annotation (which looks like a constructor call: "new elevated(...)")
        // into the ElevatedPropertyInfo so that it is easily available at runtime
        val annosArgs = if (tp.typeSymbol.asClass.isSubClass(ElevatedPropertyInfo)) {
          val elevatedAnnotation = pid.memberSym.getAnnotation(ElevatedAnnotation).get
          mkList(dupTree(elevatedAnnotation.original) :: Nil) :: Nil
        } else Nil

        val constructorArgs = LIT(pid.memberSym.name.toString) :: LIT(flags) :: annosArgs
        val taskInfoType = tp.typeSymbol.asClass
        if (taskInfoType.isSubClass(NonEntityJobPropertyInfo)) {
          if (!isEffectivelyTopLevel(pid.memberSym.owner))
            alarm(NON_TOP_LEVEL_JOB_OWNER, pid.memberSym.owner.pos)
          gen.mkClassOf(pid.memberSym.owner.tpe) :: constructorArgs
        } else if (taskInfoType.isSubClass(AsyncNodeTaskInfo) || taskInfoType.isSubClass(ElevatedPropertyInfo)) {
          gen.mkClassOf(pid.memberSym.owner.tpe) :: constructorArgs
        } else constructorArgs
      }
    }
  }
}
