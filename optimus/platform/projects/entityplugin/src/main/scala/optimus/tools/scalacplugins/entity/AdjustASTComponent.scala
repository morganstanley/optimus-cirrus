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
import optimus.entityplugin.pretyper._
import optimus.tools.scalacplugins.entity.reporter.OptimusErrors
import optimus.tools.scalacplugins.entity.reporter.OptimusNonErrorMessages
import optimus.tools.scalacplugins.entity.staged.scalaVersionRange

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.Stack
import scala.collection.mutable.HashMap
import scala.collection.mutable.LinkedHashMap
import scala.collection.mutable.ListBuffer
import scala.reflect.internal.util.ListOfNil
import scala.tools.nsc.symtab.Flags
import scala.tools.nsc.transform.Transform

//noinspection TypeAnnotation
// MUST match flags values in NodeTaskInfo.java
object PropertyFlags {
  val NOTNODE = 1L << 0
  val TWEAKABLE = 1L << 1
  val STORED = 1L << 2
  val INDEXED = 1L << 3
  val KEY = 1L << 4
  val SCENARIOINDEPENDENT = 1L << 5
  val GIVEN_RUNTIME_ENV = 1L << 6
  val TWEAKHANDLER = 1L << 7 // Tweak handler is present
  val DONT_CACHE = 1L << 8
  val IS_ABSTRACT = 1L << 9
  val IS_JOB = 1L << 10
  val IS_RECURSIVE = 1L << 11

  def isKey(flags: Long) = (flags & KEY) != 0
  def isNode(flags: Long) = (flags & NOTNODE) == 0
  def isIndexed(flags: Long) = (flags & INDEXED) != 0
  def isKeyOrIndexed(flags: Long) = (flags & (KEY | INDEXED)) != 0
}

class AdjustASTComponent(val plugin: EntityPlugin, val phaseInfo: OptimusPhaseInfo)
    extends EntityPluginComponent
    with Transform
    with TransformGrep
    with UntypedUtils
    with PluginUtils
    with CompanionMethodsGenerator
    with ColumnarPriql
    with NodeSynthesis
    with WithOptimusPhase {
  import global._

  import Flags._
  def newTransformer0(unit: CompilationUnit) = new AdjustAST(unit)

  // TODO (OPTIMUS-51110): Remove this once we're done with package migration
  private case class BannedPackage(
      srcPathPrefix: String,
      bannedParent: TermName,
      bannedPackage: TermName,
      replacement: String)

  private lazy val bannedPackages: Set[BannedPackage] = {
    StaticConfig.stringSet("bannedPackages").map { confStr =>
      val Array(src, banned, replacement) = confStr.split(":")
      // n.b. this is very specific to our particular use case of banning a certain optimus.x package so it's not
      // designed to be flexible at all
      val Array(bannedOuter, bannedInner) = banned.split("\\.")
      BannedPackage(src, TermName(bannedOuter), TermName(bannedInner), replacement)
    }
  }

  /**
   * This is a placeholder with all the information needed to create a companion object.
   */
  case class ModuleSpec(
      isEntity: Boolean,
      isEntityCompanion: Boolean,
      isEvent: Boolean,
      isEmbeddable: Boolean,
      implDef: ImplDef,
      props: ListBuffer[Tree] = ListBuffer(),
      appendProps: ListBuffer[Tree] = ListBuffer()) {
    override def toString: String = {
      s"ModuleSpec(isEntity = $isEntity, isEntityCompanion = $isEntityCompanion, isEvent = $isEvent, " +
        s"isEmbeddable = $isEmbeddable\nimplDef = $implDef\nprops = $props\nappendProps = $appendProps\n)"
    }
  }

  /**
   * Applies various transformations to @entity, @event and @embeddable types, including:
   *
   *   - adding required supertraits such as Entity and BusinessEvent
   *   - generation of apply and uniqueInstance methods on companion
   *   - generation of various @index and @key related methods on companion
   *   - generation of argument traits for @async(exposeArgTypes = true)/@node(exposeArgTypes = true) methods
   *   - various logic for accelerated and projected methods
   *   - various other logic
   *
   * A note on code evolution: We are trying to move logic out of this complex phase in to smaller more cohesive
   * post-typer phases where possible. Please avoid adding new logic here unless essential.
   */
  class AdjustAST(unit: CompilationUnit) extends Transformer {
    import CODE._
    import global._

    private val relevantBannedPackages = bannedPackages.filter(b => unit.source.path.startsWith(b.srcPathPrefix))

    case class EntityScope(isValOrDefDef: Boolean, module: HashMap[TermName, ModuleSpec] = HashMap())
    val packageName = new StateHolder[String]("")

    val scopeStack = new Stack[EntityScope]()
    // List of properties we need to add to the companion object or null if we not in the class/object

    case class Info(var storedOn: Boolean)
    private var curInfo = Info(false)

    /**
     * Add statements to the given companion object, BEFORE any user-written code.
     *
     * Note: these will get added to the companion in the outer scope, and eventually picked up and added to the
     * package.
     *
     * @param name
     *   The companion object to which to add statements.
     * @param trees
     *   The statements to add.
     */
    private def addToCompanion(name: TermName, trees: List[Tree]): Unit = {
      if (name ne null) {
        val existing = scopeStack.top.module(name)
        val existVds = existing.props.collect { case vd: ValDef => vd.name }
        val transformedTrees = trees.map(super.transform)
        existing.props ++= transformedTrees.filter {
          case vd: ValDef =>
            !existVds.contains(vd.name) // don't generate duplicate code, for inherited property used in key/index
          case _ => true
        }
      }
    }

    /**
     * Create a ModuleSpec for the companion object if one doesn't already exist.
     *
     * This will get created in the outer scope, and eventually make its way into the package.
     *
     * @param name
     *   Name of the base class.
     * @param isEntity
     *   Whether this is for an entity.
     * @param isEntityCompanion
     * @param isEvent
     *   Whether this is for an event.
     * @param isEmbeddable
     *   Whether this is for an embeddable.
     * @param implDef
     *   The body of the companion.
     */
    private def ensureCompanion(
        name: TermName,
        isEntity: Boolean,
        isEntityCompanion: Boolean,
        isEvent: Boolean,
        isEmbeddable: Boolean,
        implDef: ImplDef): Unit = {
      val existing = scopeStack.top.module
        .getOrElseUpdate(name, ModuleSpec(isEntity, isEntityCompanion, isEvent, isEmbeddable, implDef))
      if (!existing.isEntityCompanion && isEntityCompanion) {
        val mod = ModuleSpec(true, true, isEvent, isEmbeddable, implDef, existing.props, existing.appendProps)
        scopeStack.top.module.put(name, mod)
      } else if (!existing.isEmbeddable && isEmbeddable) {
        // This is result of the way we are nodifying ALL default arguments
        val mod = ModuleSpec(false, false, false, isEmbeddable, implDef, existing.props, existing.appendProps)
        scopeStack.top.module.put(name, mod)
      }
    }

    /**
     * Methods defined to handle the DocDefs to make entityplugin can work with scalaDoc properly
     */
    private def handleDocDef(trans: Tree => Tree): Tree => Tree = {
      case docDef @ DocDef(comment, definition) => treeCopy.DocDef(docDef, comment, trans(definition))
      case other                                => trans(other)
    }

    /**
     * Use to handle the partial functions used in flatMap, and the first element of the return list is the original
     * tree
     */
    private def handleDocDefHead(trans: Tree => List[Tree]): Tree => List[Tree] = {
      case docDef @ DocDef(comment, definition) =>
        val res = trans(definition)
        treeCopy.DocDef(docDef, comment, res.head) +: res.tail
      case other => trans(other)
    }

    /**
     * Use to handle the partial functions used in flatMap, and the last element of the return list is the original tree
     * (used in Module generation only for now, since we put the companion object before the class to avoid compile
     * error)
     */
    private def handleDocDefTail(trans: Tree => List[Tree]): Tree => List[Tree] = {
      case docDef @ DocDef(comment, definition) =>
        val res = trans(definition)
        res.init :+ treeCopy.DocDef(docDef, comment, res.last)
      case other => trans(other)
    }

    /**
     * Add list of properties passed as ValDefs to the corresponding companion objects corresponding companion objects
     * will created if not found
     */
    private def addPropsToModuleDef(stmts: List[Tree], specs: HashMap[TermName, ModuleSpec]) = {
      val emdef = mutable.HashSet.empty[Name] // Modified ModuleDef map

      def modParents(spec: ModuleSpec, parents: List[Tree]) = {
        val entityCompanionBase =
          if (spec.isEntityCompanion && (spec.isEntity || spec.isEvent)) {
            val hasKeys = !spec.isEvent && spec.implDef.impl.body.exists {
              case key: ValOrDefDef => isKey(key.mods)
              case _                => false
            }

            val base =
              if (spec.isEvent) EventCompanionBaseType
              else if (hasKeys) {
                if (parents.isEmpty) KeyedEntityCompanionClsType else KeyedEntityCompanionBaseType
              } else EntityCompanionBaseType

            AppliedTypeTree(base, List(mkETypeOf(spec.implDef))) :: Nil
          } else Nil

        val embeddableBase = if (spec.isEmbeddable) {
          spec.implDef match {
            case ClassDef(mods, _, _, _) =>
              val base = if (mods.isTrait) EmbeddableTraitCompanionBase else EmbeddableCompanionBaseType
              base :: Nil
            case _ => Nil
          }
        } else Nil

        val extraParents = entityCompanionBase ++ embeddableBase
        if (extraParents.isEmpty) parents
        else {
          val baseParents = parents.filter {
            // no need to have AnyRef as parent when extending a trait/class!
            case Select(_, tpnme.AnyRef) => false
            case _                       => true
          }
          baseParents ++ extraParents
        }
      }

      def copyDeprecation(spec: ModuleSpec, moduleMods: Modifiers): Modifiers = {
        val implDeprecation = getAnnotation(spec.implDef.mods, definitions.DeprecatedAttr.name)
        if (
          implDeprecation.nonEmpty &&
          !hasAnnotation(moduleMods, definitions.DeprecatedAttr.name)
        )
          moduleMods.copy(annotations = implDeprecation.get :: moduleMods.annotations)
        else moduleMods
      }

      // Update existing ModuleDef(s) to add base type e.g. EntityCompanionBaseType or just properties
      val mstmts = stmts map handleDocDef {
        case modDef @ ModuleDef(mods, name, impl) if specs.contains(name) =>
          emdef += name
          val orgSpec = specs(name) // avoid @embeddable final case class has @entity object companion
          val spec = orgSpec.copy(isEntity = orgSpec.isEntity || isEntity(mods))
          val parents = modParents(spec, impl.parents)
          parents foreach atPos(modDef.pos.focus)

          spec.props foreach atPos(modDef.pos.focus)
          spec.props ++= impl.body
          spec.appendProps foreach atPos(modDef.pos.focus)
          spec.props ++= spec.appendProps

          // Add first newely generated vals so that the user code in the constructor can refer to them
          val template = treeCopy.Template(impl, parents, impl.self, spec.props.result())
          treeCopy.ModuleDef(modDef, copyDeprecation(spec, mods), name, template)
        case other => other
      }
      val remainingSpecs = specs filter { case (k, _) => !emdef.contains(k) }
      // Add the new ModuleDefs right before the corresponding ClassDefs so they pick up
      // relevant import statements.
      // TODO (OPTIMUS-13294): can still have issues if import done inside template body
      mstmts flatMap handleDocDefTail {
        case cdef @ ClassDef(mods, name, tparams, rhs) if remainingSpecs contains name.toTermName =>
          val spec = remainingSpecs(name.toTermName)

          atPos(cdef.pos.focus) {
            import Flags._
            val parents = modParents(spec, Nil)
            val template =
              mkTemplate(
                parents,
                noSelfType,
                NoMods,
                Nil,
                List(Nil),
                spec.props.result() ++ spec.appendProps.result(),
                cdef.impl.pos.focus)
            val mmods = Modifiers(cdef.mods.flags & (PROTECTED | PRIVATE | LOCAL), cdef.mods.privateWithin)
            ModuleDef(copyDeprecation(spec, mmods), name.toTermName, template)
          } :: cdef :: Nil
        case other => List(other)
      }
    }

    /**
     * TODO (OPTIMUS-10003):
     *
     * This is a method copied temporarily from the old version of scala.tools.nsc.ast.Trees In the case of Scala 2.11.x
     * it has been modified (e.g it does not use argss anymore) and moved to scala.reflect.internal.TreeGen as
     * mkTemplate method.
     *
     * Generates a template with constructor corresponding to
     *
     * constrmods (vparams1_) ... (vparams_n) preSuper { presupers } extends superclass(args_1) ... (args_n) with mixins
     * { self => body }
     *
     * This gets translated to
     *
     * extends superclass with mixins { self => presupers' // presupers without rhs vparamss // abstract fields
     * corresponding to value parameters def <init>(vparamss) { presupers super.<init>(args) } body }
     */
    def mkTemplate(
        parents: List[Tree],
        self: ValDef,
        constrMods: Modifiers,
        vparamss: List[List[ValDef]],
        argss: List[List[Tree]],
        body: List[Tree],
        superPos: Position): Template = {
      /* Add constructor to template */

      // create parameters for <init> as synthetic trees.
      val vparamss1 = mmap(vparamss) { vd =>
        atPos(vd.pos.focus) {
          val mods = Modifiers(vd.mods.flags & (IMPLICIT | DEFAULTPARAM | BYNAMEPARAM) | PARAM | PARAMACCESSOR)
          ValDef(mods withAnnotations vd.mods.annotations, vd.name, vd.tpt.duplicate, vd.rhs.duplicate)
        }
      }
      val (edefs, rest) = body span treeInfo.isEarlyDef
      val (evdefs, etdefs) = edefs partition treeInfo.isEarlyValDef
      val gvdefs = evdefs map { case vdef @ ValDef(_, _, tpt, _) =>
        copyValDef(vdef)(
          // atPos for the new tpt is necessary, since the original tpt might have no position
          // (when missing type annotation for ValDef for example), so even though setOriginal modifies the
          // position of TypeTree, it would still be NoPosition. That's what the author meant.
          tpt = atPos(vdef.pos.focus)(TypeTree() setOriginal tpt setPos tpt.pos.focus),
          rhs = EmptyTree
        )
      }
      val lvdefs = evdefs collect { case vdef: ValDef => copyValDef(vdef)(mods = vdef.mods | PRESUPER) }

      val constrs = {
        if (constrMods hasFlag TRAIT) {
          if (body forall treeInfo.isInterfaceMember) List()
          else
            List(
              atPos(wrappingPos(superPos, lvdefs))(
                DefDef(
                  NoMods,
                  nme.MIXIN_CONSTRUCTOR,
                  List(),
                  ListOfNil,
                  TypeTree(),
                  Block(lvdefs, Literal(Constant(()))))))
        } else {
          // convert (implicit ... ) to ()(implicit ... ) if its the only parameter section
          val vparamss1NonEmpty =
            if (vparamss1.isEmpty || vparamss1.head.nonEmpty && vparamss1.head.head.mods.isImplicit)
              List() :: vparamss1
            else
              vparamss1
          val superRef: Tree = atPos(superPos)(Select(Super(This(tpnme.EMPTY), tpnme.EMPTY), nme.CONSTRUCTOR))
          val superCall = argss.foldLeft(superRef)(Apply.apply)
          List(
            atPos(wrappingPos(superPos, lvdefs ::: argss.flatten))(
              DefDef(
                constrMods,
                nme.CONSTRUCTOR,
                List(),
                vparamss1NonEmpty,
                TypeTree(),
                Block(lvdefs ::: List(superCall), Literal(Constant(()))))))
        }
      }
      constrs foreach (ensureNonOverlapping(_, parents ::: gvdefs, focus = false))
      // Field definitions for the class - remove defaults.
      val fieldDefs = vparamss.flatten map (vd => copyValDef(vd)(mods = vd.mods &~ DEFAULTPARAM, rhs = EmptyTree))

      global.Template(parents, self, gvdefs ::: fieldDefs ::: constrs ::: etdefs ::: rest)
    }

    private def isStored(tree: ValOrDefDef, isEvent: Boolean) = tree match {
      case _: DefDef => false
      case _: ValDef =>
        (isEvent || (curInfo.storedOn || hasAnnotation(tree.mods, tpnames.stored))) &&
        !tree.mods.hasFlag(PARAM) &&
        !tree.mods.isPrivateLocal &&
        !hasAnnotation(tree.mods, tpnames.transient)
    }

    /**
     * Push a fresh EntityScope onto the stack, run the superclass' tranform, then pop it and harvest the data. Note
     * that calling the superclass transform will cause us to descend recursively and call our transform on all the
     * inner elements.
     */
    private def inScope[B <: Tree](f: B, isLocal: Boolean = false) = {
      scopeStack.push(EntityScope(isLocal))
      val base = super.transform(f).asInstanceOf[B]
      (base, scopeStack.pop().module)
    }

    @inline private def atNode(mods: Modifiers, isModuleDef: Boolean)(f: => Tree) = {
      val oldInfo = curInfo
      curInfo = curInfo.copy()
      curInfo.storedOn = !isModuleDef && mods.annotations.exists(ann => isSpecificAnnotation(ann, tpnames.stored))
      val result = f
      curInfo = oldInfo
      result
    }

    /**
     * generates a "trait foo$node" with accessors for the node parameters and the parent entity. the actual node class
     * will (later) be created to implement this
     */
    private def mkNodeTrait(
        name: TermName,
        returnTpt: Tree,
        tparams: List[TypeDef],
        vparamss: List[List[ValDef]],
        implDef: ImplDef,
        pos: Position) = {
      val entityName = implDef.name
      val mods = Modifiers(Flags.ABSTRACT | Flags.INTERFACE | Flags.TRAIT)
      val traitName = mkNodeTraitName(name)
      val paramAccessors = vparamss.flatten.map { v =>
        DefDef(Modifiers(Flags.DEFERRED), v.name, Nil, Nil, dupTree(v.tpt), EmptyTree)
      }
      // if the entity's name isn't a TypeName, then it's a TermName, i.e. the name of a singleton object, so select that object's type, not its companion class type
      val entityType = if (entityName.isTypeName) mkCTypeOf(implDef) else SingletonTypeTree(Ident(entityName))
      val entityAccessor = DefDef(Modifiers(Flags.DEFERRED), names.entity, Nil, Nil, entityType, EmptyTree)
      val body = entityAccessor :: paramAccessors

      val parents = returnTpt match {
        case TypeTree() => Nil
        case _          => List(AppliedTypeTree(CompletableNodeType, List(dupTree(returnTpt))))
      }

      val impl = mkTemplate(parents, noSelfType, Modifiers(Flags.TRAIT), Nil, Nil, body, pos)
      atPos(pos)(ClassDef(mods, traitName, dupTree(tparams), impl))
    }

    // Create and return node trait if applicable. If we've got stored properties, also generate the relevant
    // initPicklers call on the companion object.
    private def createNodeTrait(
        classDef: ImplDef,
        tree: ValOrDefDef,
        vparamss: List[List[ValDef]],
        flags: Long
    ): Option[Tree] = {

      val mods = tree.mods
      val name = tree.name
      val pos = tree.pos.focus

      val isC2P = isStoredC2P(mods)
      val isKey = (flags & PropertyFlags.KEY) != 0
      val isIndex = (flags & PropertyFlags.INDEXED) != 0
      val isKeyOrIndex = isKey || isIndex

      if (isC2P && isKeyOrIndex)
        alarm(OptimusErrors.CHILD_PARENT_WITH_KEY_INDEX, tree.pos)

      // Create node trait in the class that contains current tree
      if (hasExposeArgTypes(mods)) {
        Some(mkNodeTrait(name, tree.tpt, typeParamsOf(tree), vparamss, classDef, pos))
      } else None
    }

    def shouldMakeNode(mods: Modifiers): Boolean = hasNodeAnno(mods)

    override def transform(tree: Tree): Tree = {
      try {
        // TODO (OPTIMUS-51110): Remove this once we're done with package migration
        relevantBannedPackages.foreach { ban =>
          tree match {
            case Select(Ident(ban.bannedParent), ban.bannedPackage) =>
              alarm(
                OptimusNonErrorMessages.BANNED_PACKAGE,
                tree.pos,
                s"${ban.bannedParent}.${ban.bannedPackage}",
                ban.replacement)
            case _ =>
          }
        }

        transformSafe(tree)
      } catch {
        case ex: Throwable =>
          ex.printStackTrace()
          alarm(OptimusErrors.TRANSFORM_ERROR_ADJUST_AST, tree.pos, phaseName, ex)
          null
      }
    }

    private def annotationFlags(vd: ValOrDefDef, initflags: Long) = {
      import PropertyFlags._

      val mods = vd.mods
      var flags = initflags
      if (hasAnnotation(mods, tpnames.scenarioIndependent))
        flags |= SCENARIOINDEPENDENT
      if (hasAnnotation(mods, tpnames.key))
        flags |= KEY
      if (hasAnnotation(mods, tpnames.indexed))
        flags |= INDEXED
      if (hasAnnotation(mods, tpnames.givenRuntimeEnv))
        flags |= GIVEN_RUNTIME_ENV
      if (hasAnnotation(mods, tpnames.job))
        flags |= IS_JOB
      if (hasAnnotation(mods, tpnames.recursive))
        flags |= IS_RECURSIVE

      val badFlagsCombo = PropertyFlags.TWEAKABLE | PropertyFlags.SCENARIOINDEPENDENT
      if ((flags & badFlagsCombo) == badFlagsCombo)
        alarm(OptimusErrors.SCENARIO_INDEPENDENT_CANT_TWEAK, vd.pos)
      flags
    }

    private def transformRawNodeTemplate(mods: Modifiers, className: Name, cmdef: ImplDef) = {
      cmdef match {
        case cd: ClassDef =>
          val ctorTrees = cd.impl.body.collect {
            case dd: DefDef if dd.name == nme.CONSTRUCTOR => dd
          }
          val ctorTreesSynthesis = constructorTrees(ctorTrees, cd, false, true)
          if (ctorTreesSynthesis.nonEmpty) {
            ensureCompanion(cmdef.name.toTermName, false, false, false, false, cmdef)
            addToCompanion(className.toTermName, ctorTreesSynthesis)
          }
        case _ =>
      }

      val impl = cmdef.impl
      val newBody = flatMapConserve(impl.body) {
        handleDocDefHead { tree =>
          transformRawNodeDef(
            tree,
            dd => mkNodeTrait(dd.name, dd.tpt, typeParamsOf(dd), dd.vparamss, cmdef, dd.pos.focus),
            mods,
            cmdef)
        }
      }
      treeCopy.Template(impl, impl.parents, impl.self, newBody)
    }

    private def transformBackedDef(mods: Modifiers, impl: Template): Template = {

      def checkLegalBackedDef(dd: DefDef, noArgs: Boolean, flags: Long): Unit = {
        import PropertyFlags._

        if (!mods.hasAnnotationNamed(tpnames.stored))
          alarm(OptimusErrors.BACKED_IN_TRANSIENT, dd.pos)
        if (!hasNodeAnno(dd.mods))
          alarm(OptimusErrors.BACKED_WITH_NODE, dd.pos)
        if (hasAnnotation(dd.mods, tpnames.transient))
          alarm(OptimusErrors.BACKED_WITH_TRANSIENT, dd.pos)

        if (!noArgs)
          alarm(OptimusErrors.BACKED_WITH_PARAMETERS, dd.pos)
        if ((flags & TWEAKHANDLER) == TWEAKHANDLER)
          alarm(OptimusErrors.BACKED_WITH_TWEAKHANDLER, dd.pos)
        if ((flags & TWEAKABLE) != TWEAKABLE)
          alarm(OptimusErrors.BACKED_WITH_NODE, dd.pos)
      }

      val newBody = flatMapConserve(impl.body) {
        case dd @ DefDef(mods, name, tparams, vparamss, tpt, rhs)
            if hasAnnotation(mods, tpnames.backed) && (name != nme.CONSTRUCTOR) && !mods.hasFlag(Flags.SYNTHETIC) =>
          val haveHandlers: Set[Name] = impl.body.collect { case dd: DefDef =>
            dd.name
          }.toSet

          val noArgs = vparamss.isEmpty || (vparamss.head.isEmpty && vparamss.tail.isEmpty)
          val tweakable = getNodeTweakOnSetting(dd) && dd.tparams.isEmpty
          var flags = if (tweakable) PropertyFlags.TWEAKABLE else 0L
          if (haveHandlers.contains(mkTweakHandlerName(name)))
            flags |= PropertyFlags.TWEAKHANDLER

          checkLegalBackedDef(dd, noArgs, flags)

          val backedNodeName = mkBackedNodeName(name)

          val newMembers = {
            val tweakHandlerParamName = nme.x_0
            val tweakHandlerParam = List(ValDef(Modifiers(Flags.PARAM), tweakHandlerParamName, dupTree(tpt), EmptyTree))

            // TODO (OPTIMUS-13295): there should a better way to generate: Tweaks( stored$sth := Some(x) )
            val tweakHandlerRhs = Apply(
              Ident(names.Tweaks),
              List(
                Apply(
                  Select(Ident(backedNodeName), names.colonEquals),
                  List(Apply(Ident(definitions.SomeModule), List(Ident(tweakHandlerParamName))))))
            )

            // tweak handler def
            val tweakHandlerDef = DefDef(
              Modifiers(mods.flags),
              mkTweakHandlerName(name),
              List.empty,
              List(tweakHandlerParam),
              TypeTree(),
              tweakHandlerRhs)
            val valDef =
              ValDef(mods, backedNodeName, AppliedTypeTree(Ident(tpnames.Option), List(tpt)), Ident(names.None))
            valDef :: tweakHandlerDef :: Nil
          }

          // generate the transformed @node implementation
          val newRhs = atPos(dd.pos.focus) {
            If(Select(Ident(backedNodeName), names.isDefined), Select(Ident(backedNodeName), nme.get), dupTree(rhs))
          }

          treeCopy
            .DefDef(dd, mods, name, tparams, vparamss, tpt, newRhs)
            .setPos(dd.pos.makeTransparent) :: (newMembers map atPos(dd.pos.focus))
        case vd @ ValDef(mods, _, _, _) if hasAnnotation(mods, tpnames.backed) && !mods.hasFlag(Flags.SYNTHETIC) =>
          alarm(OptimusErrors.BACKED_VAL, vd.pos)
          vd :: Nil

        case other => other :: Nil
      }

      treeCopy.Template(impl, impl.parents, impl.self, newBody)
    }

    private def reportNodeCantBeUsedInEventError(tree: Tree): Unit = {
      alarm(OptimusErrors.NONENTITY_NODE_WITH_VAL, tree.pos)
    }

    private def transformReified(implDef: ImplDef, dd: DefDef, className: Name): Unit = {
      // Given @reified final def foo = this.bar, we will need to generate something similar to following code
      // in the companion object:
      //   val foo$reified = PluginSupport.genReifiedLambda { arg$ => arg$.bar }
      // thus we have to replace "this" with "arg$", we only handle simple cases here and rely on LambdaReifier
      // to check the correctness of the tree
      class ThisRewriter(thisName: TermName, existProps: Set[Name], existParams: List[Name]) extends Transformer {
        private[this] var paramScope: ScopedSet[Name] = {
          val set = new ScopedSet[Name]()
          existParams.foreach(set.add)
          set
        }

        def rewrite(tree: Tree): Tree = transform(tree)

        override def transform(tree: Tree): Tree = {
          tree match {
            case Ident(name) if paramScope.contains(name) => tree
            case Ident(name) if existProps.contains(name) => atPos(tree.pos) { Select(Ident(thisName), name) }
            case _: This | _: Super                       => atPos(tree.pos) { Ident(thisName) }
            case Function(vparams, body) =>
              val b = atParamScope(vparams) { super.transform(body) }
              treeCopy.Function(tree, vparams, b)
            case _: Block => atParamScope(Nil) { super.transform(tree) }
            case _        => super.transform(tree)
          }
        }

        private def atParamScope(params: List[ValDef])(f: => Tree): Tree = {
          paramScope = new ScopedSet(paramScope)
          params.foreach(p => paramScope.add(p.name))
          val result = f
          paramScope = paramScope.previous
          result
        }

        class ScopedSet[T](val previous: ScopedSet[T]) {
          def this() = this(null)
          private val set = new mutable.HashSet[T]

          def add(elem: T): Boolean = set.add(elem)
          def contains(elem: T): Boolean =
            set.contains(elem) || (previous != null && previous.contains(elem))
        }
      }

      dd match {
        case DefDef(mods, name, tparams, vparamss, tpt, rhs) if hasAnnotation(mods, tpnames.reified) =>
          if (tparams.nonEmpty)
            alarm(OptimusErrors.REIFIED_WITH_TYPEARGS, dd.pos)

          val module = scopeStack.top.module(className.toTermName)
          val existProps = module.implDef.impl.body
            .collect { case dd: DefDef if dd.name == nme.CONSTRUCTOR => dd }
            .head
            .vparamss
            .flatMap(_.map(_.name))
            .toSet[Name]
          val reified = atPos(dd.pos.focus) {
            val arg = ValDef(Modifiers(SYNTHETIC | FINAL), names.arg, mkETypeOf(implDef), EmptyTree)
            val otherArgs =
              vparamss.flatten.map(vd => ValDef(Modifiers(SYNTHETIC | FINAL), vd.name, vd.tpt.duplicate, EmptyTree))
            val newRhs = new ThisRewriter(arg.name, existProps, otherArgs.map(_.name)).rewrite(rhs.duplicate)
            val lambda = Function(arg :: otherArgs, newRhs)
            val impl = Apply(PluginSupport.genReifiedLambda, lambda :: Nil)
            ValDef(Modifiers(SYNTHETIC | FINAL | LAZY), TermName("" + name + suffixes.REIFIED), TypeTree(), impl)
          }
          addToCompanion(className.toTermName, reified :: Nil)
        case _ => // do nothing
      }
    }

    private def transformAccelerated(implDef: ImplDef, dd: DefDef, className: Name): DefDef = {
      def withAccInfoAnnotation(mods: Modifiers, path: String): Modifiers = {
        val accInfoAnno = mkAppliedAnnotation(
          AccelerateInfoAnnotation,
          NamedArgTree(Ident(accelerateInfoAnnotationNames.path), Literal(Constant(path))) :: Nil)
        mods withAnnotations (accInfoAnno :: Nil)
      }

      @tailrec
      def memberAccessOnly(sel: Select): Boolean = {
        sel.qualifier match {
          case s: Select => memberAccessOnly(s)
          case _: Ident  => true
          case _: This   => true
          case _: Super  => true
        }
      }

      def argIsSimple(a: Tree): Boolean = a match {
        case Select(_: This | _: Super, _) | Ident(_) => true
        case _                                        => false
      }

      dd match {
        case DefDef(mods, name, tparams, vparamss, tpt, rhs) if hasAnnotation(mods, tpnames.projected) =>
          if (hasAnnotation(mods, tpnames.reified)) alarm(OptimusErrors.PROJECTED_WITH_REIFIED, dd.pos)
          if (hasAnnotation(mods, tpnames.handle)) alarm(OptimusErrors.PROJECTED_WITH_HANDLER, dd.pos)
          if (vparamss.nonEmpty) alarm(OptimusErrors.PROJECTED_MUST_PARAMLESS_DEF, dd.pos)

          val isKey = hasAnnotation(mods, tpnames.key)
          val isIndex = hasAnnotation(mods, tpnames.indexed)
          val isKeyOrIndex = isKey || isIndex
          val newMods = rhs match {
            case Ident(_) | Select(_: This, _) if isKeyOrIndex =>
              // @indexed @projected def foo = this.bar
              mods
            case s @ Select(qual, _) if !qual.isInstanceOf[This] && memberAccessOnly(s) =>
              // index into embeddable case class properties, we will do further check in RefChecks
              addToCompanion(
                className.toTermName,
                genProjectedMember(
                  implDef,
                  dd.pos.focus,
                  dd.name,
                  isProjectionIndexed(dd.mods),
                  isProjectionQueryable(dd.mods)) :: Nil)
              val rawPath = s.toString()
              val path = if (rawPath.startsWith("this.")) rawPath.substring(5) else rawPath
              withAccInfoAnnotation(mods, path)
            case Apply(Select(_, name), args)
                if isKeyOrIndex && name.toString.endsWith(s"Tuple${args.length}") && args.forall(argIsSimple) =>
              withAccInfoAnnotation(mods, "")
            case _ =>
              alarm(OptimusErrors.PROJECTED_INVALID_DEF, dd.pos)
              mods
          }
          treeCopy.DefDef(dd, newMods, name, tparams, vparamss, tpt, rhs)
        case _ => dd
      }
    }

    private def transformStorableTemplate(isModDef: Boolean, implDef: ImplDef) = {
      val mods = implDef.mods
      val impl = implDef.impl
      val className = implDef.name
      val typeName = className.toTypeName
      val termName = className.toTermName
      val classMods = mods

      val isEvent = AdjustASTComponent.this.isEvent(mods)
      val createImplicitConversion = isExecuteColumn(classMods)

      val ctorTrees = impl.body.collect {
        case dd: DefDef if dd.name == nme.CONSTRUCTOR => dd
      }
      val primaryCtorOpt = ctorTrees.headOption

      if (isEvent && isEntity(mods))
        alarm(OptimusErrors.EVENT_WITH_ENTITY, implDef.pos)

      if (implDef.mods.isImplicit && !isModDef) // @entity implicit class crashes; @entity implicit object is "ok"
        alarm(OptimusErrors.ILLEGAL_IMPLICIT, implDef.pos, if (isEvent) "event" else "entity")

      // show errors for variadic constructor params. in theory it's possible to support these, but needs lots of work to convert T* to Seq[T] and t to t: _* all
      // over the place. it's easier to just display a helpful error.
      val entityFieldToType: Map[TermName, String] = {
        val args = primaryCtorOpt map { _.vparamss.flatten } getOrElse Nil
        args.iterator.map { a =>
          a.tpt match {
            case AppliedTypeTree(Select(_, tpnme.REPEATED_PARAM_CLASS_NAME), _) =>
              alarm(OptimusErrors.VARIADIC_CTOR_PARAM, a.pos, a.name)
            case _ =>
          }
          // now we only take fields which are not declared in class body, e.g. @entity Test(val id: Int, val name: String)
          a.name -> a.tpt.toString
        }.toMap
      }

      val parentType = atPos(impl.pos.focus) {
        if (hasAnnotation(mods, definitions.ScalaInlineClass.name.toTypeName)) {
          if (isModDef) alarm(OptimusErrors.INLINE_WITH_ENTITY, impl.pos)
          InlineEntityType
        } else if (isEvent) EventType
        else EntityType
      }

      val extraParents =
        if (isEventContained(mods)) {
          val containedEventParent = atPos(impl.pos.focus) { ContainedEventType }
          parentType :: containedEventParent :: Nil
        } else parentType :: Nil

      val baseParents = impl.parents.filter {
        // no need to have AnyRef as parent when extending a trait/class!
        case Select(_, tpnme.AnyRef)                    => false
        case Select(_, name) if parentType.name == name => false
        case _                                          => true
      }
      val newParents: List[Tree] = {
        // Any Class parent needs to be head of list, so append trait to end of list
        if (!mods.isTrait) baseParents ++ extraParents
        else extraParents ++ baseParents
      }

      ensureCompanion(termName, true /*entity*/, !isModDef, isEvent, false, implDef)

      val valsFromCtor = {
        primaryCtorOpt map { case DefDef(_, _, _, vparamss, _, _) =>
          vparamss.flatten.collect {
            case vd @ ValDef(mods, name, _, rhs) if !rhs.isEmpty && !hasAnnotation(mods, tpnames.storedVar) =>
              (name, vd)
          }.toMap
        } getOrElse Map.empty
      }

      val prop2WithDefValue = new LinkedHashMap[TermName, (ValDef, Tree)]()

      val haveHandlers: Set[Name] = impl.body.collect { case dd: DefDef =>
        dd.name
      }.toSet

      // Tracks and verifies generated keys for this Storable
      val keyGen = new KeyGenerator(implDef, isEvent)

      val newBody = impl.body flatMap handleDocDefHead {
        case d @ DefDef(mods, _, _, _, _, _)
            if hasAnnotation(mods, tpnames.givenAnyRuntimeEnv) ||
              (hasAnnotation(mods, tpnames.givenRuntimeEnv) && !hasNodeAnno(mods)) =>
          alarm(OptimusErrors.GIVEN_RUNTIME_ENV_MISUSE, d.pos); impl.body

        // TODO (OPTIMUS-13291): ValDef and DefDef probably should be more similar (e.g. syncNode added see also RedirectAccessors)
        case tree @ (vd @ ValDef(mods, name, tpt, rhs)) /*if !mods.isParameter*/ =>
          if (hasAnnotation(mods, tpnames.job)) alarm(OptimusErrors.JOB_WITH_VAL, vd.pos) // [[JOB_EXPERIMENTAL]]
          assertNoBothIndexedAndKey(vd)
          val hasStoredAnnotation = hasAnnotation(mods, tpnames.stored)
          if (hasStoredAnnotation && mods.isPrivateLocal)
            alarm(OptimusErrors.STORED_WITH_PRIVATE, tree.pos)
          if (vd.mods.isLazy)
            alarm(OptimusErrors.LAZY_VAL_IN_DEFINITION, tree.pos, if (isEvent) "event" else "entity")
          if (isEvent && hasNodeAnno(mods)) reportNodeCantBeUsedInEventError(vd)
          if (hasAnnotation(vd.mods, tpnames.handle)) alarm(OptimusErrors.HANDLER_WITH_VAL, vd.pos)
          if (hasAnnotation(vd.mods, tpnames.impure)) alarm(OptimusErrors.IMPURE_WITH_VAL, vd.pos)
          if (hasAnnotation(vd.mods, tpnames.givenAnyRuntimeEnv) || hasAnnotation(vd.mods, tpnames.givenRuntimeEnv))
            alarm(OptimusErrors.GIVEN_RUNTIME_ENV_MISUSE, vd.pos)
          if (hasAnnotation(vd.mods, tpnames.async)) reportAsyncValError(vd)
          if (getStoredProjectedParam(mods).isDefined)
            alarm(OptimusErrors.STORED_PROJECTED_INVALID_VAL, tree.pos)

          if (!mods.hasAllFlags(Flags.PRIVATE | Flags.LOCAL | Flags.PARAMACCESSOR)) {
            if (!rhs.isEmpty)
              prop2WithDefValue(vd.name) = (vd, vd.rhs)
            else {
              val ctorPar = valsFromCtor.get(vd.name)
              if (ctorPar.isDefined)
                prop2WithDefValue(vd.name) = (vd, ctorPar.get.rhs)
            }
          }
          var newMods = mods
          val newMembers: List[Tree] =
            if (!isEvent && (hasNodeAnno(mods) || isStored(vd, isEvent))) {
              if (mods.isPrivateLocal && mods.isParamAccessor)
                alarm(OptimusErrors.NODE_WITH_FORMAL_PARAM, tree.pos)
              if (mods.isPrivateLocal)
                alarm(OptimusErrors.NODE_WITH_PRIVATE_VAL, tree.pos)
              if (mods.isMutable)
                alarm(OptimusErrors.NODE_WITH_VAR, tree.pos)

              val tweakInfo = getNodeTweakInfoOnSetting(vd)
              val tweakable = tweakInfo.isTweakable
              if (hasNodeAnno(mods) && !tweakable && mods.isFinal)
                alarm(OptimusNonErrorMessages.NODE_WITH_NONTWEAKABLE_VAL, vd.pos)

              var flags = if (tweakable) PropertyFlags.TWEAKABLE else 0L

              if (isStored(vd, isEvent))
                flags |= PropertyFlags.STORED

              if (!shouldMakeNode(mods))
                flags |= PropertyFlags.NOTNODE

              flags = annotationFlags(vd, flags | PropertyFlags.DONT_CACHE)

              if (PropertyFlags.isKeyOrIndexed(flags)) {
                if (
                  PropertyFlags.isNode(
                    flags) && (tweakable || (tweakInfo.isDefault && !mods.isFinal && !classMods.isFinal))
                )
                  alarm(OptimusErrors.KEY_INDEX_MUST_STABLE, vd.pos)
                if (!newMods.isDeferred)
                  newMods |= FINAL
              }

              var newAnnos: List[Apply] = Nil
              if (tweakable) newAnnos ::= mkAnnotation(TweakableAnnotationType, vd.pos)
              else if (!tweakInfo.isDefault) newAnnos ::= mkAnnotation(NonTweakableAnnotationType, vd.pos)

              // If we are implicitly @stored or child-to-parent, make sure we have those annotations so we can
              // just check for them in later passes
              if (!isModDef && !hasStoredAnnotation && isStored(vd, isEvent))
                newAnnos ::= mkAnnotation(StoredAnnotation, vd.pos)
              if (isStoredC2P(mods))
                newAnnos ::= mkAnnotation(C2PAnnotation, vd.pos)

              newMods = mods.withAnnotations(newAnnos.map(transformSafe))

              if (haveHandlers.contains(mkTweakHandlerName(name)))
                alarm(OptimusErrors.NODE_WITH_TWEAKHANDLER, tree.pos)

              createNodeTrait(implDef, vd, Nil, flags).toList
            } else if (mods.isMutable) {
              alarm(OptimusErrors.VAR_IN_ENTITY, tree.pos, if (isEvent) "event" else "entity")
              Nil
            } else if (!isModDef && isStored(vd, isEvent)) {
              if (!isEvent)
                internalErrorAbort(vd.pos, s"Expected $vd to be in an event")
              if (!hasStoredAnnotation) {
                val newAnnos = mkAnnotation(StoredAnnotation, vd.pos) :: Nil
                newMods = mods.withAnnotations(newAnnos.map(transformSafe))
              }
              newMods = withMiscFlag(newMods, MiscFlags.NEEDS_PROPERTY_INFO)

              var flags = PropertyFlags.STORED | PropertyFlags.NOTNODE
              flags = annotationFlags(vd, flags)

              Nil
            } else
              Nil

          if (mods.hasAnnotationNamed(tpnames.key) || mods.hasAnnotationNamed(tpnames.indexed)) {
            if (mods.isPrivateLocal)
              alarm(OptimusErrors.KEY_INDEX_WITH_PRIVATE, tree.pos)
            addToCompanion(implDef.name.toTermName, keyGen.companionKeyTrees(vd))
          }

          // we have to retain the original ValDef because if it's a constructor param then it has a magical link to that param...
          // we'll need that during RedirectAccessors when we make the "var foo$impl". unfortunately that means that we can't generate
          // the "def foo" here, because the "val foo" will cause that to be created by scalac and they will conflict. so we have to rewrite
          // def foo in RedirectAccessorsComponent too
          val newMembersFocused = newMembers map atPos(vd.pos.focus)
          treeCopy.ValDef(tree, newMods, name, tpt, rhs) :: newMembersFocused
        // @node def fun... transformation
        case dd @ DefDef(mods, name, tparams, vparamss, tpt, rhs)
            if shouldMakeNode(mods) && (name != nme.CONSTRUCTOR) && !mods.hasFlag(Flags.SYNTHETIC) =>
          checkIllegalDefAnnots(dd, implDef)

          if (hasAnnotation(mods, tpnames.key) || hasAnnotation(mods, tpnames.indexed))
            alarm(OptimusErrors.KEY_INDEX_DEF_CANNOT_BE_ASYNC, dd.pos)

          if (mods.isPrivateLocal)
            alarm(OptimusErrors.NODE_WITH_PRIVATE_DEF, dd.pos)

          if (isEvent) {
            transformRawNodeDef(
              dd
            ) // if @node in @events, handle it as raw node (no tweakability, no cacheability, no plugin)
          } else {
            val tweakInfo = getNodeTweakInfoOnSetting(dd)
            val tweakable = tweakInfo.isTweakable

            var flags = if (tweakable) PropertyFlags.TWEAKABLE else 0L
            if (haveHandlers.contains(mkTweakHandlerName(name))) flags |= PropertyFlags.TWEAKHANDLER
            flags = annotationFlags(dd, flags)

            var newAnnos: List[Tree] = Nil
            if (tweakable) {
              if (dd.tparams.nonEmpty) alarm(OptimusErrors.TWEAKNODE_WITH_GENERIC_TYPE, dd.pos)
              newAnnos ::= mkAnnotation(TweakableAnnotationType, dd.pos)
            } else if (!tweakInfo.isDefault) newAnnos ::= mkAnnotation(NonTweakableAnnotationType, dd.pos)

            val newMods = mods.withAnnotations(newAnnos.map(transformSafe))

            val newMembers = createNodeTrait(implDef, dd, vparamss, flags).toList
            transformReified(implDef, dd, className)
            treeCopy.DefDef(dd, newMods, name, tparams, vparamss, tpt, rhs) :: (newMembers map atPos(dd.pos.focus))
          }
        case dd @ DefDef(mods, name, tparams, vparamss, tpt, rhs)
            if hasAnnotation(mods, tpnames.key) || hasAnnotation(mods, tpnames.indexed) =>
          assertNoBothIndexedAndKey(dd)
          checkIllegalDefAnnots(dd, implDef)

          if (hasAnnotation(mods, tpnames.async))
            alarm(OptimusErrors.KEY_INDEX_DEF_CANNOT_BE_ASYNC, dd.pos)

          addToCompanion(implDef.name.toTermName, keyGen.companionKeyTrees(dd))
          // Suppress sync->async warnings on @key defs referring to @nodes.  See OPTIMUS-3710
          // This should only be possible with "synthetic" nodes due to lazy-loaded vals.
          var newMods = {
            val newAnnos = mkAnnotation(EntersGraphAnnotation, dd.pos.focus) :: Nil
            mods.withAnnotations(newAnnos.map(transformSafe))
          }

          // we disallow overriding a non-abstract '@indexed def', the reason is as following:
          //
          //  trait FooT {  @indexed def idx1: String  }
          //  class Foo(val a: String, val b: String) extends FooT {
          //      @indexed def idx1 = a
          //  }
          //  class Foo2(_a: String, _b: String) extends Foo(_a, _b) {
          //      override def idx1 = b
          //  }
          //
          //  DAL.put(Foo2("a1", "b1"))  // save SK("a" -> "a1", Foo), SK("idx1" -> "b1", FooT) into DAL
          //  val foo2: Foo2 = ...       // load the previous instance form DAL
          //  Foo.idx1.find(foo2.idx1)   // this will not find foo2 since foo2.idx1 == "b1"
          if (rhs.nonEmpty)
            newMods |= FINAL

          val copied = treeCopy.DefDef(dd, newMods, name, tparams, vparamss, tpt, rhs)
          val dda = transformAccelerated(implDef, copied, termName)
          transformReified(implDef, dda, termName)
          dda :: Nil
        case dd: DefDef =>
          if (hasAnnotation(dd.mods, tpnames.elevated)) {
            val anno = if (isEvent) tpnames.event else tpnames.entity
            alarm(OptimusErrors.ELEVATED_WITH_ENTITY_OR_EVENT, dd.pos, anno)
          }
          checkIllegalDefAnnots(dd, implDef)
          if (dd.name == nme.CONSTRUCTOR) {
            val newParamss = dd.vparamss.map {
              _.map { vd: ValDef =>
                val newMods = vd.mods.mapAnnotations(_.map(transformSafe))
                treeCopy.ValDef(vd, newMods, vd.name, vd.tpt, vd.rhs)
              }
            }
            transformImpure(treeCopy.DefDef(dd, dd.mods, dd.name, dd.tparams, newParamss, dd.tpt, dd.rhs)) :: Nil
          } else {
            val dda = transformAccelerated(implDef, dd, termName)
            transformReified(implDef, dda, termName)

            val nt =
              if (hasExposeArgTypesOnAsync(dd.mods)) createNodeTrait(implDef, dd, dd.vparamss, dd.mods.flags).toList
              else Nil

            transformImpure(dda) :: nt
          }

        case t => t :: Nil
      }

      // Create Priql columnar execution support classes.
      if (createImplicitConversion && entityFieldToType.nonEmpty) {
        addToCompanion(
          termName,
          List(
            createNamedColumnResult(typeName, entityFieldToType),
            createConversionModule(typeName, entityFieldToType)))
      }

      // copy all the "<ident>=<expr>" expression from the Ctor parameter list
      def inlineProlog(toPos: Position) = {
        valsFromCtor
          .map(tpl => {
            ValDef(Modifiers(Flags.LOCAL), tpl._2.name, TypeTree(tpl._2.tpe), dupTree(tpl._2.rhs)) setPos toPos
          })
          .toList
      }

      // turn an expression into:
      //   if (optimus.versioning.allowConstructorDefaultValuesInVersioning) <expression>
      //   else PluginSupport.disabledConstructorDefaultValues(<propName>)
      def ifConstructorDefaultValuesAllowed(propName: String, expr: Tree) = {
        val exception = PluginSupport.disabledGeneratedInitMethod(propName)
        If(
          VersioningPkg DOT names.RuntimeConstants DOT names.allowConstructorDefaultValuesInVersioning,
          expr,
          exception)
      }

      val defaultsWithoutInitializer = prop2WithDefValue.filter { prop =>
        val defaultDefName = mkEntPropInitializerName(prop._1)
        // check if we already got a user-defined def that provides a default value
        !impl.body.exists {
          case DocDef(_, DefDef(_, name, _, _, _, _)) =>
            name == defaultDefName // handle scalaDoc generate duplicate defs
          case DefDef(_, name, _, _, _, _) => name == defaultDefName
          case _                           => false
        }
      }

      // lift default values for "val foo = <rhs>" members into special "def foo$init = <rhs>" methods
      def liftedDefaults: List[Tree] =
        defaultsWithoutInitializer.iterator.map { prop =>
          val (vd, rhs) = prop._2
          val pos = vd.pos.focus
          atPos(pos) {
            // Suppress warning on calls to async methods.  See OPTIMUS-3932
            val annot = mkAnnotation(EntersGraphAnnotation, pos)
            DefDef(
              Modifiers(Flags.SYNTHETIC | (vd.mods.flags & Flags.OVERRIDE)) withAnnotations (annot :: Nil),
              mkEntPropInitializerName(prop._1),
              Nil,
              List(Nil),
              dupTree(vd.tpt) setPos NoPosition,
              Block(inlineProlog(pos), ifConstructorDefaultValuesAllowed(prop._1.toString, dupTree(rhs)))
            )
          }
        }.toList

      val isStorable =
        if (!isEvent) !isModDef && classMods.hasAnnotationNamed(tpnames.stored)
        else !(isModDef || classMods.hasAnnotationNamed(tpnames.transient))

      val stats = List.newBuilder[Tree]
      if (isStorable)
        if (!isEntity(classMods)) {
          stats ++= liftedDefaults
        }
      stats ++= newBody

      // Events need apply generated after we rewrite ctor.  But we need to rewrite ctor after adding property stuff to avoid xforming synthetic validTime property
      if (!isModDef && !isEvent && !implDef.mods.hasFlag(ABSTRACT))
        addToCompanion(implDef.name.toTermName, constructorTrees(ctorTrees, implDef.asInstanceOf[ClassDef], false))

      // update template with metadata def, Entity trait & property defaults
      treeCopy.Template(impl, newParents, impl.self, stats.result())
    }

    private def mkEventClass(cd: ClassDef) = {
      val ClassDef(mods, name, tparams, _) = cd
      val newImpl = transformStorableTemplate(isModDef = false, cd)

      val newCtors = List.newBuilder[DefDef]

      val newStats = newImpl.body flatMap {
        case dd @ DefDef(mods, nme.CONSTRUCTOR, tparams, vparamss, tpt, rhs) if !cd.mods.hasFlag(ABSTRACT) =>
          if (vparamss.length != 1)
            alarm(OptimusErrors.EVENT_WITH_MULTI_ARGLIST, dd.pos)
          def acc = atPos(dd.pos.focus) {
            ValDef(Modifiers(PARAMACCESSOR | OVERRIDE), names.validTime, Instant, EmptyTree)
          }
          val paramMods = Modifiers(PARAMACCESSOR | PARAM | DEFAULTPARAM)
          val formal = atPos(dd.pos.focus) {
            ValDef(paramMods, names.validTime, Instant, Select(PlatformPkg, names.storeContext))
          }
          // event constructor "enters graph" because (until optimus_constructors) it may call the default getter
          // for a superclass's validTime parameter (this is rewritten later, and never actually happens)
          val ctorMods =
            mods & (PRIVATE | PROTECTED) withAnnotations List(mkAnnotation(EntersGraphAnnotation, dd.pos.focus))
          val newCtor = treeCopy.DefDef(dd, ctorMods, nme.CONSTRUCTOR, tparams, List(vparamss.head :+ formal), tpt, rhs)
          newCtors += newCtor
          List(acc, newCtor)
        case s => s :: Nil
      }

      val r = treeCopy.ClassDef(
        cd,
        mods &~ INTERFACE,
        name,
        tparams,
        treeCopy.Template(newImpl, newImpl.parents, newImpl.self, newStats))
      if (!mods.hasFlag(ABSTRACT))
        addToCompanion(r.name.toTermName, constructorTrees(newCtors.result(), r, true))
      r
    }

    private def transformAnonymousEntityClass(aTree: Tree): Tree = {
      @tailrec
      def doTransform(tree: Tree, annotatedList: List[Tree]): Tree = tree match {
        case Annotated(anno, anotherAnno @ Annotated(_, _)) => doTransform(anotherAnno, anno :: annotatedList)
        case Annotated(annot, bl @ Block(List(cd @ ClassDef(mods, typeName, tparams, impl)), ap @ Apply(_, args))) =>
          // generate different names for different anonymous entity class, so the apply method of the objects won't conflict
          val newTname = unit.freshTypeName(typeName.toString)
          val expr = atPos(ap.pos) { New(Ident(newTname), args :: Nil) }

          // rewrite the anonymous entity class block into normal block
          val res = treeCopy.Block(
            bl,
            List(treeCopy.ClassDef(cd, mods.withAnnotations(annot :: annotatedList), newTname, tparams, impl)),
            expr)

          transformSafe(res)
        case _ => super.transform(tree)
      }

      doTransform(
        aTree,
        mkAnnotation(TransientAnnotation, aTree.pos.focus) :: Nil
      ) // set the anonymous entity class as transient
    }

    private def assertNoBothIndexedAndKey(tree: ValOrDefDef): Unit = {
      def hasIndexedAnno = hasAnnotation(tree.mods, tpnames.indexed)
      def hasKeyAnno = hasAnnotation(tree.mods, tpnames.key)
      if (hasIndexedAnno && hasKeyAnno) alarm(OptimusErrors.NO_MULTIPLE_INDEX, tree.pos, tree.name)
    }

    /**
     * It annotates a java annotation (no need for scala ones!) with  `@scala.annotation.meta.getter` or
     * `@scala.annotation.meta.field`. This is used in later phases to ensure the annotation is kept on
     * either the getter of a field, or the field itself.
     */
    private def annotateWithMetaAnnos(tree: Tree): Tree = {
      val annotationWithGetter = annotateAnnotation(GetterAnnotation, tpnames.needGetterAnnos, tree)
      annotateAnnotation(FieldAnnotation, tpnames.needFieldAnnos, annotationWithGetter)
    }

    private def annotateAnnotation(annoToAdd: Select, annosToMatch: Set[Name], anno: Tree): Tree =
      anno match {
        case anno @ Apply(fun @ Select(New(tpt @ Ident(name)), nme.CONSTRUCTOR), args) if annosToMatch.contains(name) =>
          val newQualifier = New(Annotated(mkAppliedAnnotation(annoToAdd, Nil), tpt))
          treeCopy.Apply(anno, fun.copy(qualifier = newQualifier), args)
        case anno @ Apply(fun @ Select(New(tpt @ Select(_, name)), nme.CONSTRUCTOR), args)
            if annosToMatch.contains(name) =>
          val newQualifier = New(Annotated(mkAppliedAnnotation(annoToAdd, Nil), tpt))
          treeCopy.Apply(anno, fun.copy(qualifier = newQualifier), args)
        case other => other
      }

    private def repackageAnnotations(mods: Modifiers, isModule: Boolean = true): Modifiers = {
      import OptimusErrors._

      mods.mapAnnotations {
        _.flatMap {
          case ap: Apply if isSpecificAnnotation(ap, tpnames.EntityMetaDataAnnotation) =>
            Nil // dropping it here: it will be re-computed together with @entity
          case entityAnno: Apply if isSpecificAnnotation(entityAnno, tpnames.entity) =>
            val storedAnno = getAnnotation(mods, tpnames.stored)
            val projectedParam = storedAnno.flatMap(getProjectedParam)
            val fullTextSearchParam = storedAnno.flatMap(getFullTextSearchParam)
            val monoTemporalParam = storedAnno.flatMap(getMonoTemporalParam)
            val schemaVersionParam = getSchemaVersionParam(entityAnno)
            val slotNumbParam = {
              if (schemaVersionParam.exists(_ < 0)) alarm(NEG_SCHEMA_VERSION, entityAnno.pos)
              else if (schemaVersionParam.exists(_ > 0)) {
                if (isModule)
                  alarm(ENTITY_OBJ_WITH_SCHEMA_VERSION, entityAnno.pos)
                else if (storedAnno.isEmpty)
                  alarm(TRANSIENT_ENTITY_WITH_SCHEMA_VERSION, entityAnno.pos)
                else if (mods.isTrait || mods.hasFlag(Flag.ABSTRACT))
                  alarm(ABSTRACT_ENTITY_WITH_SCHEMA_VERSION, entityAnno.pos)
              }
              schemaVersionParam
            }

            val entityMetaDataAnno = {
              import entityMetaDataAnnotationNames._
              val entityMetaDataArgs: List[Tree] =
                mkOptionalNamedArgTree(slotNumber, slotNumbParam).toList ++
                  mkConditionalNamedArgTree(explicitSlotNumber, schemaVersionParam.isDefined) ++
                  mkConditionalNamedArgTree(isStorable, storedAnno.isDefined) ++
                  mkConditionalNamedArgTree(isTrait, mods.isTrait) ++
                  mkConditionalNamedArgTree(isObject, isModule) ++
                  mkOptionalNamedArgTree(projected, projectedParam) ++
                  mkOptionalNamedArgTree(fullTextSearch, fullTextSearchParam) ++
                  mkOptionalNamedArgTree(monoTemporal, monoTemporalParam)
              mkAppliedAnnotation(EntityMetaDataAnnotation, entityMetaDataArgs)
            }

            entityAnno :: entityMetaDataAnno :: Nil
          case storedAnno @ Apply(_, args) if isSpecificAnnotation(storedAnno, tpnames.stored) =>
            val projectedParam = getProjectedParam(storedAnno)
            val fullTextSearchParam = getFullTextSearchParam(storedAnno)
            val monoTemporalParam = getMonoTemporalParam(storedAnno)
            if (args.nonEmpty && projectedParam.isEmpty && fullTextSearchParam.isEmpty && monoTemporalParam.isEmpty)
              alarm(OptimusErrors.STORED_ENTITY_WITH_ARGUMENT, storedAnno.pos)

            storedAnno :: Nil
          case anno => anno :: Nil
        }
      }
    }

    private def addEmbeddableMetaData(mods: Modifiers, isModule: Boolean): Modifiers = {
      import embeddableMetaDataAnnotationNames._
      val embeddableMetaDataArgs: List[Tree] =
        mkConditionalNamedArgTree(isTrait, mods.isTrait).toList ++
          mkConditionalNamedArgTree(isObject, isModule) ++
          mkConditionalNamedArgTree(projected, isEmbeddableProjected(mods))
      val embeddableInfoAnno = mkAppliedAnnotation(EmbeddableMetaDataAnnotation, embeddableMetaDataArgs)
      mods withAnnotations (embeddableInfoAnno :: Nil)
    }

    // Extractor for Apply and TypeApply. Applied.unapply matches any tree, not just applications
    private object Application {
      def unapply(t: GenericApply): Some[(Tree, List[Tree], List[List[Tree]])] = {
        val applied = treeInfo.dissectApplied(t)
        Some((applied.core, applied.targs, applied.argss))
      }
    }

    private object AddNowarnAttachment
    private var defStack: List[MemberDef] = Nil
    private val is213 = if (scalaVersionRange("2.13:"): @staged) true else false

    // Note that this is going to be a recursive descent, so we're going to see a number of calls to this function.
    private def transformSafe(tree: Tree): Tree = tree match {
      case dt: MemberDef if is213 && !defStack.headOption.contains(dt) =>
        // When encountering a call to --> (UI event binding), mark the enclosing method @nowarn("msg=Auto-application")
        // The following pattern is common:
        //   @handle def openWebView(arg: Int)(): HandlerResult = { ... }
        //   gui { Button(...) --> openWebView(x) }
        defStack ::= dt
        val r = transformSafe(dt)
        defStack = defStack.tail
        if (dt.getAndRemoveAttachment[AddNowarnAttachment.type].isEmpty) r
        else {
          val Annotated(ann, _) =
            tq"""Int @_root_.scala.annotation.nowarn("msg=Auto-application to `\\(\\)` is deprecated")"""
          val nowarn = atPos(tree.pos.focusStart)(ann)
          r match {
            case cd: ClassDef =>
              treeCopy.ClassDef(
                cd,
                cd.mods.copy(annotations = nowarn :: cd.mods.annotations),
                cd.name,
                cd.tparams,
                cd.impl)
            case md: ModuleDef =>
              treeCopy.ModuleDef(md, md.mods.copy(annotations = nowarn :: md.mods.annotations), md.name, md.impl)
            case dd: DefDef =>
              treeCopy.DefDef(
                dd,
                dd.mods.copy(annotations = nowarn :: dd.mods.annotations),
                dd.name,
                dd.tparams,
                dd.vparamss,
                dd.tpt,
                dd.rhs)
            case vd: ValDef =>
              treeCopy.ValDef(vd, vd.mods.copy(annotations = nowarn :: vd.mods.annotations), vd.name, vd.tpt, vd.rhs)
            case t => t
          }
        }

      case Application(Select(_, fun), _, _) if is213 && fun.decoded == "-->" =>
        defStack.head.updateAttachment(AddNowarnAttachment)
        super.transform(tree)

      case pd: PackageDef =>
        val (newP, props) = packageName.enterNewContext(pd.pid.toString.replace(".", "$")) {
          inScope(pd)
        }

        treeCopy.PackageDef(newP, newP.pid, addPropsToModuleDef(newP.stats, props))
      case tree: AppliedTypeTree =>
        whileInAppliedTypeTree {
          val newTree = atPos(tree.pos) { transformToNodeFunction(tree) }
          super.transform(newTree)
        }
      case annotated: Annotated =>
        atPos(tree.pos) {
          alertIfNodeAnnotated(annotated)
          transformAnonymousEntityClass(annotated)
        }
      case b: Block =>
        val (base, props) = inScope(b, isLocal = true)
        val stmts = addPropsToModuleDef(base.stats, props)
        treeCopy.Block(base, stmts, base.expr)
      case md @ ModuleDef(mods, name, _) =>
        atNode(mods, isModuleDef = true) {
          val hasEntityAnno = isEntity(mods)
          if (isEvent(mods)) alarm(OptimusErrors.EVENT_WITH_OBJECT, tree.pos)
          if (hasEntityAnno) {
            if (mods.hasAnnotationNamed(tpnames.transient))
              alarm(OptimusErrors.ENTITY_WITH_TRANSIENT, tree.pos)
          }

          val t1 =
            if (hasEntityAnno)
              treeCopy.ModuleDef(tree, mods, name, transformStorableTemplate(isModDef = true, md))
            else
              treeCopy.ModuleDef(tree, mods, name, transformRawNodeTemplate(mods, name, md))
          // ClassDefs contained within a ModuleDef may be entities, so we need to create their companion objects.
          val (t2, props) = inScope(t1)

          val (newMods, newParents) = if (hasAnnotation(mods, tpnames.embeddable)) {
            if (hasEntityAnno) alarm(OptimusErrors.EMBEDDABLE_AND_ENTITY, tree.pos)
            if (!mods.hasFlag(Flag.CASE)) alarm(OptimusErrors.EMBEDDABLE_ONLY_WITH_CASE_OBJECT, tree.pos)
            (addEmbeddableMetaData(mods, isModule = true), t2.impl.parents :+ EmbeddableType)
          } else {
            (repackageAnnotations(mods), t2.impl.parents)
          }
          val newModsL = addLoomAnnotationIfNeeded(newMods, t2.pos, hasEntityAnno)
          val newBody = addPropsToModuleDef(t2.impl.body, props)
          val newTemplate = treeCopy.Template(t2.impl, newParents, t2.impl.self, newBody)
          treeCopy.ModuleDef(t2, newModsL, t2.name, newTemplate)
        }
      case cd @ ClassDef(mods, name, tparams, _) if isEntity(mods) =>
        atNode(mods, isModuleDef = false) {
          // Processing regular @entity class Foo { ... }
          if (mods.hasAnnotationNamed(tpnames.transient))
            alarm(OptimusErrors.ENTITY_WITH_TRANSIENT, cd.pos)

          if (mods.isCase)
            alarm(OptimusErrors.ENTITY_WITH_CASE_CLASS, cd.pos)
          if (scopeStack.top.isValOrDefDef)
            alarm(OptimusErrors.METHOD_LOCAL_ENTITY, cd.pos)
          val storedDefTransTree = treeCopy.ClassDef(tree, mods, name, tparams, transformBackedDef(cd.mods, cd.impl))
          val t1Template = transformStorableTemplate(isModDef = false, storedDefTransTree)
          val t1 = treeCopy.ClassDef(tree, mods, name, tparams, t1Template)
          val (t2, props) = inScope(t1)

          val newMods = repackageAnnotations(t2.mods &~ INTERFACE, isModule = false)
          val newModsL = addLoomAnnotationIfNeeded(newMods, t2.pos, isEntity = true)
          val newBody = addPropsToModuleDef(t2.impl.body, props)
          val newTemplate = treeCopy.Template(t2.impl, t2.impl.parents, t2.impl.self, newBody)
          treeCopy.ClassDef(t2, newModsL, t2.name, t2.tparams, newTemplate)
        }
      case cd @ ClassDef(mods, name, _, _) if isEvent(mods) =>
        if (isEventContained(mods)) {
          if (mods.isTrait || mods.hasFlag(Flag.ABSTRACT))
            alarm(OptimusErrors.ABSTRACT_CONTAINED_EVENT, cd.pos)
        }
        if (mods.isCase)
          alarm(OptimusErrors.EVENT_WITH_CASE_CLASS, cd.pos)
        if (isTransient(mods))
          alarm(OptimusErrors.EVENT_WITH_TRANSIENT, cd.pos)
        if (scopeStack.top.isValOrDefDef)
          alarm(OptimusErrors.METHOD_LOCAL_EVENT, cd.pos)
        val newTree = mkEventClass(cd)
        val projected = isEventProjected(mods)
        val metaProj = DefDef(Modifiers(FINAL), names.projected, Nil, Nil, TypeTree(), LIT(projected))
        addToCompanion(name.toTermName, metaProj :: Nil)
        val (t2, props) = inScope(newTree)
        treeCopy.ClassDef(
          t2,
          t2.mods &~ INTERFACE,
          t2.name,
          t2.tparams,
          treeCopy.Template(t2.impl, t2.impl.parents, t2.impl.self, addPropsToModuleDef(t2.impl.body, props))
        )
      case cd @ ClassDef(mods, name, tparams, _) if hasAnnotation(mods, tpnames.embeddable) =>
        if (!mods.isCase && !mods.isTrait) {
          alarm(OptimusErrors.EMBEDDABLE_ONLY_WITH_CASE_CLASS, cd.pos)
          tree
        } else {
          val newTree = treeCopy.ClassDef(tree, mods, name, tparams, transformRawNodeTemplate(mods, name, cd))
          ensureCompanion(name.toTermName, false /*isEntity*/, false, false, true, cd)
          val projected = isEmbeddableProjected(mods)
          if (projected && !mods.isTrait) {
            val members = mkProjectedMembers(cd)
            if (members.isEmpty) alarm(OptimusErrors.PROJECTED_FIELD_MISSING, cd.pos)
            else {
              addToCompanion(name.toTermName, members)
              val rhs = mkList(members.collect { case d: ValDef => Select(This(tpnme.EMPTY), d.name) })
              val membersVal = ValDef(Modifiers(FINAL | OVERRIDE), names.projectedMembers, TypeTree(), rhs)
              addToCompanion(name.toTermName, membersVal :: Nil)
            }
          }
          newTree.impl.body.collect { case d: DefDef =>
            transformReified(cd, d, name.toTermName)
            d
          }
          val (t2, props) = inScope(newTree)
          val newParents = t2.impl.parents :+ EmbeddableType
          treeCopy.ClassDef(
            t2,
            addEmbeddableMetaData(t2.mods, cd.symbol.isModuleOrModuleClass),
            t2.name,
            t2.tparams,
            treeCopy.Template(t2.impl, newParents, t2.impl.self, addPropsToModuleDef(t2.impl.body, props))
          )
        }
      case cd @ ClassDef(mods, name, tparams, _) =>
        val newTree = treeCopy.ClassDef(tree, mods, name, tparams, transformRawNodeTemplate(mods, name, cd))
        val (t2, props) = inScope(newTree)
        treeCopy.ClassDef(
          t2,
          t2.mods,
          t2.name,
          t2.tparams,
          treeCopy.Template(t2.impl, t2.impl.parents, t2.impl.self, addPropsToModuleDef(t2.impl.body, props)))
      case anno @ Apply(Select(New(_), nme.CONSTRUCTOR), _) => annotateWithMetaAnnos(anno)
      case _                                                => super.transform(tree)
    }

    private def addLoomAnnotationIfNeeded(mods: Modifiers, pos: Position, isEntity: Boolean): Modifiers = {
      if (isEntity && plugin.settings.loom && !hasAnyAnnotation(mods, Seq(tpnames.loom, tpnames.loomOff)))
        mods.withAnnotations(mkAnnotation(LoomAnnotation, pos) :: Nil)
      else mods
    }

    private def mkProjectedMembers(cd: ClassDef): List[Tree] = {
      val ctorArgs = cd.impl.body.collect { case dd: DefDef if dd.name == nme.CONSTRUCTOR => dd }.head.vparamss.flatten
      ctorArgs.map(a =>
        genProjectedMember(cd, cd.pos.focus, a.name, isProjectionIndexed(a.mods), isProjectionQueryable(a.mods)))
    }

    private def genProjectedMember(
        implDef: ImplDef,
        pos: Position,
        name: TermName,
        indexed: Boolean,
        queryable: Boolean): Tree = {
      atPos(pos) {
        val valarg = ValDef(Modifiers(SYNTHETIC | FINAL), names.arg, mkETypeOf(implDef), EmptyTree)
        val entityFun = Function(List(valarg), Select(Ident(valarg.name), name))
        val rhs = Apply(
          PluginSupport.genProjectedMember,
          LIT(name.toString) :: LIT(indexed) :: LIT(queryable) :: entityFun :: Nil)
        ValDef(Modifiers(SYNTHETIC | FINAL), TermName("" + name + suffixes.MEMBER_DESC), TypeTree(), rhs)
      }
    }

    private def transformToNodeFunction(tree: Tree): Tree = tree match {
      case appliedType @ AppliedTypeTree(_, args) if isScalaFunction(appliedType) =>
        // e.g., A => B @node
        // scala.Function has at least one type param! so we know that args is nonEmpty
        val idx = args.size - 1
        val (paramTypes, returnType :: Nil) = args.splitAt(idx)
        val newParamTypes = paramTypes.map { paramType =>
          // only the return type (i.e.: the last arg) can have the @node type annotation!
          alertIfNodeAnnotated(paramType)
        }
        returnType match {
          case Annotated(annot, tpt) if isSpecificAnnotation(annot, tpnames.node) =>
            val nodeFunction = Select(PlatformPkg, newTypeName(s"NodeFunction$idx"))
            AppliedTypeTree(nodeFunction, newParamTypes :+ tpt)
          case _ => appliedType.copy(args = newParamTypes :+ returnType)
        }
      case other => other
    }

    private var withinAppliedType: Boolean = false
    private def whileInAppliedTypeTree(f: => Tree): Tree = {
      withinAppliedType = true
      val tree = f
      withinAppliedType = false
      tree
    }

    private def alertIfNodeAnnotated(tree: Tree): Tree = tree match {
      case Annotated(annot, tpe) if isSpecificAnnotation(annot, tpnames.node) =>
        if (withinAppliedType) alarm(OptimusErrors.INVALID_TYPE_ANNOTATION, tpe.pos, tree)
        else alarm(OptimusErrors.INVALID_TYPE_ANNOTATION_WITH_HINT, tpe.pos, tree, s"\'() => $tpe @node\'")
        tpe
      case other => other // all good!
    }
  }
}
