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
package optimus.entityplugin.pretyper

import optimus.tools.scalacplugins.entity.AdjustASTComponent
import optimus.tools.scalacplugins.entity.reporter.{OptimusErrors, OptimusNonErrorMessages}

import scala.reflect.internal.ModifierFlags
import scala.tools.nsc.symtab.Flags

trait CompanionMethodsGenerator { this: AdjustASTComponent =>
  import global._

  class KeyGenerator(implDef: ImplDef, isEvent: Boolean) {
    private[this] var hasKey = false
    private[this] var hasPrimary = false

    private[this] def checkKey(kt: ValOrDefDef) = {
      if (!isEvent && !hasAnnotation(implDef.mods, tpnames.stored)) {
        alarm(OptimusErrors.NON_STORED_KEY_INDEX, kt.pos)
      }
      if (isKey(kt.mods)) {
        if (hasPrimary)
          alarm(OptimusErrors.MULTI_PRIMARY_KEY, kt.pos, if (isEvent) "Event" else "Entity", implDef.name)
        hasPrimary = true
      }
      if (isEvent && isIndex(kt.mods)) {
        if (isUniqueIndex(kt.mods))
          alarm(OptimusErrors.UNIQUE_INDEX_OF_EVENT, kt.pos, implDef.name)
        if (kt.mods.hasFlag(ModifierFlags.PRIVATE))
          alarm(OptimusErrors.NO_PRIVATE_EVENT_INDEX, kt.pos)
      }

      if (kt.mods.hasFlag(ModifierFlags.OVERRIDE))
        alarm(OptimusErrors.NO_INDEX_OVERRIDE, kt.pos)

      hasKey = true
    }

    def companionKeyTrees(vodd: ValOrDefDef): List[Tree] = {
      checkKey(vodd)
      keyGenerator(implDef, vodd) map { _.keyTrees } getOrElse Nil flatMap transformRawNodeDef
    }
  }

  private def keyGenerator(implDef: ImplDef, vodd: ValOrDefDef) = {
    implDef match {
      case _: ModuleDef =>
        alarm(OptimusErrors.KEY_INDEX_IN_OBJ_ENTITY, vodd.pos)
        None
      case cd: ClassDef =>
        vodd match {
          case vd @ ValDef(mods, name, tpt, rhs) =>
            rhs match {
              case Apply(fun, args) if fun.toString.startsWith("scala.Tuple") =>
                alarm(OptimusNonErrorMessages.COMPOUND_KEY_MUST_DEF, vd.pos)
              case _ =>
            }
            val gen = new KeyGeneratorImpl(cd, vd.name, Ident(vd.name) :: Nil, vd)
            Some(gen)
          case dd @ DefDef(mods, name, tparams, vparamss, tpt, rhs) =>
            def make(newRhs: List[Tree]) =
              if (vparamss.length > 0) {
                alarm(OptimusErrors.KEY_INDEX_MUST_PARAMLESS_DEF, dd.pos)
                None
              } else {
                Some(new KeyGeneratorImpl(cd, name, newRhs, dd))
              }

            rhs match {
              case s @ Select(_, _)                                           => make(s :: Nil)
              case Literal(Constant(()))                                      => make(Nil)
              case id @ Ident(_)                                              => make(List(id))
              case Apply(fun, args) if fun.toString.startsWith("scala.Tuple") => make(args)
              case a @ Apply(Select(_, n), _) if n.toString == "map"          => make(a :: Nil)
              case e @ EmptyTree                                              => make(e :: Nil)
              case _ =>
                alarm(OptimusErrors.CANT_GENERATE_INDEX, dd.pos, dd)
                None
            }
        }
    }
  }

  /**
   * A methods generate synthesis methods according to constructors
   *
   * @param ctors
   *   The list of constructors of a class, whose head should be primary constructor
   * @param cls
   *   ClassDefinition itself
   * @param isEvent
   *   whether the class definition is an @event
   * @param isRegularClass
   *   whether the class definition is an entity class
   *
   * For entities: We generate apply(), uniqueInstance() and their default getter for each of their constructors
   *
   * For non-entities: We only generate default getter for all constructors in case their rhs is an async call.
   * OPTIMUS-12927
   *
   * In addition, for case classes since scala compiler generates apply method for primary constructor, so we need
   * generate default getter for the "companion" apply method of primary constructor and put it into companion object
   *
   * The reason we don't separate this into two different methods for regular class and entity classes is: We want take
   * advantage of mkMethod logic to generate intermediate "ctorWithTypeParameter" and "applyMethod" We won't add them
   * into companion object, but we use them to generate the default getter methods which will be added into companion
   * object.
   */
  def constructorTrees(ctors: List[DefDef], cls: ClassDef, isEvent: Boolean, isRegularClass: Boolean = false) =
    ctors flatMap { ctor =>
      import Flags._

      val isEmbeddableOrStable = hasAnnotation(cls.mods, tpnames.embeddable) || hasAnnotation(cls.mods, tpnames.stable)

      def mods = {
        // don't mark embeddable apply as SYNTHETIC (even though it is) because of [1] infra
        val newFlags = (ctor.mods & AccessFlags) | (if (!isEmbeddableOrStable) SYNTHETIC else 0)
        addAutoGenCreationAnnotationToMods(newFlags, ctor.pos.focus)
      }

      def mkMethod(name: TermName, mods: Modifiers, xform: Tree => Tree) = {
        val rhs = {
          val res = New(
            typedRef(cls.name, cls.tparams),
            ctor.vparamss map {
              _ map { p =>
                Ident(p.name)
              }
            })
          if (!isEvent)
            xform(res)
          else
            res
        }
        atPos(ctor.pos.focus) {
          val tdefs = dupTypeDefsAsInvariant(cls.tparams)
          val vparamss = ctor.vparamss map { vps =>
            vps map { vp =>
              ValDef(
                Modifiers(vp.mods.flags & (DEFAULTPARAM | PARAM | IMPLICIT)),
                vp.name,
                dupTree(vp.tpt),
                dupTree(vp.rhs))
            }
          }
          DefDef(mods, name, tdefs, vparamss, typedRef(cls.name, cls.tparams), rhs)
        }
      }

      def applyMethod = mkMethod(nme.apply, mods, (t: Tree) => t)
      def uniqueInstanceMethod =
        mkMethod(
          names.uniqueInstance,
          mods withAnnotations List(mkAnnotation(ImpureAnnotation, ctor.pos.focus)),
          (t: Tree) => transToUniqueUniverse(t))

      val isConstructorNode = hasAnnotation(ctor.mods, tpnames.node)

      if (isConstructorNode) {
        if (!isEmbeddableOrStable)
          alarm(OptimusErrors.ONLY_ALLOW_NODE_FOR_EMBEDDABLE_CTOR, ctor.pos, ctor)
        if (ctors.exists(one => !hasAnnotation(one.mods, tpnames.node)))
          alarm(OptimusErrors.ALL_EMBEDDABLE_CTOR_MUST_BE_NODE_IF_ONE_IS_NODE, ctor.pos, ctor)
      }
      if (isRegularClass) {
        if (cls.mods.isCase && ctor == ctors.head) {
          // [1] if the case class constructor is private, generate the apply method (as private too) so that scalac doesn't
          // autogenerate one (that is annoyingly always public)
          if (!ctor.mods.isPublic && isEmbeddableOrStable) {
            // unfortunately as of 2.11.11 scalac always generates the default args for apply, so if the ctor has
            // default args we can't do this trick
            // TODO (OPTIMUS-17433): Remove this limitation when scalac supports it
            if (ctor.vparamss.exists(_.exists(_.rhs != EmptyTree)))
              alarm(OptimusErrors.EMBEDDABLE_PRIVATE_CTOR_DEFAULTS, ctor.pos, ctor)

            applyMethod :: Nil
          } else Nil
        } else Nil
      } else if (isEvent) {
        uniqueInstanceMethod :: Nil
      } else
        applyMethod :: uniqueInstanceMethod :: Nil
    }

  private class KeyGeneratorImpl(clsDef: ClassDef, name: TermName, args: List[Tree], keyTree: ValOrDefDef) {
    val entity = names.arg

    val pos = keyTree.pos.focus
    val isUnique = isKey(keyTree.mods) || isUniqueIndex(keyTree.mods)
    val isIndexed = isUniqueIndex(keyTree.mods) || isIndex(keyTree.mods)
    val isC2P = isStoredC2P(keyTree.mods)
    val isSecondary = isIndex(keyTree.mods)
    val isEventClass = isEvent(clsDef.mods)
    val isQueryable = isQueryableIndex(keyTree.mods) || isKey(keyTree.mods)
    val isQueryableByErefOnly = isQueryableByRefOnly(keyTree.mods)
    var isSelectLiteral = false

    import CODE._

    import Flags._

    // genIndexXXX overloads take just String not Seq[String] for singleton indexes, so conditionally make a list if needed.
    def listify(names: List[Name]): Tree = names match {
      case Nil       => LIT(())
      case hd :: Nil => LIT(hd.toString)
      case _ =>
        mkList(names map { n =>
          LIT(n.toString)
        })
    }

    private def expr(t: Tree): Tree = t match {
      case Ident(name)        => Select(mkCast(LIT(null), mkUBoundTypeOf(clsDef)), name)
      case Select(qual, name) => Select(expr(qual), name)
      case Apply(func, args)  => Apply(expr(func), args)
    }

    def mkOne(arg: Tree): (TermName, Tree) = arg match {
      case EmptyTree =>
        if (isUnique || !isIndexed)
          alarm(OptimusErrors.KEY_CANNOT_BE_ABSTRACT, arg.pos)
        (name, PluginMacros.findPickler(mkCast(LIT(null), keyTree.tpt)))
      case Ident(propName: TermName) =>
        (propName, Select(Select(This(tpnme.EMPTY), propName), names.pickler))
      case Select(_: Super | _: This, propName: TermName) =>
        (propName, Select(Select(This(tpnme.EMPTY), propName), names.pickler))
      case Select(qual, name2) =>
        isSelectLiteral = true
        (name, PluginMacros.findPickler(expr(arg).duplicate))
      case a: Apply => (name -> PluginMacros.findPickler(expr(a).duplicate))
      case x: Tree =>
        alarm(OptimusErrors.UNSUPPORTED_KEY_INDEX_SPEC, x.pos, x)
        (nme.EMPTY, EmptyTree)
    }

    val (propNames, picklers) = args map mkOne unzip

    val entityFun = atPos(pos) {
      val valarg = ValDef(NoMods, entity, mkETypeOf(clsDef), EmptyTree)
      Function(List(valarg), Select(Ident(valarg.name), name))
    }

    val singlePropertyBasedKey = args.size == 1 && !isSelectLiteral && !keyTree.isInstanceOf[DefDef]
    val keyInfoName = if (singlePropertyBasedKey) mkKeyInfoName(name) else name.toTermName
    val generator =
      if (isEventClass) {
        if (isUnique)
          PluginSupport.genEventKeyInfo
        else if (isQueryable)
          PluginSupport.genQueryableEventIndexInfo
        else
          PluginSupport.genEventIndexInfo
      } else {
        if (isUnique)
          if (isQueryable) PluginSupport.genQueryableEntityKeyInfo else PluginSupport.genEntityKeyInfo
        else if (isQueryable)
          if (isQueryableByErefOnly)
            PluginSupport.genQueryableIndexInfoWithErefFilter
          else
            PluginSupport.genQueryableIndexInfo
        else PluginSupport.genIndexInfo
      }

    // Gen val foo$key = ...
    def keyInfo = atPos(pos) {
      val common: List[Tree] = PluginMacros.getIsCollection(clsDef, name) :: mkClassOf(clsDef) :: Nil
      val base: List[Tree] = if (isUnique) (LIT(isIndexed) :: LIT(!isSecondary) :: common) else common
      val rhs =
        if (propNames.nonEmpty && propNames.tail.isEmpty)
          if (keyTree.isInstanceOf[ValDef])
            Apply(generator, LIT(name.toString) :: picklers.head.asInstanceOf[Select].qualifier :: base)
          else
            Apply(generator, LIT(name.toString) :: listify(propNames) :: entityFun :: picklers.head :: base)
        else
          Apply(generator, LIT(name.toString) :: listify(propNames) :: entityFun :: mkList(picklers) :: base)

      ValDef(Modifiers(Flags.LAZY | Flags.SYNTHETIC), keyInfoName, TypeTree(), rhs)
    }
    // take types from property members e.g. this.<propName>.PropType
    def typeNames = propNames.map { nm =>
      Select(Select(This(clsDef.name), nm), tpnames.PropType)
    }

    // Note: This generates argument ValDefs with names identical to some members of this
    // companion object.  So be aware of scoping issues when using these.
    def freshKeyArgs = map3(args, propNames, typeNames) { (arg, name, tpe) =>
      ValDef(NoMods, name, tpe, EmptyTree)
    }

    def mkFlags = SYNTHETIC | (keyTree.mods.flags & (PRIVATE))

    def keyImplType = if (isUnique) tpnames.UniqueKeyImpl else tpnames.NonUniqueKeyImpl

    def genGetter(func: Select, getterName: TermName): List[DefDef] = genTypedGetter(func, getterName, null)
    def genTypedGetter(func: Select, getterName: TermName, rtpe: Tree): List[DefDef] = {
      val funcName = if (rtpe ne null) TermName("" + getterName + "Typed") else getterName
      val rtpeQ = if (rtpe ne null) AppliedTypeTree(NodeType, List(rtpe)) else null
      List(
        genTypedGetter(func, funcName, syncAnnotation = true, rtpe),
        genTypedGetter(
          func,
          mkGetNodeName(funcName),
          syncAnnotation = false,
          rtpeQ,
          wrapper = PlatformPkgObj.queuedNodeOf)
      )
    }

    def genTypedGetter(
        func: Tree,
        funcName: TermName,
        syncAnnotation: Boolean,
        rtpe: Tree,
        wrapper: Tree => Tree = identity): DefDef = {
      val tpe = Select(Select(This(tpnme.EMPTY), keyInfoName), keyImplType)
      val args = freshKeyArgs
      val params =
        if (args.isEmpty) Literal(Constant(())) :: Nil
        else
          args map { ka =>
            Ident(ka.name)
          }
      val rhs = {
        val noCastRhs = wrapper {
          Apply(func, New(tpe, params :: Nil) :: Nil)
        }
        if (rtpe ne null) mkCast(noCastRhs, rtpe) else noCastRhs
      }
      val annos = if (syncAnnotation) mkAnnotation(NodeSyncAnnotationType, func.pos) :: Nil else Nil
      val tpeArg = if (rtpe ne null) dupTypeDefsAsInvariant(clsDef.tparams) else Nil
      atPos(pos) { DefDef(Modifiers(mkFlags) withAnnotations annos, funcName, tpeArg, List(args), TypeTree(), rhs) }
    }

    def genMakeKey = {
      val tpe = Select(Select(This(tpnme.EMPTY), keyInfoName), keyImplType)
      val args = freshKeyArgs
      val params =
        if (args.isEmpty) Literal(Constant(())) :: Nil
        else
          args map { ka =>
            Ident(ka.name)
          }
      val rhs = New(tpe) DOT nme.CONSTRUCTOR APPLY (params)
      // val keyTpe = applyType(KeyType, mkETypeOf(clsDef))
      atPos(pos) { DefDef(Modifiers(mkFlags), names.makeKey, Nil, args :: Nil, TypeTree() /*keyTpe*/, rhs) }
    }

    def genInheritPropertyInfo = {
      val clsProps = clsDef.impl.body collect { case vd: ValDef =>
        vd.name
      }
      val inheritPropNames = args.collect {
        case Ident(propName) if !clsProps.contains(propName) => propName // use inherited property directly
        case Select(_: This | _: Super, propName) if !clsProps.contains(propName) => propName // this.foo, super.bar
      }

      inheritPropNames map { name =>
        ValDef(
          Modifiers(PARAM | SYNTHETIC | PRIVATE | LOCAL),
          name.toTermName,
          TypeTree(),
          PluginMacros.findInheritPropertyInfoForCompoundKey(clsDef, name.toTermName))
      }
    }

    def genericPrimaryKeyInfo =
      if (isEventClass) Nil
      else
        {
          ValDef(Modifiers(LAZY | SYNTHETIC | OVERRIDE | FINAL), names.KeyInfo, TypeTree(), Ident(keyInfoName))
        } :: Nil

    // Gen getByKey
    def getter(rename: Boolean = true): List[DefDef] = {
      val getterName = if (isSecondary && rename) mkGetByName(name) else nme.get
      val func = if (isEventClass) PluginSupport.getEvent else PluginSupport.getEntity

      if (clsDef.tparams.isEmpty) genGetter(func, getterName)
      else {
        val rtpe = typedRef(clsDef.name, clsDef.tparams)
        genGetter(func, getterName) ::: genTypedGetter(func, getterName, rtpe)
      }
    }

    // Gen getOptionByKey
    def getterOption(rename: Boolean = true): List[DefDef] = {
      val getterName = if (isSecondary && rename) mkGetOptionByName(name) else names.getOption
      val func = if (isEventClass) PluginSupport.getEventOption else PluginSupport.getEntityOption

      if (clsDef.tparams.isEmpty) genGetter(func, getterName)
      else {
        val rtpe = AppliedTypeTree(OptionType, typedRef(clsDef.name, clsDef.tparams) :: Nil)
        genGetter(func, getterName) ::: genTypedGetter(func, getterName, rtpe)
      }
    }

    def keyTrees: List[Tree] = {
      if (isUnique && isSecondary)
        keyInfo :: genInheritPropertyInfo
      else if (isUnique)
        keyInfo :: genMakeKey :: (genericPrimaryKeyInfo ::: getter() ::: getterOption() ::: genInheritPropertyInfo)
      else
        keyInfo :: genInheritPropertyInfo
    }
  }
}
