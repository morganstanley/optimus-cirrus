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
import CollectionUtils._
import Attachment._
import optimus.tools.scalacplugins.entity.Attachment.NodeFieldType._

import scala.tools.nsc._
import scala.tools.nsc.plugins.PluginComponent
import scala.tools.nsc.symtab.Flags._
import scala.tools.nsc.transform._

/*
 * 1) Handles stored properties (i.e. non-transient ValDefs) on entities:
 *
 *  - for all stored vals, create a val foo$impl field to hold the current value
 *  - for non-@node stored vals, rewrite the accessor def to load from
 *    the dal (if lazy loaded) / from the $impl (if not)
 *  - for @node stored vals, rewrite the foo$newNode function to create a node which loads from
 *    the dal (if lazy loaded) / from the $impl (if not)
 *  - for overiden stored vals(and transient entity's @node property): depending on the way it overriden and whether its used to extends super class directly,
 *    case 1:
 *    @stored @entity Foo(val ent: Int) {}
 *    @stored @entity Bar extends Foo(420) {
 *        override val ent: Int = 42
 *        def ent$init() = 42
 *    }
 *    ---
 *    We generate "val ent$impl" and corresponding accessor method in both super and sub class.
 *    Strictly speaking, it's a var, but we only write to using Unsafe puts.
 *
 *    case 2:
 *    @stored @entity Foo(val ent: Int) {}
 *    @stored @entity Bar(override val ent: Int) extends Foo(ent + 1) {}
 *    or
 *    @stored @entity Bar(override val ent: Int) extends Foo(42) {}
 *    ---
 *    We generate "val ent$impl" and corresponding accessor method in both super and sub class.
 *    We re-arrage order of initialization in Bar#<init> to make assignment to Bar.this.ent$impl happen before
 *    call its super class' constructor:
 *    def <init>(ent: Int) {
 *      Bar.this.ent$impl = ent
 *      Bar.super.<init>(ent + 1)
 *    }
 *
 *
 * 2) Add argsHash and argsEquals methods to the entity based on the vals
 *
 *    - also creates foo$4eq methods for private vals so that q
 *
 * 3) Rewrites "@node def foo = ?" as "def foo = EvaluationContext.getNode(foo$newNode(X)).get". We have to do that rewrite post-typer because
 *    if we do it pre-typer then it could conflict with the DefDef accessor for a ValDef. (TODO (OPTIMUS-0000): but we don't have to do it in this phase - could be
 *    done in a new phase)
 *    TODO (OPTIMUS-0000): Can we remove (3) now we're doing everything post-typer?
 */
class RedirectAccessorsComponent(val plugin: EntityPlugin, val phaseInfo: OptimusPhaseInfo)
    extends EntityPluginComponent
    with Transform
    with TransformGrep
    with TypingTransformers
    with TreeDuplicator
    with TypedUtils
    with ast.TreeDSL
    with WithOptimusPhase {
  import global._

  override protected def newTransformer0(unit: CompilationUnit) = new RedirectAccessors(unit)

  class RedirectAccessors(unit: CompilationUnit) extends TypingTransformer(unit) {
    import global.definitions._
    import CODE._

    override def transform(tree: Tree): Tree = {
      try {
        transformSafe(tree)
      } catch {
        case ex: Throwable =>
          ex.printStackTrace()
          alarm(OptimusErrors.TRANSFORM_ERROR3, tree.pos, ex)
          null
      }
    }

    private[this] def transformSafe(tree: Tree): Tree = tree match {
      /*
       * Add foo$impl property vars on Entity classes.  These vars hold the "Ground truth" values
       * of properties from the Depot or as provided during construction of temporary entities.
       */
      case cd: ClassDef if isEntity(cd.symbol) =>
        super.transform(
          deriveClassDef(cd)(deriveTemplate(_) { body =>
            // Generate static ctor to init VarHandles for valSyms:
            val withLookup = generateLookupForVarHandles(cd, body)
            val withStatics = generateStatics(cd, withLookup)
            val newBody = rewriteWithValBackingFields(cd, withStatics)
            // We generate (if needs to be) argsHash and argsEquals methods
            // We also generate private (as opposed to private[this]) accessors for private[this] ValDef so
            // that we can access them in equals method!
            val argsMethods = generateArgsMethods(cd, newBody)
            newBody ::: argsMethods
          })
        )

      /*
       * Add foo$impl property vars on Entity objects.  These vars hold the "Ground truth" values
       * of properties from the Depot or as provided during construction of temporary entities.
       */
      case md: ModuleDef if isEntity(md.symbol) =>
        super.transform(
          deriveModuleDef(md)(deriveTemplate(_) { body =>
            val newBody = rewriteWithValBackingFields(md, body)
            val outerHashAndEquals = mkArgsHashAndEquals(md, args = Nil)
            outerHashAndEquals ::: newBody
          })
        )

      /*
       * Add foo$impl property vars on Entity objects.  These vars hold the "Ground truth" values
       * of properties from the Depot or as provided during construction of temporary entities.
       */
      case cd: ClassDef if isEvent(cd.symbol) =>
        super.transform(
          deriveClassDef(cd)(deriveTemplate(_) { body =>
            rewriteWithValBackingFields(cd, body)
          })
        )

      // If we are stored, eagerly-loaded and not a node, we'll need to rewrite ourself to access the stored value.
      // i.e. for a "val foo", we are currently "def foo = foo", and we must become "def foo = <stored property load code>".
      // `@node val`s and lazily-loaded (ie. blocking) stored vals need to wait to be rewritten in TypedNodeGeneration,
      // since the `foo$impl` methods for those return Node[T]s and so need to be called from `foo$newNode`.
      // Note: This DefDef may be the original accessor def for a backing val, or the def generated
      // in `rewriteClassOrModuleBody` to replace the original bare val.
      case dd: DefDef if {
            val sym = dd.symbol
            sym.attachments.get[Stored].exists(!_.lazyLoaded) && !sym
              .hasAttachment[Node] && !sym.isDeferred && !sym.isLazy
          } =>
        val valGetterSym = dd.symbol // def foo (getter)
        val classSym = valGetterSym.enclClass
        val classInfo = classSym.info

        // Simple stored (eagerly-loaded) vals can be considered stable
        // (subject to the caveats outlined in TypedNodeSynthesis.rewriteNodeDef)
        valGetterSym.setFlag(STABLE)

        val storedImplName = mkImplName(valGetterSym.name) // def foo$impl
        val storedImplSym = classInfo.decl(storedImplName)
        assert(storedImplSym != NoSymbol, s"Couldn't find $storedImplName")

        val newRhs = gen.mkAttributedSelect(gen.mkAttributedThis(classSym), storedImplSym)
        deriveDefDef(dd)(_ => newRhs)

      case _ => super.transform(tree)
    }

    private[this] def generateLookupForVarHandles(cd: ClassDef, body: List[Tree]): List[Tree] = {
      if (cd.symbol.isTrait) body
      else {
        var lookupAdded = false
        val lookup = flatMapConserve(body) {
          case vd: ValDef
              if !lookupAdded && (vd.symbol.hasAttachment[Stored] || vd.symbol.hasAttachment[Node]) &&
                needsLazyUnpickling(vd.symbol) =>
            val valSym = vd.symbol
            val propName = TermName("lookup")
            val storableClass = valSym.enclClass
            val vhSymbol = storableClass
              .newValue(propName, cd.pos)
              .setFlag(STATIC | FINAL | PRIVATE | SYNTHETIC)
              .setInfoAndEnter(LookupClass.tpe)
            val rhs = q"java.lang.invoke.MethodHandles.lookup()"
            val tree = localTyper.typedPos(cd.pos) { ValDef(vhSymbol, rhs) }
            lookupAdded = true
            tree :: vd :: Nil
          case t => t :: Nil
        }
        lookup
      }
    }

    private[this] def generateStatics(cd: ClassDef, body: List[Tree]): List[Tree] = {
      if (cd.symbol.isTrait) {
        body
      } else {
        flatMapConserve(body) {
          case vd: ValDef
              if (vd.symbol.hasAttachment[Stored] || vd.symbol.hasAttachment[Node]) &&
                needsLazyUnpickling(vd.symbol) =>
            val valSym = vd.symbol
            val propName = valSym.name.toTermName.getterName
            val storableClass = valSym.enclClass
            val vhSymbol = storableClass
              .newValue(mkVarHandleName(propName), cd.pos)
              .setFlag(STATIC | FINAL | PRIVATE | SYNTHETIC)
              .setInfoAndEnter(VarHandleClass.tpe)
            val rhs =
              q"${This(cd.symbol)}.lookup.findVarHandle(classOf[${cd.symbol.tpe}], ${mkImplName(propName).toString}, classOf[Any])"
            val tree = localTyper.typedPos(cd.pos) { ValDef(vhSymbol, rhs) }
            tree :: vd :: Nil
          case t =>
            t :: Nil
        }
      }
    }

    private[this] def needsLazyUnpickling(valSym: Symbol) = {
      val storedInfo = valSym.attachments.get[Stored]
      storedInfo.exists(_.lazyLoaded)
    }

    // Generates val foo$impl and appropriate getters and setters for any stored or node vals, and
    // removes the original vals
    // For classes: when we find a @stored or @node ValDef, synthesize:
    //   @s <synthetic> private[this] var foo$impl: X = ...
    //   <accessor> private def foo$impl = { CC.this.foo$impl }
    // and drop the ValDef itself.
    //
    // For traits: when we find the ValDef corresponding to val sym / ValDef pairs (as there currently is no accessor),
    // synthesize:
    //   <synthetic> <accessor> private[this] def foo$backing_=(x$1: X): Unit
    //   @s @backingStore <synthetic> <accessor> private[this] var foo$backing: Int = 1
    //   <synthetic> private def foo$impl: Int = { this.foo$backing }
    //   [whatever flags were on valdef] def foo = ...
    private[this] def rewriteWithValBackingFields(tree: ImplDef, body: List[Tree]): List[Tree] = flatMapConserve(body) {
      case vd: ValDef if vd.symbol.hasAttachment[Stored] || vd.symbol.hasAttachment[Node] =>
        // (vdd.isInstanceOf[DefDef] && vdd.symbol.attachments.get[Node].exists(_.nodeType.isInstanceOf[ValNode])) =>
        val valSym = vd.symbol

        val storedInfo = valSym.attachments.get[Stored]
        val isStored = storedInfo.isDefined
        val needsLazyUnpickling = storedInfo.exists(_.lazyLoaded)

        if (isStored && valSym.hasAnnotation(definitions.TransientAttr))
          alarm(OptimusErrors.STORED_HAS_TRANSIENT_VAL, tree.pos)

        val (backingFields, getterSym) = if (valSym.isDeferred) {
          // For deferred trait property vals, we don't need the backing vals until the subtypes which will
          // actually define an implementation for the val
          (Nil, NoSymbol)
        } else {
          // It's odd that for paramaccessors we need to generate a fake RHS of `Foo.this.foo`, but this
          // is a sneaky way of getting the scala constructors phase to write the correct initialisation line
          // into the constructor. We can't use the standard mechanism of just marking `val foo$impl` as a paramaccessor
          // because normally scala expects the constructor argument name to equal the val name, but in this case
          // we want to assign the constructor argument `foo` to the val `foo$impl`. This is all less than ideal.
          val rhs =
            if (valSym.isParamAccessor) gen.mkAttributedRef(valSym)
            else treeDuplicator.transform(vd.rhs)
          valBackingFields(valSym, isStored, needsLazyUnpickling, rhs, tree.pos)
        }

        // Note that below we always reset the ACCESSOR and STABLE flags. We'll re-add the STABLE flag (if appropriate)
        // later, as part of rewriting the DefDef to the new implementation (either here or in TypedNodeSynthesis).
        val newGetterDef: Tree =
          if (valSym.hasAttachment[NodeFieldType.BareVal] || valSym.hasAttachment[StoredGetter]) {
            // If we don't have a separate DefDef getter to transform later, convert the ValDef to a DefDef
            valSym.resetFlag(STABLE | ACCESSOR)
            markAsGenerated(valSym)
            val newRhs = {
              if (getterSym eq NoSymbol) EmptyTree // deferred
              else if (needsLazyUnpickling) {
                if (curInfo.isLoom) vd.rhs else PoisonedPlaceholder.shouldHaveReplacedThis
              } else gen.mkAttributedRef(getterSym)
            }
            val getter = DefDef(valSym, newRhs)
            localTyper.typedPos(vd.pos)(getter)
          } else {
            // we've already got a getter, so we don't need this ValDef
            valSym.enclClass.info.decls.unlink(valSym)
            valSym.removeAttachment[Attachment.Node]

            // valSym.attachments.get[StoredBackingVal] match {
            // case Some(StoredBackingVal(getterSym0, _)) =>
            val getterSym: Symbol = valSym.getter
            getterSym.resetFlag(STABLE | ACCESSOR)
            markAsGenerated(getterSym)
            if (valSym.hasAnnotation(StoredAnnotation))
              getterSym.addAnnotation(StoredAnnotation)
            // case _ =>
            // do nothing
            // }
            null
          }

        newGetterDef :?: backingFields

      // For
      //    class Foo(x: Int)
      //    class Bar(y: Int) extends Foo(y)
      // scala will elide storage of y and write its getter as
      //    def y: Int = Bar.super.x
      // This is a problem if x is a node, so in that case we remove the alias.
      case vd: ValDef
          if vd.symbol.isParamAccessor && vd.symbol.isTerm &&
            // parent val is a tweakable node
            vd.symbol.alias.getAnnotation(NodeAnnotation).nonEmpty &&
            // names are different - if they're the same, it either doesn't matter or we'll get a different error
            !vd.symbol.alias.nameString.equals(vd.symbol.nameString) =>
        alarm(OptimusNonErrorMessages.DEALIASING, vd.pos, vd.symbol.fullName, vd.symbol.alias.fullName)
        vd.symbol.asTerm.referenced = NoSymbol
        vd :: Nil
      case vd: DefDef if vd.symbol.hasAnnotation(StoredAnnotation) && !vd.symbol.enclClass.isTrait =>
        val rhs = q"${This(vd.symbol.enclClass)}.${mkImplName(vd.name)}"
        localTyper.typedPos(vd.pos) { DefDef(vd.symbol, rhs) } :: Nil
      case t =>
        t :: Nil
    }

    /*
     * Given a (possibly @node) val foo = ..., generate:
     * 1. val foo$impl (for classes) or def foo$backing (for traits)
     * 2. def foo$impl
     */
    private[this] def valBackingFields(
        valSym: Symbol,
        isStored: Boolean,
        needsLazyUnpickling: Boolean, // implies isStored
        rhs: Tree,
        pos: Position
    ): (List[Tree], Symbol) = {
      val storableClass = valSym.enclClass
      val isTrait = storableClass.isTrait

      // For every ValDef / DefDef accessor pair found above, synthesize
      //   @s <synthetic> private[this] val foo$impl: X = ...
      // we don't need the super accessor rewrite for @node generated $impls - to avoid call on-graph code in ctor
      if (valSym.isTerm && valSym.alias != NoSymbol) valSym.asTerm.referenced = NoSymbol

      val valType = if (needsLazyUnpickling) {
        if (isTrait) appliedType(Node.tpe, valSym.tpe.resultType :: Nil) else AnyRefTpe
      } else
        valSym.tpe.resultType

      require(valType != NoType)

      val propName = valSym.name.toTermName.getterName
      val dollarImplName = mkImplName(propName)

      // For ...... found in trait, synthesize getter
      // <synthetic> private def foo$impl: Int = { this.foo$backing }
      val backingValDef: Tree = {
        // For classes, synthesize:
        //   <synthetic> private[this] static final var foo$vh: VarHandle
        //   @s <synthetic> private[this] val foo$impl: X = ...
        // For traits:
        //   @s @backingStore <synthetic> <accessor> <stable> private[this] val foo$backing: X = ...
        // note that this is a ValDef with a method symbol.
        //
        // We recreate a backing foo$xxx every time foo is overridden because the <rhs> might have changed.
        // Now that unpickling is done with Unsafe puts, we never need explicit setters.  The fields phase
        // will synthesize any that are necessary for initialization.
        //
        // For traits, the actual holder variable for non-overridden vals will be synthesized in the first inheriting class.
        // If they are overridden, of course, there's no point holding anything.
        //
        // NB we taint these vars as synthetic so that we can spot them later on e.g. in macros. We may want to change
        // this to give them some specific annotation or flag them some other way; synthetic is already used for other
        // things by scalac.
        val backingValSym =
          if (isTrait)
            storableClass
              .newMethodSymbol(mkBackingName(propName), pos, newFlags = ACCESSOR)
              .setFlag(SYNTHETIC | PRIVATE | LOCAL | STABLE)
              .addAnnotation(BackingStoreAnnotation)
              .applyIf(isStored)(_.addAnnotation(AnnotationInfo(StoredAnnotation.tpe, Nil, Nil)))
              .setInfoAndEnter(NullaryMethodType(valType.widen))
          else {
            val impl =
              if (needsLazyUnpickling) {
                // need $impl to be a var for entities so that we can write to it as the state
                // transitions from entityReference to LPR to entity itself by the pluginHelper.resolveEntity*
                // methods.
                storableClass.newVariable(dollarImplName.localName, pos).addAnnotation(VolatileAttr)
              } else {
                storableClass.newValue(dollarImplName.localName, pos)
              }
            impl
              .setFlag(~(PARAMACCESSOR | ACCESSOR) & (valSym.flags | SYNTHETIC | LOCAL))
              .applyIf(isStored)(_.addAnnotation(AnnotationInfo(StoredAnnotation.tpe, Nil, Nil)))
              .setInfoAndEnter(valType.widen)
          }

        for (orgAnnotation <- valSym.annotations) {
          if (orgAnnotation.symbol.isJavaAnnotation && !OptimusJavaAnnos.contains(orgAnnotation.symbol))
            backingValSym.addAnnotation(orgAnnotation)
        }
        // DO NOT ENABLE EVEN IF TESTS PASS
        // CAUSES BREAKAGE ON INCREMENTAL COMPILES
        // backingValSym.privateWithin = entityCls

        val backingValRhs =
          if (needsLazyUnpickling) {
            // TODO (OPTIMUS-0000): Can we just reuse genStoredPropertyLoad(..) here?
            if (isEntity(storableClass)) {
              if (isTrait) {
                // @stored @entity trait
                val propInfo = Select(gen.mkAttributedRef(storableClass.companionModule), propName)
                New(
                  TypeTree(appliedType(AlreadyCompletedPropertyNode.tpe, valSym.tpe.resultType :: Nil)),
                  List(List(rhs, This(storableClass), propInfo)))
              } else {
                rhs // @stored @entity class
              }
            } else {
              // embeddable?
              New(TypeTree(appliedType(AlreadyCompletedNode.tpe, valSym.tpe.resultType :: Nil)), List(List(rhs)))
            }
          } else {
            // No lazy pickling
            rhs
          }

        new ChangeOwnerTraverser(valSym, backingValSym).traverse(backingValRhs)

        localTyper.typedPos(pos) { ValDef(backingValSym, backingValRhs) }
      }

      val (backingGetters, getterSym) = if (needsLazyUnpickling && !isTrait) {
        // For vals whose type is Entity, we generate foo$Impl, foo$queued and foo$newNode
        mkGettersForEntityAccessors(backingValDef, valSym, SYNTHETIC)
      } else {
        // For vals whose type is not Entity, backing getter is always foo$impl
        val backingGetterName = dollarImplName.toTermName.getterName
        val backingGetterFlags = (SYNTHETIC | PRIVATE) & ~(OVERRIDE | STABLE)
        val backingGetter = mkGetter(backingValDef, backingGetterFlags, backingGetterName, valType)
        if (!isTrait)
          backingGetter.symbol.setFlag(ACCESSOR)
        if (valSym.isParamAccessor) backingGetter.symbol.setFlag(PARAMACCESSOR)
        (List(backingGetter), backingGetter.symbol)
      }

      // Switch the param accessor from `def foo` to the generated `def foo$impl`. Importantly, we don't
      // reset the flag on valSym itself, since the hacky way we get the scala constructors phase to move
      // the initialisation of backingValDef into the constructor relies on the paramaccessor flag on the old
      // val (see comment in rewriteWithValBackingFields for more details).
      if (valSym.isParamAccessor) {
        valSym.getterIn(valSym.owner).resetFlag(PARAMACCESSOR)
      }

      // backingValDef = val foo$backing (if trait) or foo$impl (if class)
      // backingGetter = def foo$impl
      val meths = backingValDef :: backingGetters
      (meths, getterSym)
    }

    private[this] def generateArgsMethods(classDef: ClassDef, body: List[Tree]): List[Tree] = {
      var argsMethods: List[Tree] = Nil

      val classSym = classDef.symbol

      if (classSym.isAbstractClass) Nil
      else {
        def isNonValCtorArgument(sym: Symbol) =
          if (!(sym.isParamAccessor && sym.isPrivateLocal)) false
          else {
            val accessor = sym.getterIn(sym.owner)
            !accessor.isStable // $impl generated accessors drop stable flag
          }

        // Collect the constructor args for this entity
        val args_base = body collect {
          case vd: ValDef if isNonValCtorArgument(vd.symbol) =>
            val alias = vd.symbol.alias
            if ((alias ne NoSymbol) && (!alias.isGetter || isNode(alias) || alias.hasAttachment[Stored])) {
              // An alias for a non-val symbol is the corresponding val symbol in a parent constructor. For example, in:
              // class Foo(val a: Int); class Bar(b: Int) extends Foo(a)
              // Foo.a is the alias for Bar.b. However, typer expects aliases to be getter defs with backing vals, and
              // in many cases we'll have rewritten (or will be about to rewrite) Foo.a such that it no longer fits
              // this requirement. In those cases, strip the alias (which is stored in the `referenced` field) off
              // Bar.b to avoid upsetting typer if another class later extends Bar.
              vd.symbol.asTerm.referenced = NoSymbol
              // If a non-val constructor is shadowing a tweakable val in the super type, ensure that any accesses
              // are to the symbol in the super type (so that we see the tweaks), rather than accessing the non-val
              // constructor.
              verifySymbolUsed(classDef, vd.symbol, alias)
            }
            val dd = mkGetter(vd, FINAL | PRIVATE | SYNTHETIC, vd.name.append(names.$4eq))
            dd.updateAttachment(StoredGetter(true))
            argsMethods = dd :: argsMethods
            dd

          case dd: DefDef if dd.symbol.isParamAccessor => dd
        }

        // Combine with constructor args for any outer entity
        val args =
          if (!isEntity(classSym.owner) || classSym.owner.isTerm) args_base
          else {
            val outerSym = classSym.owner
            val getterSym = classSym
              .newMethod(names.$4eq, classDef.pos)
              .setFlag(FINAL | PRIVATE | SYNTHETIC)
              .setInfo(NullaryMethodType(outerSym.tpe))
            classSym.info.decls enter getterSym
            val dd =
              localTyper
                .typedPos(classDef.pos) { DefDef(getterSym, gen.mkAttributedThis(outerSym)) }
                .asInstanceOf[DefDef]
            argsMethods = dd :: argsMethods
            dd :: args_base
          }

        argsMethods ++= mkArgsHashAndEquals(classDef, args)
      }
      argsMethods
    }

    def mkArgsHashAndEquals(entityClassOrModule: Tree, args: List[ValOrDefDef]): List[Tree] = {
      val pos = entityClassOrModule.pos
      val sym = entityClassOrModule.symbol
      val entitySymbol =
        if (sym.isClass) sym
        else sym.moduleClass

      // For entities which are (transitively) defined inside a class we will include the (immediate) outer class in the
      // argsHash/argsEquals (since the outer is effectively an argument, and they may affect the return value of nodes
      // etc.). Note that if it's modules all the way up, we don't need to do that.
      def isInsideClass(s: Symbol): Boolean = (!s.isTopLevel) && (s.owner.isClass || isInsideClass(s.owner))
      val includeOuter = isInsideClass(entitySymbol)

      if (includeOuter || args.nonEmpty) {
        val hashSym =
          entitySymbol
            .newMethod(names.argsHash)
            .setFlag(OVERRIDE | PROTECTED | SYNTHETIC)
            .setInfo(NullaryMethodType(IntClass.tpe))
        entitySymbol.info.decls enter hashSym
        val hashStart =
          if (includeOuter) Apply(PluginSupport.outerHash, gen.mkAttributedThis(entitySymbol) :: Nil) else LIT(1)
        val hashDef = localTyper.typedPos(pos) { DefDef(hashSym, mkArgsHashT(args, entitySymbol, hashStart)) }

        val equalsSym = entitySymbol.newMethod(names.argsEquals, pos).setFlag(OVERRIDE | PROTECTED | SYNTHETIC)
        val argSym = equalsSym.newValueParameter(names.arg, pos).setInfo(EntityClass.tpe)
        equalsSym.setInfoAndEnter(MethodType(List(argSym), BooleanClass.tpe))
        val equalsDef = localTyper.typedPos(pos) {
          mkArgsEqualsT(argSym, entitySymbol, args, includeOuter).setSymbol(equalsSym)
        }
        hashDef :: equalsDef :: Nil
      } else Nil
    }

    private[this] def mkGettersForEntityAccessors(valDef: Tree, valSym: Symbol, flags: Long): (List[Tree], Symbol) = {
      val valName = valSym.name.toTermName.getterName
      val symbol = valDef.symbol
      val entityCls = symbol.enclClass

      // to generate def foo$impl: X = PluginHelpers.resolveRef(...)
      val implGetter = entityCls
        .newMethod(newTermName("" + valName + "$impl"))
        .setFlag(flags | SYNTHETIC | PRIVATE | (if (valSym.isParamAccessor) PARAMACCESSOR | ACCESSOR else 0L))
        .setInfo(NullaryMethodType(valSym.tpe))
      implGetter.setAttachments(implGetter.attachments.addElement(StoredGetter(true)))

      // to generate def foo$queued: NodeFuture[X] = PluginHelpers.resolveRefQueued(...)
      val queuedGetter = entityCls
        .newMethod(newTermName("" + valName + "$queued"))
        .setFlag(flags | SYNTHETIC)
        .setInfo(NullaryMethodType(appliedType(NodeFuture.tpe, List(valSym.tpe.finalResultType))))

      // to generate def foo#newNode(): Node[X] = PluginHelpers.resolveRefNewNode(...)
      val newNodeGetter = entityCls
        .newMethod(newTermName("" + valName + "$newNode"))
        .setFlag(flags | SYNTHETIC)
        .setInfo(
          MethodType(
            Nil,
            intersectionType(
              List(
                appliedType(Node.tpe, List(valSym.tpe.finalResultType)),
                appliedType(NodeKey.tpe, List(valSym.tpe.finalResultType))))))

      newNodeGetter.addAnnotation(DefNodeCreatorAnnotation)

      val defs: List[DefDef] = List(
        ("resolveRef", implGetter),
        ("resolveRefQueued", queuedGetter),
        ("resolveRefNewNode", newNodeGetter)
      ) map { case (helper, getterSym) =>
        entityCls.info.decls enter getterSym
        val block = {
          val expr = gen.mkAttributedRef(symbol)
          if (entityCls.hasAnnotation(StoredAnnotation) && entityCls.hasAnnotation(EntityAnnotation)) {
            val valSymName = valSym.name.toTermName.getterName
            val propInfo = Select(gen.mkAttributedRef(entityCls.companionModule), valSymName)
            val vhHandle = q"${This(entityCls)}.${mkVarHandleName(valSymName)}"
            val helperName = newTermName(helper)
            q"optimus.platform.PluginHelpers.$helperName[${valSym.tpe}](${This(entityCls)}, $vhHandle, $propInfo)"
          } else expr
        }
        localTyper.typedPos(valDef.pos) { DefDef(getterSym, block) }.asInstanceOf[DefDef]
      }
      (defs, defs.head.symbol)
    }

    private[this] def mkGetter(valDef: Tree, flags: Long, getterName: TermName, tpe: Type = null) = {
      val symbol = valDef.symbol
      val entityCls = symbol.enclClass
      val getterSym = entityCls
        .newMethod(getterName, valDef.pos)
        .setFlag(flags)
        .setInfo(NullaryMethodType(if (tpe ne null) tpe else symbol.tpe))
      markAsGenerated(getterSym)
      entityCls.info.decls enter getterSym
      val block = gen.mkAttributedRef(symbol)
      localTyper.typedPos(valDef.pos) { DefDef(getterSym, block) }.asInstanceOf[DefDef]
    }

    private[this] def verifySymbolUsed(start: Tree, vd: Symbol, symbol: Symbol): Unit = {
      if (symbol.hasAnnotation(TweakableAnnotation)) {
        val t = new Traverser {
          override def traverse(tree: Tree): Unit = {
            if (tree.symbol != null && (tree.symbol eq vd) && (!tree.isInstanceOf[ValDef]))
              alarm(OptimusNonErrorMessages.RELY_ON_BROKEN_BEHAVIOUR, tree.pos, vd.name, symbol.name)
            super.traverse(tree)
          }
        }
        t.traverse(start)
      }
    }

    def mkArgsEqualsT(argSym: Symbol, entitySym: Symbol, vals: List[SymTree], includeOuter: Boolean) = {
      val objArg = List(List(mkArg(names.arg, TypeTree(argSym.tpe)).setSymbol(argSym)))
      val listOfEquals = List.newBuilder[Tree]
      vals foreach { v =>
        val ts = v.symbol.tpe.resultType.typeSymbol
        if (ts != definitions.UnitClass)
          if (ts == definitions.FloatClass) {
            listOfEquals += Apply(
              PluginSupport.equals,
              gen.mkAttributedRef(v.symbol) :: gen.mkAttributedSelect(Ident(names.key), v.symbol) :: Nil)
          } else if (ts == definitions.DoubleClass) {
            listOfEquals += Apply(
              PluginSupport.equals,
              gen.mkAttributedRef(v.symbol) :: gen.mkAttributedSelect(Ident(names.key), v.symbol) :: Nil)
          } else {
            listOfEquals += Apply(
              Select(valueOf(q"${This(entitySym)}", entitySym, v.symbol), nme.EQ),
              valueOf(q"${names.key}", entitySym, v.symbol) :: Nil)
          }
      }
      if (includeOuter)
        listOfEquals += Apply(PluginSupport.outerEquals, gen.mkAttributedThis(entitySym) :: Ident(names.key) :: Nil)
      val cond = mkIsInstanceOf(Ident(names.arg), TypeTree(entitySym.tpe))
      val body = Block(
        ValDef(NoMods, names.key, TypeTree(entitySym.tpe), mkCast(Ident(names.arg), TypeTree(entitySym.tpe))),
        mkLogicalAnd(listOfEquals.result()))
      val rhs = If(cond, body, FALSE)
      DefDef(
        Modifiers(OVERRIDE | PROTECTED | SYNTHETIC),
        names.argsEquals,
        Nil,
        objArg,
        gen.mkAttributedRef(BooleanClass),
        rhs)
    }
  }

}
