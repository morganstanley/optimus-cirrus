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

import optimus.tools.scalacplugins.entity.reporter.OptimusPluginReporter

import scala.reflect.ClassTag
import scala.tools.nsc.Global
import scala.tools.nsc.ast.TreeDSL
import scala.tools.nsc.symtab.Flags

/*
 * General "utils" for the compiler plugin.  Code here should be shared between pre-typer and post-typer phases.
 */
trait PluginUtils extends TreeDSL with OptimusNames with TreeDuplicator with OptimusPluginReporter {

  val global: Global
  import global._
  import definitions._
  import gen.rootScalaDot

  // Common tree creators
  def CorePkg = Select(Select(Ident(nme.ROOTPKG), names.optimus), names.core)
  def PlatformPkg = Select(Select(Ident(nme.ROOTPKG), names.optimus), names.platform)
  def ScalaCompatCollectionPkg =
    Select(Select(Select(Ident(nme.ROOTPKG), names.optimus), names.scalacompat), names.collection)
  def VersioningPkg = Select(Select(Ident(nme.ROOTPKG), names.optimus), names.versioning)
  def StorablePkg = Select(PlatformPkg, names.storable)
  def DalPkg = Select(PlatformPkg, names.dal)
  def GraphPkg = Select(Select(Ident(nme.ROOTPKG), names.optimus), names.graph)
  def DistPkg = Select(Select(Ident(nme.ROOTPKG), names.optimus), names.dist)
  def ElevatedPkg = Select(DistPkg, newTermName("elevated"))
  def EntityPkg = Select(Select(Ident(nme.ROOTPKG), names.optimus), names.entity)
  def AnnotationPkg = Select(PlatformPkg, names.annotations)
  def InternalAnnotationPkg = Select(AnnotationPkg, names.internal)
  def PicklingPkg = Select(PlatformPkg, names.pickling)
  def JavaTimePkg = Select(Select(Ident(nme.ROOTPKG), newTermName("java")), newTermName("time"))
  def InternalPkg = Select(PlatformPkg, names.internal)
  def ScalaPkg = Select(Ident(nme.ROOTPKG), nme.scala_)
  def ScalaReflectPkg = Select(ScalaPkg, names.reflect)
  def JavaLangPkg = Select(Select(Ident(nme.ROOTPKG), nme.java), nme.lang)
  def ScalaAnnotationPkg = Select(Select(Ident(nme.ROOTPKG), nme.scala_), nme.annotation)
  def ScalaMetaAnnotationPkg = Select(ScalaAnnotationPkg, newTermName("meta"))

  def loadContext = Select(PlatformPkg, names.loadContext)

  def EvaluationContextVerifyOffGraph = Select(Select(PlatformPkg, names.EvaluationContext), names.verifyOffGraph)
  def EvaluationContextVerifyImpure = Select(Select(PlatformPkg, names.EvaluationContext), names.verifyImpure)

  object PluginSupport {
    lazy val corePluginSupportSym = rootMirror.getRequiredModule("optimus.graph.CorePluginSupport")
    lazy val platformPluginSupportSym = rootMirror.getRequiredModule("optimus.graph.PluginSupport")
    private def CorePluginSupport = gen.mkAttributedRef(corePluginSupportSym)
    private def PlatformPluginSupport = gen.mkAttributedRef(platformPluginSupportSym)
    def PlatformPluginHelpers = Select(PlatformPkg, names.PluginHelpers)
    def CanEqualClass = Select(ScalaCompatCollectionPkg, names.CanEqual)
    def CoreSupport = Select(CorePkg, names.CoreSupport)

    object localNames {
      val genReifiedLambda = newTermName("genReifiedLambda")
      val genProjectedMember = newTermName("genProjectedMember")
      val genEntityKeyInfo = newTermName("genEntityKeyInfo")
      val genQueryableEntityKeyInfo = newTermName("genQueryableEntityKeyInfo")
      val genEventKeyInfo = newTermName("genEventKeyInfo")
      val genEventIndexInfo = newTermName("genEventIndexInfo")
      val genQueryableEventIndexInfo = newTermName("genQueryableEventIndexInfo")
      val getEntity = newTermName("getEntityByKeyAtNow")
      val getEntityOption = newTermName("getEntityByKeyAtNowOption")
      val getEvent = newTermName("getEventByKeyAtNow")
      val getEventOption = newTermName("getEventByKeyAtNowOption")
      val missingProperty = newTermName("missingProperty")
      val disabledConstructorDefaultValues = newTermName("disabledConstructorDefaultValues")
      val safeResult = newTermName("safeResult")
      val witnessVersion = newTermName("witnessVersion")
      val isTickableContext = newTermName("isTickableContext")
      val getEntityReference = newTermName("getEntityReference")
      val observedValueNode = newTermName("observedValueNode")
      val outerHash = newTermName("outerHash")
      val outerEquals = newTermName("outerEquals")
      val avoidZero = newTermName("avoidZero")
    }

    object localDefns {
      val observedValueNode = getMember(corePluginSupportSym, localNames.observedValueNode)
    }

    def genProjectedMember = Select(PlatformPluginSupport, localNames.genProjectedMember)
    def genReifiedLambda = Select(PlatformPluginSupport, localNames.genReifiedLambda)
    def genEntityKeyInfo = Select(PlatformPluginSupport, localNames.genEntityKeyInfo)
    def genQueryableEntityKeyInfo = Select(PlatformPluginSupport, localNames.genQueryableEntityKeyInfo)
    def genEventKeyInfo = Select(PlatformPluginSupport, localNames.genEventKeyInfo)
    def genIndexInfo = Select(PlatformPluginSupport, names.genIndexInfo)
    def genQueryableIndexInfo = Select(PlatformPluginSupport, names.genQueryableIndexInfo)
    def genQueryableIndexInfoWithErefFilter = Select(PlatformPluginSupport, names.genQueryableIndexInfoWithErefFilter)
    def genEventIndexInfo = Select(PlatformPluginSupport, localNames.genEventIndexInfo)
    def genQueryableEventIndexInfo = Select(PlatformPluginSupport, localNames.genQueryableEventIndexInfo)
    def missingProperty(name: String) =
      Apply(Select(PlatformPluginSupport, localNames.missingProperty), Literal(Constant(name)) :: Nil)
    def disabledGeneratedInitMethod(name: String) =
      Apply(Select(PlatformPluginSupport, localNames.disabledConstructorDefaultValues), Literal(Constant(name)) :: Nil)
    def observedValueNode(value: Tree, entity: Tree, propInfo: Tree): Tree = // technically only safe post-typer
      Apply(gen.mkAttributedSelect(CorePluginSupport, localDefns.observedValueNode), List(value, entity, propInfo))

    def hashOf = Select(PlatformPluginHelpers, names.hashOf)
    def equals = Select(PlatformPluginHelpers, names.equals)
    def equalsAvoidingZero = Select(PlatformPluginHelpers, names.equalsAvoidingZero)
    def canEqual = Select(CanEqualClass, names.canEqual)
    def canEqualStorable = Select(PlatformPluginHelpers, names.canEqualStorable)
    def safeResult = Select(PlatformPluginHelpers, localNames.safeResult)
    def avoidZero = Select(PlatformPluginHelpers, localNames.avoidZero)
    def toNode = Select(CoreSupport, names.toNode)

    def getEntity = Select(PlatformPluginSupport, localNames.getEntity)
    def getEntityOption = Select(PlatformPluginSupport, localNames.getEntityOption)
    def getEvent = Select(PlatformPluginSupport, localNames.getEvent)
    def getEventOption = Select(PlatformPluginSupport, localNames.getEventOption)

    def getEntityReference = Select(PlatformPluginSupport, localNames.getEntityReference)

    def outerHash = Select(PlatformPluginHelpers, localNames.outerHash)
    def outerEquals = Select(PlatformPluginHelpers, localNames.outerEquals)
  }
  object NodeSupport {
    lazy val thisSym = rootMirror.getRequiredModule("optimus.platform.storable.NodeSupport")
    lazy val lookupConstructorCache = definitions.getMemberMethod(thisSym, TermName("lookupConstructorCache"))
  }

  object PlatformPkgObj {
    lazy val thisSym = rootMirror.getRequiredModule("optimus.platform.package")
    def THIS = gen.mkAttributedRef(thisSym)

    object localNames {
      val nodeFutureOf = newTermName("nodeFutureOf")
    }

    def nodeFutureOf(node: Tree): Tree = Apply(Select(THIS, localNames.nodeFutureOf), node :: Nil)
  }

  def EmbeddableType = Select(StorablePkg, tpnames.Embeddable)
  def EntityType = Select(StorablePkg, tpnames.Entity)
  def InlineEntityType = Select(StorablePkg, tpnames.InlineEntity)
  def EventType = Select(PlatformPkg, tpnames.BusinessEvent)
  def StorableType = Select(StorablePkg, tpnames.Storable)
  def ContainedEventType = Select(PlatformPkg, tpnames.ContainedEvent)

  def ScenarioType = Select(PlatformPkg, tpnames.Scenario)
  def ScenarioStackType = Select(PlatformPkg, tpnames.ScenarioStack)
  def TemporalContextType = Select(PlatformPkg, tpnames.TemporalContext)
  def NodeType = Select(GraphPkg, tpnames.Node)
  def NodeFutureType = Select(GraphPkg, tpnames.NodeFuture)
  def AsyncLazyType = Select(Select(PlatformPkg, tpnames.asyncLazyWithAnyRuntimeEnv), tpnames.Lazy)

  def mkImpureBlock(tree: Tree): Tree =
    treeCopy.Block(tree, atPos(tree.pos.focus)(Apply(EvaluationContextVerifyImpure, Nil)) :: Nil, tree)
  def mkOffGraphBlock(tree: Block): Block =
    treeCopy.Block(tree, Apply(EvaluationContextVerifyOffGraph, Literal(Constant(true)) :: Nil) :: Nil, tree)

  def mkExistentialType(tycon: Tree) = {
    val bounds = TypeBoundsTree(rootScalaDot(tpnme.Nothing), rootScalaDot(tpnme.Any))
    val typeParam = TypeDef(Modifiers(Flags.DEFERRED | Flags.SYNTHETIC), tpnames.tmp, Nil, bounds)
    ExistentialTypeTree(AppliedTypeTree(tycon, List(Ident(tpnames.tmp))), List(typeParam))
  }

  // Existential types NodeKey[_] and PropertyInfo[_]
  def NodeKeyXType = mkExistentialType(NodeKeyType)
  def PropertyInfoXType = mkExistentialType(Select(GraphPkg, tpnames.PropertyInfo))

  def PropertyNodeType = Select(GraphPkg, tpnames.PropertyNode)
  def CompletableNodeType = Select(GraphPkg, tpnames.CompletableNode)
  def NodeKeyType = Select(GraphPkg, tpnames.NodeKey)
  def EventCompanionBaseType = Select(StorablePkg, tpnames.EventCompanionBase)
  def EntityCompanionBaseType = Select(StorablePkg, tpnames.EntityCompanionBase)
  def EmbeddableCompanionBaseType = Select(StorablePkg, tpnames.EmbeddableCompanionBase)
  def EmbeddableTraitCompanionBase = Select(StorablePkg, tpnames.EmbeddableTraitCompanionBase)
  def KeyedEntityCompanionBaseType = Select(StorablePkg, tpnames.KeyedEntityCompanionBase)
  def KeyedEntityCompanionClsType = Select(StorablePkg, tpnames.KeyedEntityCompanionCls)

  // Common make helpers

  lazy val MapModule = rootMirror.getRequiredModule("scala.collection.immutable.Map")

  // List(arg1, arg2 ...)
  def mkList(args: List[Tree]) = Apply(Select(gen.mkAttributedRef(ListModule), nme.apply), args)
  // Array(arg1, arg2 ...)
  def mkArray(args: List[Tree]) = Apply(Select(gen.mkAttributedRef(ArrayModule), nme.apply), args)
  def mkMap(targs: List[Tree], args: List[Tree]) =
    Apply(gen.mkTypeApply(Select(gen.mkAttributedRef(MapModule), nme.apply), targs), args)

  // (val arg1, val arg2...)
  def mkNodeCtorArgs(vparamss: List[List[ValDef]]) = {
    val paramMods = Modifiers(Flags.PARAMACCESSOR | Flags.FINAL)
    for (vd @ ValDef(mods, name, tpt, rhs) <- dupTreeFlat(vparamss)) yield {
      ValDef(paramMods, name, tpt, rhs).setPos(vd.pos)
    }
  }

  def typedRef(name: Name, tparams: List[TypeDef]) =
    if (tparams.isEmpty) Ident(name)
    else {
      val tpnames = tparams map { td =>
        Ident(td.name)
      }
      AppliedTypeTree(Ident(name), tpnames)
    }

  // new nodeClassName(arg1, arg2,...)
  def mkNewNode(nodeClass: Tree, vparamss: List[List[ValDef]]) =
    New(
      nodeClass,
      List(vparamss.flatten.map { arg =>
        Ident(arg.name)
      }))

  def mkArg(name: TermName, tpt: Tree, flags: Long = 0L) = ValDef(Modifiers(flags), name, tpt, EmptyTree)
  def mkType(tpe: Name) = tpe match {
    case _: TypeName => Ident(tpe)
    case _: TermName => SingletonTypeTree(Ident(tpe))
  }
  def mkCast(value: Tree, tpt: Tree) = TypeApply(Select(value, nme.asInstanceOf_), List(tpt))
  def mkIsInstanceOf(value: Tree, tpt: Tree) = TypeApply(Select(value, nme.isInstanceOf_), List(tpt))

  def flatMapConserve[A <: AnyRef](as: List[A])(f: A => List[A]) = {
    import scala.collection.mutable.ListBuffer

    @scala.annotation.tailrec
    def loop(mapped: ListBuffer[A], unchanged: List[A], pending: List[A]): List[A] =
      if (pending.isEmpty) {
        if (mapped eq null) unchanged
        else mapped.prependToList(unchanged)
      } else {
        val head0 = pending.head
        val head1 = f(head0)

        head1 match {
          case x :: Nil if (x eq head0) =>
            loop(mapped, unchanged, pending.tail)
          case _ =>
            val b = if (mapped eq null) new ListBuffer[A] else mapped
            var xc = unchanged
            while (xc ne pending) {
              b += xc.head
              xc = xc.tail
            }
            b ++= head1
            val tail0 = pending.tail
            loop(b, tail0, tail0)
        }
      }
    loop(null, as, as)
  }

  /**
   * This is a class help for traverse Trees and pass information in different recursions
   */
  class StateHolder[T](defaultValue: T) {

    private var _currentValue: T = defaultValue

    def currentValue = _currentValue

    def enterNewContext[U](newValue: T)(func: => U): U = {
      val oldValue = _currentValue
      _currentValue = newValue
      val res = func
      _currentValue = oldValue
      res
    }
  }

  def createTemplate(
      parents: List[Tree],
      self: ValDef,
      constrMods: Modifiers,
      vparamss: List[List[ValDef]],
      argss: List[List[Tree]],
      body: List[Tree],
      superPos: Position): Template = {
    import Flags._

    // create parameters for <init> as synthetic trees.
    var vparamss1 = mmap(vparamss) { vd =>
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
        if (body forall treeInfo.isInterfaceMember) List.empty
        else
          List(
            atPos(wrappingPos(superPos, lvdefs))(
              DefDef(
                NoMods,
                nme.MIXIN_CONSTRUCTOR,
                List.empty,
                Nil :: Nil,
                TypeTree(),
                Block(lvdefs, Literal(Constant(()))))))
      } else {
        // convert (implicit ... ) to ()(implicit ... ) if its the only parameter section
        if (vparamss1.isEmpty || vparamss1.head.nonEmpty && vparamss1.head.head.mods.isImplicit)
          vparamss1 = List.empty :: vparamss1;
        val superSelect = Select(Super(This(tpnme.EMPTY), tpnme.EMPTY), nme.CONSTRUCTOR)
        val superRef: Tree = atPos(superPos)(superSelect)
        val superCall = argss.foldLeft(superRef)(Apply.apply)
        List(
          atPos(wrappingPos(superPos, lvdefs ::: argss.flatten))(
            DefDef(
              constrMods,
              nme.CONSTRUCTOR,
              List.empty,
              vparamss1,
              TypeTree(),
              Block(lvdefs ::: List(superCall), Literal(Constant(()))))))
      }
    }
    constrs foreach (ensureNonOverlapping(_, parents ::: gvdefs, focus = false))
    // Field definitions for the class - remove defaults.
    val fieldDefs = vparamss.flatten map (vd => copyValDef(vd)(mods = vd.mods &~ DEFAULTPARAM, rhs = EmptyTree))

    global.Template(parents, self, gvdefs ::: fieldDefs ::: constrs ::: etdefs ::: rest)
  }

  def createClassDef(
      sym: Symbol,
      constrMods: Modifiers,
      vparamss: List[List[ValDef]],
      argss: List[List[Tree]],
      body: List[Tree],
      superPos: Position): ClassDef = {
    // "if they have symbols they should be owned by `sym`"
    assert(mforall(vparamss)(p => (p.symbol eq NoSymbol) || (p.symbol.owner == sym)), ((mmap(vparamss)(_.symbol), sym)))

    global.ClassDef(
      sym,
      createTemplate(
        sym.info.parents.map(TypeTree(_)),
        if (sym.thisSym == sym || phase.erasedTypes) noSelfType else ValDef(sym.thisSym),
        constrMods,
        vparamss,
        argss,
        body,
        superPos)
    )
  }

  def vParamssOf(tree: Tree): List[List[ValDef]] = tree match {
    case dd: DefDef => dd.vparamss
    case _          => Nil
  }

  def typeParamsOf(tree: Tree): List[TypeDef] = tree match {
    case dd: DefDef   => dd.tparams
    case cd: ClassDef => cd.tparams
    case _            => Nil
  }

  object Attached {
    def apply[A: ClassTag]: AttachmentUnapplier[A] = new AttachmentUnapplier[A] {
      def unapply(sym: Symbol): Option[A] = { sym.info; sym.attachments.get[A] }
      def unapply(tree: Tree): Option[(Tree, A)] = Option(tree.symbol).flatMap(unapply).map(tree -> _)
    }
  }
  trait AttachmentUnapplier[A] {
    // would that we could do `case (vd: ValDef) @ NodeAttached(info) =>`
    // but that tries the limits of the already-overworked pattern matcher
    def unapply(tree: Tree): Option[(Tree, A)]
    def unapply(sym: Symbol): Option[A]
  }

  // 2.12.x vs 2.13.x compiler API compatibility
  import staged.scalaVersionRange

  object NamedArgTree {
    def apply(lhs: Tree, rhs: Tree): Tree =
      if (scalaVersionRange("2.13:"): @staged) {
        global.NamedArg(lhs, rhs)
      } else {
        global.AssignOrNamedArg(lhs, rhs)
      }
    def unapply(tree: Tree): Option[(Tree, Tree)] = {
      if (scalaVersionRange("2.13:"): @staged) {
        tree match {
          case t: global.NamedArg => global.NamedArg.unapply(t)
          case _                  => None
        }
      } else {
        tree match {
          case t: global.AssignOrNamedArg => global.AssignOrNamedArg.unapply(t)
          case _                          => None
        }
      }
    }
  }

  /**
   * Wrap `Assign` trees in a block. This prevents quasiquote expansion from changing `Assign` trees in argument
   * position to `NamedArg` (see `assignmentToMaybeNamedArg` in the compiler). Example:
   * {{{
   *   val arg = q"x = 10"
   *   val call = q"apply(${wrapAssign(arg)})"
   * }}}
   */
  def wrapAssign(t: Tree): Tree = t match {
    case _: Assign => q"{ $t; () }"
    case _         => t
  }
}
