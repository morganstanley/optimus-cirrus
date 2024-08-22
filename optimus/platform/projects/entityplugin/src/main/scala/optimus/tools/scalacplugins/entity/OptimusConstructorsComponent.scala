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
import optimus.tools.scalacplugins.entity.staged.scalaVersionRange

import scala.annotation.tailrec
import scala.collection.mutable
import scala.reflect.internal.Flags.MIXEDIN
import scala.tools.nsc.transform.Transform
import scala.tools.nsc.symtab.Flags
import scala.tools.nsc.transform._

/**
 * Strategy:
 *
 * Optimus entities use custom serialization (called "pickling") code rather than relying on Java serialization. This
 * allows better support for pickling partial entity graphs, dealing with lazy loading, capturing tweaks, etc. as well
 * as providing better performance.
 *
 * For each entity, we generate a pickle method and an auxiliary constructor that takes a (PickledInputStream,
 * TemporalContext). These methods have to deal properly with superclasses and traits.
 *
 * This phase runs pretty late, after constructors and mixin, where the underlying fields from traits already exist, and
 * the actual writes to all fields exist in the primary constructor body.
 *
 * Note that the auxiliary constructor we generate here is not expressible in scala. It does not call the primary
 * constructor directly. We do it this late so that we have sufficient control over the initialization of fields mixed
 * in by traits, etc.
 */
class OptimusConstructorsComponent(val plugin: EntityPlugin, val phaseInfo: OptimusPhaseInfo)
    extends EntityPluginComponent
    with Transform
    with TransformGrep
    with TypingTransformers
    with TypedUtils
    with TreeDuplicator
    with WithOptimusPhase {
  import global._

  def newTransformer0(unit: CompilationUnit) = new OptimusConstructors(unit)

  class OptimusConstructors(unit: CompilationUnit) extends TypingTransformer(unit) {
    import global._

    override def transform(tree: Tree): Tree = {
      tree match {
        case _: Select =>
          if (scalaVersionRange("2.13:"): @staged) {
            // Scala 2.13 emits top level module fields as static. The implementation sets this flag very late,
            // in getPlainClass. But if this reference to the symbol is emitted before that point, it will
            // get a GETFIELD rather than GETSTATIC.
            //
            // Workaround by setting the flag here eagerly.
            val sym = tree.symbol
            if (genBCode.bTypes.isStaticModuleClass(sym.owner) && !sym.isMethod && sym.isTerm && !sym.isModule)
              tree.symbol.setFlag(Flags.STATIC)
          }

          super.transform(tree)

        case ClassDef(_, _, _, Template(_, _, stats)) =>
          trackNodeCallsAndLambdas(tree)
          tryMakeCtor(tree, stats)
          super.transform(tree)
        // transform stats inside primary constructor:
        //
        // For example, for this class:
        // @stored @entity class Foo(val param: Int) {
        //   val content = param
        // }
        //
        // The primary constructor would generate:
        // def <init>(param) {
        //   Foo.this.content = new AlreadyCompletedPropertyNode(Foo.this.param(), Foo.this, graph.this.Foo.param())
        // }
        //
        // We need to instead generate:
        // def <init>(param) {
        //   Foo.this.content = new AlreadyCompletedPropertyNode(param, Foo.this, graph.this.Foo.param())
        // }
        case dd @ DefDef(mods, name, tparams, vparamss, tpt, rhs: Block)
            if dd.symbol == dd.symbol.owner.primaryConstructor =>
          val reorderredRhs = reorderStatsInPrimaryCtor(rhs)
          val newRhs = replaceValAccessorsWithLocalParameter(vparamss, reorderredRhs)
          super.transform(treeCopy.DefDef(dd, mods, name, tparams, vparamss, tpt, newRhs))

        case dd: DefDef if dd.symbol.hasFlag(MIXEDIN) =>
          // this drops optimus attributes off generated mix code:
          // mixin generated code should remain an ignored Scala implementation detail!
          dd.symbol.filterAnnotations(anno => !isOptimusGraphSpecific(anno))
          super.transform(dd)
        // for subclasses of events, we have to rewrite the super ctor call to pass in the supplied vt parameter.
        // prior to this rewrite the call looks like super.<init>(args, SuperClass.this.<init>$default$1)
        case Apply(sel @ Select(_: Super, nme.CONSTRUCTOR), args)
            if isEvent(sel.symbol.owner) && (sel.symbol.owner.tpe.typeSymbol ne BusinessEventImplClass) &&
              !sel.symbol.owner.isAbstractClass =>
          // get the symbol of the constructor's validTime parameter.
          // why not just currentOwner.vparamss.head.last? exiting erasure, that no longer matches up with the symbol
          // of the parameter ValDef. why not? why hasn't this caused other problems? who knows.
          // The backend refers to parameters by the symbol of the parameter val, so that's what we need to use.
          // Phase travelling to before erasure could also work, but this seems more obviously correct to me.
          val vtParam = localTyper.context.enclMethod.tree.asInstanceOf[DefDef].vparamss.head.last
          val vtArg = gen.mkAttributedIdent(vtParam.symbol)
          assert(vtArg.tpe.typeSymbol == InstantCls, (sel.symbol.owner, vtArg.tpe)) // just an integrity check
          val newArgs = args.init :+ vtArg
          val newTree = treeCopy.Apply(tree, sel, newArgs)
          super.transform(newTree)

        case _ => super.transform(tree)
      }
    }

    // Refered to normal scalac constructors phase:
    // https://github.com/scala/scala/blob/3bb735823f6815002895b1a335c6d105ddbe3e9e/src/compiler/scala/tools/nsc/transform/Constructors.scala#L520-L521
    private def replaceValAccessorsWithLocalParameter(vparamss: List[List[ValDef]], rhs: Tree): Tree = {
      new Transformer {
        override def transform(tree: Tree): Tree = tree match {
          case ap @ Apply(func @ Select(This(_), _), _)
              if func.symbol != null && func.symbol != NoSymbol && func.symbol.hasAnnotation(ValAccessorAnnotation) =>
            mfind(vparamss)(_.name.dropLocal == func.symbol.name.dropLocal) match {
              case Some(accessedValDef) =>
                gen.mkAttributedIdent(accessedValDef.symbol) setPos ap.pos
              case None =>
                super.transform(tree)
            }
          case _ =>
            super.transform(tree)
        }
      }.transform(rhs)
    }

    // put "stored property" = "paramaccessor" assignment statement before call super constructor just like scala compiler did
    // Since if the rhs of original property foo needs lazy load (Entity), the generated foo$impl will be type of Node, which hold reference to "this"
    // We don't reorder for such $impl assignment
    private def reorderStatsInPrimaryCtor(rhs: Block): Block = {
      val (paramInits, others) = rhs.stats.partition {
        case Assign(lhs, _)
            if lhs.symbol != NoSymbol && lhs.symbol.hasAnnotation(
              StoredAnnotation) && !lhs.symbol.tpe.resultType.typeSymbol
              .isSubClass(Node) =>
          val getter = lhs.symbol.getterIn(lhs.symbol.owner)
          getter != NoSymbol && getter.hasFlag(Flags.PARAMACCESSOR)
        case _ => false
      }
      treeCopy.Block(rhs, paramInits ::: others, rhs.expr)
    }
  }

  private def tryMakeCtor(tree: Tree, stats: List[Tree]): Unit =
    // TODO (OPTIMUS-0000): The check for BusinessEventImpl is a temporal fix so that we won't do the transform for it
    // We should move it to the core project so that it's not compiled with entityplugin
    if (shouldMakeCtor(tree.symbol) && tree.symbol.tpe.typeSymbol != BusinessEventImplClass) {
      val props: Map[Symbol, Tree] = stats.collect {
        case stat @ ValDef(_, _, _, rhs) if stat.symbol hasAnnotation StoredAnnotation =>
          (stat.symbol, rhs)
      }.toMap

      // We're about to generate a constructor that takes a PickledInputStream and initializes the
      // stored vars with the values from the stream.

      val primaryCtor = tree.symbol.primaryConstructor
      require(primaryCtor != NoSymbol)

      val pcTree = stats.find { _.symbol == primaryCtor }.get.asInstanceOf[DefDef]
      val pcStats = pcTree.rhs.asInstanceOf[Block]

      object SuperStorableConstructor {
        @tailrec
        def unapply(t: Tree): Option[Select] = t match {
          case ap @ Apply(fun @ Select(sup: Super, nme.CONSTRUCTOR), _) if isEntityOrEvent(fun.symbol.owner) =>
            Some(fun)
          // In some cases scalac generates a Block containing the super call instead of a direct Apply tree.
          // E.g. DerivingParentWithDefaultCtorParamsUsingNamedParams test case.
          case Block(_, ap: Apply) =>
            unapply(ap)
          case _ => None
        }
      }

      def checkStatement(st: Tree): Unit = {
        st match {
          // Is this a declared stored field that needs to be unpickled?
          case s @ Assign(lhs, rhs) if props.contains(lhs.symbol)                                => ()
          case SuperStorableConstructor(fun @ Select(sup: Super, nme.CONSTRUCTOR))               => ()
          case ap @ Apply(fun @ Select(sup: Super, nme.CONSTRUCTOR), args)                       => ()
          case ap @ Apply(fun @ Select(qual, nme.MIXIN_CONSTRUCTOR), args)                       => ()
          case st @ Assign(lhs, Ident(names.validTime)) if isEvent(tree.symbol)                  => ()
          case st @ Assign(lhs, rhs) if (rhs.symbol ne null) && (rhs.symbol hasFlag Flags.PARAM) => ()
          case Apply(Select(base, term), _) if term == names.assert || term == names.require     => ()
          // inner classes (e.g. in REPL wrappers) have this check in the constructor
          case If(Apply(Select(Ident(nme.OUTER), nme.eq), Literal(Constant(null)) :: Nil), _, _) => ()
          case Apply(sel: Select, Nil)
              if sel.symbol.owner == definitions.RuntimeStaticsModule.moduleClass && sel.symbol.name.string_==(
                "releaseFence") =>
            ()
          case _ =>
            alarm(OptimusErrors.INVALID_CONSTRUCTOR_STATEMENT, st.pos)
        }
      }

      pcStats.stats foreach checkStatement
    }

  private def shouldMakeCtor(sym: Symbol): Boolean = {
    // is this a storable class
    (isEntityOrEvent(sym) && !sym.isTrait && !sym.isModuleClass) && ((sym hasAnnotation StoredAnnotation) || isEvent(
      sym))
  }

  private def trackNodeCallsAndLambdas(tree: Tree): Unit = if (curInfo.isLoom) {
    tree.symbol.getAnnotation(LoomAnnotation).foreach { loomAnno =>
      val nodesArg = {
        val nodeCalls = getOrComputeLoomNodes(tree, loomAnno)
        if (nodeCalls.isEmpty) Nil else List(tpnames.loomNodes -> ArrayAnnotArg(nodeCalls))
      }
      val lambdasArg = {
        val nodeLiftCalls = getOrComputeLoomLambdas(tree, loomAnno)
        if (nodeLiftCalls.isEmpty) Nil else List(tpnames.loomLambdas -> ArrayAnnotArg(nodeLiftCalls))
      }
      val updatedLoomAnno: AnnotationInfo = AnnotationInfo(LoomAnnotation.tpe, Nil, nodesArg ++ lambdasArg)
      tree.symbol.removeAnnotation(loomAnno.symbol)
      tree.symbol.addAnnotation(updatedLoomAnno)
    }
  }

  private def getOrComputeLoomNodes(tree: Tree, loomAnno: AnnotationInfo): Array[ClassfileAnnotArg] =
    // manually written annotation arg, usually for debugging purposes
    getLoomNodesArg(loomAnno).getOrElse {
      val nodeCalls = {

        /**
         * Figuring out receiver might require duping some logic from
         * [[scala.tools.nsc.backend.jvm.BCodeBodyBuilder.PlainBodyBuilder#genApply]] and
         * [[scala.tools.nsc.backend.jvm.BCodeBodyBuilder.PlainBodyBuilder.genCallMethod]]
         */
        val calls = new mutable.HashSet[(Symbol, Symbol)]() // method, receiver
        for {
          sel @ Select(_, _) <- tree
          if hasNodeSyncAnnotation(sel) && !hasAsyncAnnotation(sel)
        } { calls.add(sel.symbol, sel.qualifier.tpe.typeSymbol) }
        calls.toSeq
      }

      val nodeCallsArgs: Array[ClassfileAnnotArg] = {
        val sortedCallsByReceiver = nodeCalls
          .groupBy { case (_, receiver) => receiver.javaBinaryNameString }
          .map { case (o, ss) => o -> ss.map { case (method, _) => method.rawname.toString }.sorted }
          .toSeq
          .sortBy { case (receiver, _) => receiver }
        sortedCallsByReceiver.flatMap { case (owner, calls) =>
          LiteralAnnotArg(Constant(owner)) +: calls.map(c => LiteralAnnotArg(Constant(c)))
        }
      }.toArray

      nodeCallsArgs
    }

  private def getOrComputeLoomLambdas(tree: Tree, loomAnno: AnnotationInfo): Array[ClassfileAnnotArg] =
    // manually written annotation arg, usually for debugging purposes
    getLoomLambdasArg(loomAnno).getOrElse {
      // NOTE: here we can assume that the receiver is the current symbol, so no need to track it
      val lambdaCalls = {
        val calls = new mutable.HashSet[Symbol]() // method
        for {
          apply @ Apply(_, args) <- tree
          // sadly apply.symbol.isMethod is not enough...
          if apply.symbol.isInstanceOf[MethodSymbol] && args.nonEmpty
        } {
          val method = apply.symbol.asMethod
          val List(params) = method.paramss // paramss has always size 1, since we're post-uncurry
          params.zip(args).foreach { case (param, t) =>
            if (hasNodeLiftAnno(param)) {
              for { sel @ Select(_, _) <- t } { calls.add(sel.symbol) }
            }
          }
        }
        calls.toSeq
      }

      val lambdaCallsArgs: Array[ClassfileAnnotArg] = {
        val sortedCalls = lambdaCalls.map(_.rawname.toString()).sorted
        sortedCalls.map(c => LiteralAnnotArg(Constant(c)))
      }.toArray

      lambdaCallsArgs
    }

}
