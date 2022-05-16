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
package rewrite

import java.nio.charset.Charset

import optimus.tools.scalacplugins.entity.reporter.OptimusPluginReporter

import scala.annotation.tailrec
import scala.collection.mutable
import scala.reflect.internal.Flags
import scala.reflect.internal.ModifierFlags
import scala.reflect.internal.util.Position
import scala.reflect.internal.util.SourceFile
import scala.reflect.internal.util.TriState
import scala.tools.nsc.Global
import scala.tools.nsc.Mode
import scala.tools.nsc.Phase
import scala.tools.nsc.plugins.PluginComponent
import scala.tools.nsc.transform.TypingTransformers

/**
 * Automated code rewrites and supporting infrastructure.
 */
class RewriteComponent(val pluginData: PluginData, val global: Global, val phaseInfo: OptimusPhaseInfo)
    extends PluginComponent
    with TypingTransformers
    with OptimusPluginReporter
    with WithOptimusPhase {
  import global._

  object names {}
  object tpnames {}
  def newPhase(prev: Phase): StdPhase = {
    new RewritePhase(prev)
  }

  class RewritePhase(prev: Phase) extends StdPhase(prev) {
    val encoding = Charset.forName(settings.encoding.value)
    val patchSets = mutable.Buffer[Patches]()
    override def run(): Unit = {
      super.run()
      if (!reporter.hasErrors) {
        if (patchSets.nonEmpty) {
          patchSets.foreach(_.overwriteSourceFile())
          reporter.echo("Patched: " + patchSets.map(_.source.file.name).mkString(","))
        }
      }
    }

    override def apply(unit: CompilationUnit): Unit = {
      val config = pluginData.rewriteConfig
      if (config.anyEnabled) {
        val underlyingFile = Patches
          .underlyingFile(unit.source)
          .getOrElse(return
          )
        val state = new RewriteState(ParseTree(unit.source))
        def go(bool: Boolean, trans: RewriteTypingTransformer) =
          if (bool) trans.transform(unit.body)
        go(config.rewriteCollectionSeq, new CollectionSeqTransformer(unit, state)) // Seq -> collection.Seq
        go(config.rewriteMapValues, new MapValuesRewriter(unit, state))
        go(
          config.rewriteBreakOutOps,
          new BreakoutToIteratorOp(unit, state)
        ) // c.op()(breakOut) => c.iterator.op().to(R)
        go(
          config.rewriteAsyncBreakOutOps,
          new AsyncBreakOutToCompanionBreakOut(unit, state)
        ) // collection.breakOut -> pack.Collection.breakOut
        go(config.rewriteToConversion, new ToConversion(unit, state))
        go(
          config.rewriteVarargsToSeq,
          new VarargsToSeq(unit, state)
        ) // f(xs: _*) -> f(xs.toSeq: _*) if xs is not immutable.Seq
        go(
          config.rewriteMapConcatWiden,
          new MapConcat(unit, state)
        ) // Map[A, X] ++ Map[B, Y]: add comment, 2.13 doesn't lub keys
        go(config.rewriteNilaryInfix, new NilaryInfixRewriter(unit, state)) // obj foo () -> obj.foo()
        go(config.unitCompanion, new UnitCompanion(unit, state)) // val x: Unit = Unit -> val x: Unit = ()
        go(config.anyFormatted, new AnyFormatted(unit, state)) // value.formatted("%fmt") -> f"$value%fmt"
        go(config.rewriteCaseClassToFinal, new CaseClassTransformer(unit, state))
        if (state.newImports.nonEmpty) new AddImports(unit, state).run(unit.body)
        patchSets += Patches(state.patches.toArray, unit.source, underlyingFile, encoding)
      }
    }
  }

  private class RewriteState(val parseTree: ParseTree) {
    val patches = mutable.ArrayBuffer.empty[Patch]
    val eliminatedBreakOuts = mutable.Set.empty[Tree]
    val newImports = mutable.Set.empty[NewImport]
  }

  sealed trait NewImport {
    def impString: String
    protected def sym: Symbol
    def exists: Boolean = sym.exists
    def matches(imp: Import): Boolean =
      imp.expr.tpe.termSymbol == sym &&
        imp.selectors.exists(_.name == nme.WILDCARD)
  }

  object CollectionCompatImport extends NewImport {
    protected lazy val sym = rootMirror.getPackageIfDefined("scala.collection.compat")
    val impString = "import scala.collection.compat._"
  }

  object OptimusCompatCollectionImport extends NewImport {
    protected lazy val sym = rootMirror.getPackageIfDefined("optimus.scalacompat.collection")
    val impString = "import optimus.scalacompat.collection._"
  }

  class ParseTree private (val tree: Tree, val index: collection.Map[Position, Tree])
  object ParseTree {
    def apply(source: SourceFile): ParseTree = {
      val unit = new CompilationUnit(source)
      unit.body = newUnitParser(unit).parse()
      val index = mutable.HashMap[Position, Tree]()
      unit.body.foreach { x =>
        if (!x.pos.isTransparent && x.pos.isRange) {
          index(x.pos) = x
          x match {
            case _: Ident =>
              index(x.pos.focusStart) =
                x // Needed for @storable @entity class C(val x: Seq[Int]), maybe fixable in the entity plugin instead?
            case _ =>
          }
        }
      }
      new ParseTree(unit.body, index)
    }
  }

  // Applied.unapply matches any tree, not just applications
  private object Application {
    def unapply(t: GenericApply): Some[(Tree, List[Tree], List[List[Tree]])] = {
      val applied = treeInfo.dissectApplied(t)
      Some((applied.core, applied.targs, applied.argss))
    }
  }

  // Select.unapply returns names, not symbols
  private object SelectSym {
    def unapply(sel: Select): Some[(Tree, Symbol)] = Some((sel.qualifier, sel.symbol))
  }

  private class RewriteTypingTransformer(unit: CompilationUnit) extends TypingTransformer(unit) {
    // position for new imports: after last import at the top of the file (after the package clause)
    var lastTopLevelContext: analyzer.Context = analyzer.NoContext
    var topLevelImportPos: Position = unit.source.position(0)
    setLastImport(List(unit.body))

    lazy val collectionSeqModule = rootMirror.getRequiredModule("scala.collection.Seq")
    lazy val collectionIndexedSeqModule = rootMirror.getRequiredModule("scala.collection.IndexedSeq")

    private def isPackageObjectDef(pd: PackageDef) = pd.stats match {
      case List(m: ModuleDef) => m.symbol.isPackageObject
      case _                  => false
    }

    def setLastImport(trees: List[Tree]): Unit = trees match {
      case (pd: PackageDef) :: xs if !isPackageObjectDef(pd) =>
        topLevelImportPos = pd.pid.pos.focusEnd
        lastTopLevelContext = localTyper.context
        setLastImport(pd.stats ::: xs)

      case (imp: Import) :: xs =>
        localTyper.context = localTyper.context.make(imp)
        val context = localTyper.context
        if (context.enclClass.owner.hasPackageFlag) {
          lastTopLevelContext = context
          topLevelImportPos = imp.pos.focusEnd
        }
        setLastImport(xs)

      case _ =>
    }

    private object MacroExpansion {
      def unapply(t: Tree): Option[Tree] = t.attachments.get[analyzer.MacroExpansionAttachment].map(_.expandee)
    }

    override def transform(t: Tree): Tree = t match {
      case MacroExpansion(expandee) =>
        super.transform(expandee)
      case Typed(expr, tt @ TypeTree()) =>
        val origToVisit = tt.original match {
          case null              => None
          case Annotated(ann, _) =>
            // for `expr: @ann`, the typer creates a `Typed(expr, tp)` tree. don't traverse `expr` twice
            Some(ann)
          case o if (expr.pos.end < o.pos.start) => Some(o)
          case _                                 => None
        }
        origToVisit.foreach(transform)
        transform(expr)
      case tt: TypeTree if tt.original != null =>
        val saved = tt.original.tpe
        tt.original.setType(tt.tpe)
        try transform(tt.original)
        finally tt.original.setType(saved)
      case Block(stats, expr) =>
        val entered = mutable.ListBuffer[Symbol]()
        val saved = localTyper.context
        def enter(sym: Symbol) = {
          entered += sym
          localTyper.context.scope.enter(sym)
        }
        try {
          val stats1 = stats.mapConserve { stat =>
            stat match {
              case md: MemberDef =>
                val sym = md.symbol
                if (sym != NoSymbol)
                  enter(stat.symbol)
              case imp: Import =>
                localTyper.context = localTyper.context.make(imp)
              case _ =>
            }
            transform(stat)
          }
          val expr1 = transform(expr)
          treeCopy.Block(t, stats1, expr1)
        } finally {
          entered.foreach(saved.scope.unlink)
          localTyper.context = saved
        }
      case dd: DefDef =>
        if (dd.symbol.isSynthetic) t
        else {
          localTyper.reenterTypeParams(dd.tparams)
          localTyper.reenterValueParams(dd.vparamss)
          try super.transform(t)
          finally {
            val scope = localTyper.context.scope
            dd.tparams.foreach(t => scope.unlink(t.symbol))
            mforeach(dd.vparamss)(t => scope.unlink(t.symbol))
          }
        }
      case cd: ClassDef =>
        localTyper.reenterTypeParams(cd.tparams)
        try super.transform(t)
        finally {
          val scope = localTyper.context.scope
          cd.tparams.foreach(t => scope.unlink(t.symbol))
        }
      case _ =>
        super.transform(t)
    }

    /**
     * traverse qualifier, type args and argss of an application. calling `super.traverse(tree)` can lead to the same
     * case triggering again, e.g., in `Apply(TypeApply(f, targs), args)`, the `Apply` and `TypeApply` trees might both
     * match.
     */
    protected def traverseApplicationRest(tree: Tree): Unit = tree match {
      case Application(fun, targs, argss) =>
        fun match {
          case Select(qual, _) => transform(qual)
          case _               =>
        }
        transformTrees(targs)
        argss.foreach(transformTrees)
      case _ =>
    }

    def silentTyped(tree: Tree, mode: Mode): util.Try[Tree] = {
      val typer = localTyper
      val result = typer.silent(_.typed(tree, mode))
      result match {
        case analyzer.SilentResultValue(tree: Tree) =>
          util.Success(tree)
        case analyzer.SilentTypeError(err) =>
          util.Failure(new TypeError(err.errPos, err.errMsg))
      }
    }

    def qualifiedSelectTerm(sym: Symbol): String = {
      // don't emit plain `Seq`
      if (sym == collectionSeqModule || sym == collectionIndexedSeqModule)
        s"${qualifiedSelectTerm(sym.enclosingPackage)}.${sym.name}"
      else {
        val errors = mutable.Buffer[Throwable]()
        val parts = ("_root_." + sym.fullName).split("\\.")
        val paths = List.tabulate(parts.length)(i => parts.takeRight(i + 1).mkString("."))
        paths
          .find { path =>
            val ref = newUnitParser(newCompilationUnit(path)).parseRule(_.expr())
            silentTyped(ref, Mode.QUALmode) match {
              case util.Failure(t)     => errors += t; false
              case util.Success(typed) => typed.tpe.termSymbol == sym
            }
          }
          .getOrElse {
            reporter.error(NoPosition, s"Unable to build short qualified path for $sym: $errors")
            sym.fullName
          }
      }
    }

    def codeOf(pos: Position) = Patches.codeOf(pos, unit.source)

    def withEnclosingParens(pos: Position): Position = {
      @tailrec def skip(offset: Int, inc: Int): Int =
        if (unit.source.content(offset).isWhitespace) skip(offset + inc, inc) else offset

      val closingPos = skip(pos.end, 1)
      val closing = unit.source.content(closingPos)

      def checkOpening(expected: Char) = {
        val openingPos = skip(pos.start - 1, -1)
        val opening = unit.source.content(openingPos)
        if (opening == expected) withEnclosingParens(pos.withStart(openingPos).withEnd(closingPos + 1))
        else pos
      }
      if (closing == ')') checkOpening('(')
      else if (closing == '}') checkOpening('{')
      else pos
    }

    def isInfix(tree: Tree, parseTree: ParseTree): TriState = tree match {
      case sel: Select =>
        // look at parse tree; e.g. `Foo(arg)` in source would have AST `pack.Foo.apply(arg)`, so it's a Select after
        // typer. We should not use the positions of the typer trees to go back to the source.
        parseTree.index.get(sel.pos) match {
          case Some(fun: Select) =>
            val qualEnd = withEnclosingParens(fun.qualifier.pos).end
            val c = unit.source.content(unit.source.skipWhitespace(qualEnd))
            c != '.'
          case _ => TriState.Unknown
        }
      case Application(fun, _, _) => isInfix(fun, parseTree)
      case _                      => TriState.Unknown
    }

    /**
     * Select `.code`, wrap in parens if it's infix. Example:
     *   - tree: `coll.mapValues[T](fun)`
     *   - code: `toMap`
     *
     * `tree` could be infix `coll mapValues fun` in source.
     *
     * `reuseParens`: if `tree` already has parens around it, whether to insert new parens or not. example:
     *   - `foo(coll mapValues fun)` => cannot reuse parens, need `foo((coll mapValues fun).toMap)`
     *   - `(col map f).map(g)(breakOut)` => can reuse parens, `(col map f).iterator.map(g).to(T)`
     */
    def selectFromInfix(tree: Tree, code: String, parseTree: ParseTree, reuseParens: Boolean): List[Patch] = {
      val patches = mutable.ListBuffer[Patch]()
      val posWithParens = if (reuseParens) withEnclosingParens(tree.pos) else tree.pos
      val needParens = isInfix(tree, parseTree) == TriState.True && posWithParens.end == tree.pos.end
      if (needParens) {
        patches += Patch(tree.pos.focusStart, "(")
        patches += Patch(tree.pos.focusEnd, ")." + code)
      } else {
        patches += Patch(posWithParens.focusEnd, "." + code)
      }
      patches.toList
    }
  }

  class TypeRenderer(rewriteTypingTransformer: RewriteTypingTransformer) extends TypeMap {
    override def apply(tp: Type): Type = tp match {
      case SingleType(pre, sym) if tp.prefix.typeSymbol.isOmittablePrefix =>
        adjust(tp, pre, sym, Mode.QUALmode)((pre1, sym1) => SingleType(pre1, sym1))
      case TypeRef(pre, sym, args) =>
        val args1 = args.mapConserve(this)
        adjust(tp, pre, sym, Mode.TAPPmode | Mode.FUNmode)((pre1, sym1) => TypeRef(pre1, sym1, args1))
      case _ =>
        mapOver(tp)
    }

    def adjust(tp: Type, pre: Type, sym: Symbol, mode: Mode)(f: (Type, Symbol) => Type): Type = {
      if (pre.typeSymbol.isOmittablePrefix || global.shorthands.contains(sym.fullName)) {
        val typedTree = rewriteTypingTransformer.silentTyped(Ident(sym.name), mode).getOrElse(EmptyTree)
        if (typedTree.symbol == sym || sym.tpeHK =:= typedTree.tpe)
          f(NoPrefix, sym)
        else {
          val dummyOwner = NoSymbol.newClassSymbol(TypeName(pre.typeSymbol.fullName))
          dummyOwner.setInfo(ThisType(dummyOwner))
          val pre1 = pre match {
            case ThisType(_) | SingleType(_, _) => SingleType(NoPrefix, dummyOwner)
            case _                              => TypeRef(NoPrefix, dummyOwner, Nil)
          }
          f(pre1, sym.cloneSymbol(dummyOwner))
        }
      } else {
        mapOver(tp)
      }
    }
  }

  private class MethodMatcher(symbols: Symbol*) {
    private val byName = symbols.groupBy(_.name)
    def apply(sym: Symbol): Boolean =
      byName.get(sym.name).flatMap(_.find(sameName => sym.overrideChain.contains(sameName))).nonEmpty
    def unapply(sym: Symbol): Boolean = apply(sym)
  }
  // Rewrites

  private object BreakoutInfo {
    lazy val breakOutSym =
      definitions.getMemberMethod(rootMirror.getPackageObject("scala.collection"), TermName("breakOut"))

    lazy val toSym =
      definitions.getMemberMethod(rootMirror.getClassIfDefined("scala.collection.GenTraversableOnce"), TermName("to"))

    lazy val GenIterableLikeSym = rootMirror.getRequiredClass("scala.collection.GenIterableLike")
    lazy val CollectionMapSym = rootMirror.requiredClass[collection.Map[_, _]]
    lazy val ImmutableMapSym = rootMirror.requiredClass[collection.immutable.Map[_, _]]
    lazy val CollectionSetSym = rootMirror.requiredClass[collection.Set[_]]
    lazy val ImmutableSetSym = rootMirror.requiredClass[collection.immutable.Set[_]]
    lazy val CollectionSeqSym = rootMirror.requiredClass[collection.Seq[_]]
    lazy val ImmutableSeqSym = rootMirror.requiredClass[collection.immutable.Seq[_]]
    lazy val CollectionIndexedSeqSym = rootMirror.requiredClass[collection.IndexedSeq[_]]
    lazy val ImmutableIndexedSeqSym = rootMirror.requiredClass[collection.immutable.IndexedSeq[_]]
    lazy val ImmutableListSym = rootMirror.requiredClass[collection.immutable.List[_]]
    lazy val ImmutableVectorSym = rootMirror.requiredClass[collection.immutable.Vector[_]]
    lazy val ArraySym = definitions.ArrayClass
    lazy val CollectionBitSetSym = rootMirror.requiredClass[collection.BitSet]

    lazy val directConversions: Map[Symbol, String] = Map(
      CollectionMapSym -> "toMap",
      ImmutableMapSym -> "toMap",
      CollectionSetSym -> "toSet",
      ImmutableSetSym -> "toSet",
      // Don't use toSeq, in 2.12 it calls toStream (unless overridden)
      // Due to `fallbackStringCanBuildFrom`, `to[Seq]` and `(breakOut): Seq` build an IndexedSeq
      CollectionSeqSym -> "toIndexedSeq",
      ImmutableSeqSym -> "toIndexedSeq",
      CollectionIndexedSeqSym -> "toIndexedSeq",
      ImmutableIndexedSeqSym -> "toIndexedSeq",
      ImmutableListSym -> "toList",
      ImmutableVectorSym -> "toVector",
      ArraySym -> "toArray"
    )

    lazy val OptimusAsyncBaseSym = rootMirror.getClassIfDefined("optimus.platform.AsyncBase")

    def isInferredArg(tree: Tree): Boolean = tree match {
      case tt: TypeTree => tt.original eq null
      case _ =>
        val pos = tree.pos
        pos.isOffset && tree.forAll(t => {
          val tpos = t.pos
          tpos == NoPosition || tpos.isOffset && tpos.point == pos.point
        })
    }
  }

  private class AsyncBreakOutToCompanionBreakOut(unit: CompilationUnit, state: RewriteState)
      extends RewriteTypingTransformer(unit) {
    import BreakoutInfo._

    private def needImport(companion: Symbol) = companion.name.toString match {
      case "OptimusSeq" | "OptimusDoubleSeq" => false
      case _                                 => true
    }

    override def transform(tree: Tree): Tree = tree match {
      case _ if breakOutSym == NoSymbol || OptimusAsyncBaseSym == NoSymbol => tree

      case Application(Select(coll, _), _, _ :+ List(bo @ Application(boFun, boTargs, _)))
          if boFun.symbol == breakOutSym && coll.tpe.typeSymbol.isNonBottomSubClass(OptimusAsyncBaseSym) =>
        val companion = boTargs.last.tpe.typeSymbol.companionModule
        if (companion.exists) {
          val breakOutStr = qualifiedSelectTerm(companion) + ".breakOut"
          state.patches += Patch(bo.pos, breakOutStr)
          if (needImport(companion))
            state.newImports += OptimusCompatCollectionImport
        }
        super.transform(tree)

      case _ => super.transform(tree)
    }
  }

  private class BreakoutToIteratorOp(unit: CompilationUnit, state: RewriteState)
      extends RewriteTypingTransformer(unit) {
    import BreakoutInfo._
    // not `++:`, the method doesn't exist on Iterator
    // could use `.view`, but `++:` is deprecated in Iterable on 2.13 (not in Seq), so probably not worth it
    val breakOutMethods = Set("map", "collect", "flatMap", "++", "scanLeft", "zip", "zipAll")

    val Predef_fallbackStringCanBuildFrom = definitions.PredefModule.info.member(TermName("fallbackStringCanBuildFrom"))

    // coll.fun[targs](args)(breakOut) --> coll.iterator.fun[targs](args).to(Target)
    override def transform(tree: Tree): Tree = tree match {
      case Application(Select(coll, funName), _, argss :+ List(bo @ Application(boFun, boTargs, List(List(boArg)))))
          if boFun.symbol == breakOutSym =>
        if (
          coll.tpe.typeSymbol.isNonBottomSubClass(GenIterableLikeSym) &&
          breakOutMethods.contains(funName.decode)
        ) {
          def patch(conversion: String): Unit = {
            state.patches ++= selectFromInfix(coll, "iterator", state.parseTree, reuseParens = true)
            state.patches += Patch(withEnclosingParens(bo.pos), conversion)
            if (funName.startsWith("zip"))
              state.patches ++= selectFromInfix(argss.head.head, "iterator", state.parseTree, reuseParens = false)
            state.eliminatedBreakOuts += bo
          }
          boArg match {
            case Application(fun, _, _) if fun.symbol == Predef_fallbackStringCanBuildFrom =>
              patch(".toIndexedSeq")

            case _ =>
              val targetClass = boTargs.last.tpe.typeSymbol
              val companion = targetClass.companionModule
              if (companion.exists) {
                val conversion = directConversions.get(targetClass) match {
                  case Some(conv) => s".$conv"
                  case _ =>
                    val convertMethod =
                      if (
                        targetClass.isNonBottomSubClass(CollectionMapSym) || targetClass.isNonBottomSubClass(
                          CollectionBitSetSym)
                      ) {
                        state.newImports += OptimusCompatCollectionImport
                        "convertTo"
                      } else "to"
                    state.newImports += CollectionCompatImport
                    s".$convertMethod(${qualifiedSelectTerm(companion)})"
                }
                patch(conversion)
              }
          }
        }
        traverseApplicationRest(tree)
        tree
      case _ =>
        super.transform(tree)
    }
  }

  private class ToConversion(unit: CompilationUnit, state: RewriteState) extends RewriteTypingTransformer(unit) {
    import BreakoutInfo._
    val isToMethod = new MethodMatcher(toSym)
    override def transform(tree: Tree): Tree = tree match {
      case Application(fun, targs, _) if isToMethod(fun.symbol) && !isInferredArg(targs.head) =>
        val targetClass = targs.head.tpe.typeSymbol
        val companion = targetClass.companionModule
        if (companion.exists) {
          val conversion = directConversions.getOrElse(targetClass, {
            state.newImports += CollectionCompatImport
            s"to(${ qualifiedSelectTerm(companion) })"
          })
          state.patches += Patch(tree.pos.withStart(fun.pos.point), conversion)
        }
        traverseApplicationRest(tree)
        tree
      case _ =>
        super.transform(tree)
    }
  }

  private class VarargsToSeq(unit: CompilationUnit, state: RewriteState) extends RewriteTypingTransformer(unit) {
    val CollectionImmutableSeq = rootMirror.requiredClass[scala.collection.immutable.Seq[_]]
    val CollectionSeq = rootMirror.requiredClass[scala.collection.Seq[_]]

    val isToSeq = new MethodMatcher(
      rootMirror.requiredClass[scala.collection.GenTraversableOnce[_]].info.decl(TermName("toSeq")))
    def addToSeq(arg: Tree) =
      !arg.tpe.typeSymbol.isNonBottomSubClass(CollectionImmutableSeq) &&
        arg.tpe.typeSymbol.isNonBottomSubClass(CollectionSeq) &&
        !PartialFunction.cond(arg) {
          case Ident(_)     => definitions.isScalaRepeatedParamType(arg.symbol.tpe)
          case Select(_, _) => isToSeq(arg.symbol)
        }

    var currentFun: Symbol = null
    def withCurrent[T](fun: Symbol)(op: => T): T = {
      val old = currentFun
      currentFun = fun
      try op
      finally currentFun = old
    }

    override def transform(tree: Tree): Tree = tree match {
      case Typed(expr, Ident(tpnme.WILDCARD_STAR)) if addToSeq(expr) =>
        val op = if (currentFun.isJavaDefined) "toArray" else "toSeq"
        state.patches ++= selectFromInfix(expr, op, state.parseTree, reuseParens = true)
        super.transform(tree)
      case Application(fun, _, _) =>
        withCurrent(fun.symbol)(super.transform(tree))
      case _ =>
        super.transform(tree)
    }
  }

  // rewrites non final non sealed abstract case classes to have the final
  private class CaseClassTransformer(unit: CompilationUnit, state: RewriteState) extends RewriteTypingTransformer(unit) {
    var inClass = false
    private def alarmOnCaseClass(mods: Modifiers): Boolean =
      !inClass && mods.isCase && !(mods.isSealed && mods.hasAbstractFlag) && !mods.isFinal

    override def transform(tree: Tree): Tree = {
      tree match {
        case ClassDef(mods, _, _, _) if alarmOnCaseClass(mods) =>
          // these lines do the same thing as mods.calculateFlagString but put the access modifiers first

          // adding final flag and removing sealed flag
          val basis = (mods.flags & ~ModifierFlags.SEALED) | ModifierFlags.FINAL
          val accessString = mods.accessString
          val newModsStringAccessFirst =
            s"${accessString}${if (accessString.length == 0) "" else " "}${mods.flagBitsToString(basis & ~ModifierFlags.AccessFlags)}"
          val newPos = tree.pos.withEnd(tree.pos.focus.end)
          state.patches += Patch(newPos, s"$newModsStringAccessFirst class ")
          tree

        case _: ClassDef =>
          val oldInClass = inClass
          inClass = true
          super.transform(tree)
          inClass = oldInClass
          tree

        case _ => super.transform(tree)
      }
    }
  }

  /**
   * Rewrites Idents that refer to scala.Seq/IndexedSeq as collection.Seq (or scala.collection.Seq if qualification is
   * needed)
   */
  private class CollectionSeqTransformer(unit: CompilationUnit, state: RewriteState)
      extends RewriteTypingTransformer(unit) {
    case class Rewrite(name: String, typeAlias: Symbol, termAlias: Symbol, cls: Symbol, module: Symbol)
    val ScalaCollectionPackage = rootMirror.getPackage("scala.collection")
    def rewrite(name: String) =
      Rewrite(
        name,
        definitions.ScalaPackage.packageObject.info.decl(TypeName(name)),
        definitions.ScalaPackage.packageObject.info.decl(TermName(name)),
        rootMirror.getRequiredClass("scala.collection." + name),
        rootMirror.getRequiredModule("scala.collection." + name)
      )
    val rewrites = List(rewrite("Seq"), rewrite("IndexedSeq"))
    override def transform(tree: Tree): Tree = {
      tree match {
        case ref: RefTree =>
          for (rewrite <- rewrites) {
            val sym = ref.symbol
            if (sym == rewrite.cls || sym == rewrite.module || sym == rewrite.termAlias || sym == rewrite.typeAlias) {
              state.parseTree.index.get(ref.pos) match {
                case Some(Ident(name)) if name.string_==(rewrite.name) =>
                  val qual: String = qualifiedSelectTerm(ScalaCollectionPackage)
                  val patchCode = qual + "." + rewrite.name
                  state.patches += Patch(ref.pos, patchCode)
                case _ =>
              }
            }
          }
        case _ =>
      }
      super.transform(tree)
    }
  }

  /** Add `import scala.collection.compat._` at the top-level */
  private class AddImports(unit: CompilationUnit, state: RewriteState) extends RewriteTypingTransformer(unit) {
    def run(tree: Tree) = {
      // RewriteTypingTransformer.setLastImport sets up the required state (lastTopLevelContext, topLevelImportPos)
      transform(tree)
      val topLevelImports = collectTopLevel
      val toAdd = state.newImports.filterNot(newImp => topLevelImports.exists(newImp.matches))
      if (toAdd.nonEmpty) {
        val top = topLevelImportPos.point == 0
        val imps = toAdd.map(_.impString).toList.sorted.mkString(if (top) "" else "\n", "\n", if (top) "\n" else "")
        state.patches += Patch(topLevelImportPos, imps)
      }
    }

    private def collectTopLevel: List[Import] = lastTopLevelContext match {
      case analyzer.NoContext =>
        Nil
      case ctx =>
        ctx.enclosingContextChain.iterator
          .map(_.tree)
          .collect { case imp: Import =>
            imp
          }
          .toList
    }
  }

  private class MapValuesRewriter(unit: CompilationUnit, state: RewriteState) extends RewriteTypingTransformer(unit) {
    val GenTravLike = rootMirror.getRequiredClass("scala.collection.GenTraversableLike")
    val GenMapLike = rootMirror.getRequiredClass("scala.collection.GenMapLike")
    val GenTravOnce = rootMirror.requiredClass[scala.collection.GenTraversableOnce[_]]

    val GroupBy = new MethodMatcher(GenTravLike.info.decl(TermName("groupBy")))
    val FilterKeys = new MethodMatcher(GenMapLike.info.decl(TermName("filterKeys")))
    val MapApply = new MethodMatcher(GenMapLike.info.decl(nme.apply))
    val MapMethod = new MethodMatcher(GenTravLike.info.decl(nme.map))
    val MapValues = new MethodMatcher(GenMapLike.info.decl(TermName("mapValues")))
    val ToMap = new MethodMatcher(GenTravOnce.info.decl(TermName("toMap")))

    // no need to add `toMap` if it's already there, or in `m.mapValues(f).apply(x)`
    // curTree is the next outer tree (tracked by TypingTransformer)
    def skipRewrite = PartialFunction.cond(curTree) { case SelectSym(_, ToMap() | MapApply()) =>
      true
    }

    private object IsGroupMap {
      def unapply(tree: Tree): Option[(Select, Select, Function, Tree)] = tree match {
        case SelectSym(tree, ToMap()) => unapply(tree)
        case Application(
              mapValues @ SelectSym(Application(groupBy @ SelectSym(rec, GroupBy()), _, _), MapValues()),
              _,
              List(
                List(
                  map @ Function(
                    List(mapValuesParam),
                    Application(mapMeth @ SelectSym(mapQual, MapMethod()), _, List(List(_), _*))))))
            if mapValuesParam.symbol == mapQual.symbol =>
          Some((groupBy, mapValues, map, mapMeth))
        case _ => None
      }
    }

    override def transform(tree: Tree): Tree = tree match {
      case IsGroupMap(groupBy, mapValues, map, mapMeth) if CollectionCompatImport.exists =>
        // xs.groupBy(key).mapValues(_.map(fun)).toMap            ==>  xs.groupMap(key)(fun)
        // xs.groupBy(key).mapValues { xs => xs.map(f) }          ==>  xs.groupMap(key)(f)
        // xs.groupBy(key).mapValues(xs => xs.map { x => f(x) })  ==>  xs.groupMap(key) { x => f(x) }
        // Apply(Select(
        //     Apply(
        //       Select(xs, groupBy),           // xs.groupBy
        //       key),                          // xs.groupBy(key)
        //     mapValues),                      // xs.groupBy(key).mapValues
        //   Apply(
        //     Select(_, map),                  // _.map
        //     fun)                             // _.map(fun)
        // )                                    // xs.groupBy(key).mapValues(_.map(fun))
        def Pos(start: Int, end: Int) = Position.range(unit.source, start, start, end)
        state.patches ++= {
          selectFromInfix(groupBy.qualifier, "groupMap", state.parseTree, reuseParens = true) match {
            case ps :+ p =>
              ps :+ p.copy(span = p.span.withEnd(groupBy.pos.end).withStart {
                if (isInfix(groupBy, state.parseTree) == TriState.True) p.span.start
                else unit.source.skipWhitespace(p.span.start)
              })
          }
        } // replace ".groupBy" with ".groupMap"
        state.patches += Patch(
          Pos(mapValues.qualifier.pos.end, mapMeth.pos.end),
          ""
        ) // remove  ".mapValues { xs => xs.map"  (eating leading whitespace)
        state.patches += Patch(Pos(map.pos.end, tree.pos.end), "") // remove  "}" or ").toMap"
        state.newImports += CollectionCompatImport
        traverseApplicationRest(tree)
        tree
      case Application(sel @ SelectSym(_, MapValues() | FilterKeys()), _, _) =>
        if (!skipRewrite) {
          if (OptimusCompatCollectionImport.exists && pluginData.rewriteConfig.useOptimusCompat) {
            state.patches += Patch(sel.pos.focusEnd, "Now")
            state.newImports += OptimusCompatCollectionImport
          } else
            state.patches ++= selectFromInfix(tree, code = "toMap", state.parseTree, reuseParens = false)
        }
        traverseApplicationRest(tree)
        tree
      case _ =>
        super.transform(tree)
    }
  }

  private class NilaryInfixRewriter(unit: CompilationUnit, state: RewriteState) extends RewriteTypingTransformer(unit) {
    override def transform(tree: Tree): Tree = tree match {
      case Application(fun: Select, _, List(Nil)) if isInfix(fun, state.parseTree) == TriState.True =>
        state.patches ++= {
          // skip the whitespace, so `qual foo ()` doesn't become `qual. foo ()`
          selectFromInfix(fun.qualifier, "", state.parseTree, reuseParens = true) match {
            case ps :+ p => ps :+ p.copy(span = p.span.withEnd(unit.source.skipWhitespace(p.span.end)))
          }
        }
        state.patches += Patch(fun.pos.focusEnd.withEnd(unit.source.skipWhitespace(fun.pos.end)), "")
        super.transform(tree)
      case _ =>
        super.transform(tree)
    }
  }
  private class UnitCompanion(unit: CompilationUnit, state: RewriteState) extends RewriteTypingTransformer(unit) {
    val unitModule = definitions.UnitClass.companionModule
    override def transform(tree: Tree): Tree = tree match {
      case Application(sel: Select, targs, argss) if sel.qualifier.symbol == unitModule =>
        transformTrees(targs)
        argss.foreach(transformTrees)
        state.patches += Patch(tree.pos.focusEnd, " /*TODO-2.13-migration Unit companion*/")
        tree
      case _ if tree.symbol == unitModule =>
        state.patches += Patch(tree.pos, "()")
        tree
      case _ =>
        super.transform(tree)
    }
  }

  private class AnyFormatted(unit: CompilationUnit, state: RewriteState) extends RewriteTypingTransformer(unit) {
    val formattedMethod =
      definitions.PredefModule.info.member(TypeName("StringFormat")).info.decl(TermName("formatted"))

    override def transform(tree: global.Tree): global.Tree = {
      tree match {
        case Application(fun: Select, _, List(List(arg))) if fun.symbol == formattedMethod =>
          val value = codeOf(fun.qualifier.pos)
          val valueParseTree = state.parseTree.index.get(fun.qualifier.pos)
          val valueIsIdent = valueParseTree.exists(_.isInstanceOf[Ident])
          val valueIsPlaceholderFunction = valueParseTree.toList.flatMap(_.collect {
            case id: Ident if id.name.startsWith(nme.FRESH_TERM_NAME_PREFIX) => id
          }) match {
            case List(id) => codeOf(id.pos) == "_"
            case _        => false
          }
          val argValue = if (valueIsPlaceholderFunction) "fmtValue" + value.substring(1) else value

          val format = {
            val f = codeOf(arg.pos)
            if (isInfix(arg, state.parseTree) == TriState.True) s"($f)" else f
          }
          val formatLiteral = arg match {
            case Literal(Constant(s: String)) => Some(s)
            case _                            => None
          }
          val formatIsString = formatLiteral.contains("%s")

          val replacement =
            if (formatIsString) argValue
            else
              formatLiteral match {
                case Some(f) =>
                  s"""f"$$${if (valueIsIdent) argValue else s"{$argValue}"}$f""""
                case _ =>
                  s"$format.format($argValue)"
              }
          state.patches += Patch(
            tree.pos,
            if (valueIsPlaceholderFunction) "fmtValue => " + replacement else replacement)
          tree
        case _ =>
          super.transform(tree)
      }
    }
  }

  private class MapConcat(unit: CompilationUnit, state: RewriteState) extends RewriteTypingTransformer(unit) {
    val TraversableLike_++ =
      rootMirror.getClassIfDefined("scala.collection.TraversableLike").info.decl(TermName("++").encodedName)

    override def transform(tree: Tree): Tree =
      if (TraversableLike_++ == NoSymbol) tree
      else {
        tree match {
          case Application(fun @ Select(qual, _), targs, argss)
              if fun.symbol == TraversableLike_++ &&
                qual.tpe.typeSymbol.isNonBottomSubClass(symbolOf[collection.Map[Any, Any]]) =>
            val renderer = new TypeRenderer(this)
            val k1 = targs.head.tpe.typeArgs.head
            val k = qual.tpe.baseType(symbolOf[collection.Map[Any, Any]]).typeArgs.head.upperBound
            if (!(k1 <:< k)) {
              state.patches += Patch(fun.pos.focusEnd, s"/* TODO_213: K=${renderer(k)} K1=${renderer(k1)}*/")
            }
            super.transform(tree)
          case _ =>
            super.transform(tree)
        }
        tree
      }
  }
}
