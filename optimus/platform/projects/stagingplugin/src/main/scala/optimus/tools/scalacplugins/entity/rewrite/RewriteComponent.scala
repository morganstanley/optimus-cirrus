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

import optimus.tools.scalacplugins.entity.OptimusPhaseInfo
import optimus.tools.scalacplugins.entity.PluginData
import optimus.tools.scalacplugins.entity.WithOptimusPhase
import optimus.tools.scalacplugins.entity.reporter.OptimusPluginReporter

import scala.collection.GenTraversableOnce
import scala.collection.mutable
import scala.reflect.internal.util.Position
import scala.reflect.internal.util.SourceFile
import scala.tools.nsc.Global
import scala.tools.nsc.Mode
import scala.tools.nsc.Phase
import scala.tools.nsc.ast.parser.Tokens
import scala.tools.nsc.plugins.PluginComponent
import scala.tools.nsc.transform.TypingTransformers

/**
 * Automated code rewrites and supporting infrastructure.
 */
class RewriteComponent(
    val pluginData: PluginData,
    val global: Global,
    val phaseInfo: OptimusPhaseInfo,
    stagingEnabled: => Boolean = true)
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
        val patches = mutable.LinkedHashSet[Patch]()
        val parseTree = ParseTree(unit.source)
        if (config.rewriteCollectionSeq) {
          val rewriter = new CollectionSeqRewriter(unit, parseTree, patches)
          rewriter.transform(unit.body)
        }

        if (config.rewriteMapValues) {
          val rewriter = new MapValuesRewriter(unit, parseTree, patches)
          rewriter.transform(unit.body)
        }
        if (config.rewriteBreakOutArgs) {
          val rewriter = new BreakOutArgsRewriter(unit, parseTree, patches)
          rewriter.transform(unit.body)
        }
        if (config.rewriteVarargsToSeq) {
          val rewriter = new VarargsToSeq(unit, patches)
          rewriter.transform(unit.body)
        }
        patchSets += Patches(patches.toSeq, unit.source, encoding)
      }
    }
  }

  private class CollectionSeqRewriter(
      unit: CompilationUnit,
      parseTree: ParseTree,
      patches: mutable.LinkedHashSet[Patch])
      extends RewriteTypingTransformer(unit) {
    val ScalaCollectionPackage = rootMirror.getPackage("scala.collection")
    case class Rewrite(name: String, typeAlias: Symbol, termAlias: Symbol, cls: Symbol, module: Symbol)
    object Rewrite {
      def apply(name: String): Rewrite = {
        Rewrite(
          name,
          definitions.ScalaPackage.packageObject.info.decl(TypeName(name)),
          definitions.ScalaPackage.packageObject.info.decl(TermName(name)),
          rootMirror.getRequiredClass("scala.collection." + name),
          rootMirror.getRequiredModule("scala.collection." + name),
        )
      }
    }
    val rewrites = List(Rewrite("Seq"), Rewrite("IndexedSeq"))
    override def transform(tree: Tree): Tree = {
      tree match {
        case ref: RefTree =>
          val sym = ref.symbol
          for (rewrite <- rewrites) {
            if (sym == rewrite.cls || sym == rewrite.module || sym == rewrite.termAlias || sym == rewrite.typeAlias) {
              parseTree.index.get(ref.pos) match {
                case Some(sourceTree @ Ident(name)) if name.string_==(rewrite.name) =>
                  val qual: String =
                    chooseQualiferExpr(ref.pos, List("collection", "scala.collection", "_root_.scala.collection")) {
                      tree =>
                        tree.tpe.termSymbol == ScalaCollectionPackage
                    }
                  val patchCode = qual + "." + rewrite.name

                  patches += Patch(sourceTree.pos, patchCode)
                case _ =>
              }
            }
          }
        case _ =>
      }
      super.transform(tree)
    }
  }
  private class MapValuesRewriter(unit: CompilationUnit, parseTree: ParseTree, patches: mutable.LinkedHashSet[Patch])
      extends RewriteTypingTransformer(unit) {
    val GenMapLike_mapValues =
      rootMirror.getRequiredClass("scala.collection.GenMapLike").info.decl(TermName("mapValues"))
    val GenTraversableOnce_toMap = rootMirror.requiredClass[GenTraversableOnce[_]].info.decl(TermName("toMap"))
    private def overrrides(sym: Symbol, base: Symbol) =
      sym.name == base.name && sym.overriddenSymbol(base.owner) == base

    override def transform(tree: Tree): Tree = {
      tree match {
        case Select(Apply(fun, args), _) if overrrides(tree.symbol, GenTraversableOnce_toMap) =>
          transform(fun)
          transformTrees(args)
        case ap @ Apply(TypeApply(s: Select, targs), args)
            if tree.symbol.name == GenMapLike_mapValues.name && tree.symbol.overrideChain.contains(
              GenMapLike_mapValues) =>
          reporter.warning(tree.pos, show(localTyper.context.enclMethod.tree, printPositions = true))
          patches ++= patchSelect(s, ap, "toMap")
        case _ =>
          super.transform(tree)
      }
      tree
    }
  }

  private class BreakOutArgsRewriter(unit: CompilationUnit, parseTree: ParseTree, patches: mutable.LinkedHashSet[Patch])
      extends RewriteTypingTransformer(unit) {
    val ScalaCollection_breakOut = rootMirror.getPackageObject("scala.collection").info.decl(TermName("breakOut"))

    override def transform(tree: Tree): Tree =
      if (ScalaCollection_breakOut == NoSymbol) tree
      else {
        tree match {
          case Application(fun, targs, argss) if fun.symbol == ScalaCollection_breakOut =>
            val inferredBreakOut = targs.forall(isInferredArg) && mforall(argss)(isInferredArg)
            if (inferredBreakOut) {
              val renderer = new TypeRenderer(this)
              if (targs.isEmpty) {
                globalError(tree.pos, "empty targs: " + (tree, targs))
              } else {
                val companionStr = renderer.apply(targs.last.tpe.typeSymbol.tpeHK) + ".breakOut"
                patches += Patch(tree.pos, companionStr)
              }
            }
            super.transform(tree)
          case _ =>
            super.transform(tree)
        }
        tree
      }
  }
  private class VarargsToSeq(unit: CompilationUnit, patches: mutable.LinkedHashSet[Patch])
      extends RewriteTypingTransformer(unit) {
    val CollectionImmutableSeqClass = rootMirror.requiredClass[scala.collection.immutable.Seq[Any]]
    val CollectionSeqClass = rootMirror.requiredClass[scala.collection.Seq[Any]]
    val GenTraversableOnce_toSeq =
      definitions.getMember(rootMirror.requiredClass[GenTraversableOnce[Any]], TermName("toSeq"))
    override def transform(tree: Tree): Tree = tree match {
      case Typed(expr, Ident(tpnme.WILDCARD_STAR)) =>
        val sym = expr.tpe.typeSymbol
        if (expr.pos.isOpaqueRange && !sym.isNonBottomSubClass(CollectionImmutableSeqClass) && sym.isNonBottomSubClass(
              CollectionSeqClass)) {
          val sym = expr.symbol
          val isToSeq = sym != null && sym.overriddenSymbol(GenTraversableOnce_toSeq.owner) == GenTraversableOnce_toSeq
          val isVarargsIdent = expr match {
            case Ident(_) if expr.symbol.isParameter && definitions.isRepeated(expr.symbol) => true
            case _                                                                          => false
          }
          if (!isToSeq && !isVarargsIdent) {
            patches += Patch(expr.pos.focusEnd, ".toSeq")
          }
        }
        super.transform(tree)
      case _ => super.transform(tree)
    }
  }

  private def isInferredArg(tree: Tree) = tree match {
    case tt: TypeTree => tt.original eq null
    case _ =>
      val pos = tree.pos
      pos.isOffset && tree.forAll { t =>
        val tpos = t.pos
        tpos == NoPosition || tpos.isOffset && tpos.point == pos.point
      }
  }

  private object Application {
    def unapply(t: Tree): Option[(Tree, List[Tree], List[List[Tree]])] = t match {
      case _: Apply | _: TypeApply =>
        val applied = treeInfo.dissectApplied(t)
        Some((applied.core, applied.targs, applied.argss))
      case _ =>
        None
    }
  }
  class ParseTree private (val tree: Tree, val index: collection.Map[Position, Tree]) {}
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
              index(x.pos.focusStart) = x // Needed for @storable @entity class C(val x: Seq[Int]), maybe fixable in the entity plugin instead?
            case _ =>
          }
        }
      }
      new ParseTree(unit.body, index)
    }
  }

  private class RewriteTypingTransformer(unit: CompilationUnit) extends TypingTransformer(unit) {
    override def transform(t: Tree): Tree = t match {
      case t if t.hasAttachment[analyzer.MacroExpansionAttachment] =>
        val expandee = t.attachments.get[analyzer.MacroExpansionAttachment].get.expandee
        super.transform(expandee)
        t
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
        localTyper.reenterTypeParams(dd.tparams)
        localTyper.reenterValueParams(dd.vparamss)
        try super.transform(t)
        finally {
          val scope = localTyper.context.scope
          dd.tparams.foreach(t => scope.unlink(t.symbol))
          mforeach(dd.vparamss)(t => scope.unlink(t.symbol))
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
    def chooseQualiferExpr(pos: Position, options: List[String])(f: Tree => Boolean): String = {
      val errors = mutable.Buffer[Throwable]()
      options
        .find { qual =>
          val ref = newUnitParser(newCompilationUnit(qual)).parseRule(_.expr())

          silentTyped(ref, Mode.QUALmode) match {
            case util.Failure(t) => errors += t; false
            case util.Success(t) => f(t)
          }
        }
        .getOrElse {
          reporter.error(pos, "Unable to resolve any of the following qualifiers: " + options + ". Errors: " + errors)
          options.last
        }
    }
    def patchSelect(select: Select, app: Apply, code: String): List[Patch] = {
      val patches = mutable.ListBuffer[Patch]()
      val qualEnd = select.qualifier.pos.end
      val c = unit.source.content(unit.source.skipWhitespace(qualEnd))
      val dotted = (c == '.')
      if (!dotted) {
        patches += Patch(app.pos.focusStart, "(")
        // Scalac's parser doesn't span the Apply tree to include the } in `m mapValues { x => y }`,
        // it only makes it up the `y`. Compensate by skipping whitespace. This hack wouldn't work for comments,
        // see @Ignored test `mapValuesInfixComment`.
        val treeEnd = app.pos.end
        val treeEnd1 = unit.source.skipWhitespace(treeEnd)
        patches += Patch(unit.position(treeEnd1 + 1), ")." + code)
      } else {
        patches += Patch(app.pos.focusEnd, "." + code)
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
        if (typedTree.symbol == sym || sym.tpeHK =:= typedTree.tpe) {
          f(NoPrefix, sym)
        } else {
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
}
