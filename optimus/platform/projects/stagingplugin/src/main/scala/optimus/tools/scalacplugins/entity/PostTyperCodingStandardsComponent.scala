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

import optimus.tools.scalacplugins.entity.reporter._

import scala.collection.mutable
import scala.reflect.internal.Flags.ABSTRACT
import scala.tools.nsc.Global
import scala.tools.nsc.Phase
import scala.tools.nsc.plugins.PluginComponent

class PostTyperCodingStandardsComponent(
    val pluginData: PluginData,
    val global: Global,
    val phaseInfo: OptimusPhaseInfo
) extends PluginComponent
    with OptimusPluginReporter
    with WithOptimusPhase
    with StagingPluginDefinitions {
  import global._
  import definitions._
  import PostTyperCodingStandardsComponent._
  lazy val appClass = rootMirror.getRequiredClass("scala.App")
  lazy val optimusAppClass = rootMirror.getClassIfDefined("optimus.platform.OptimusApp")
  lazy val jdkCollectionConverters = rootMirror.getModuleIfDefined("scala.jdk.CollectionConverters")

  val lazyListFactoryFunctions: Set[Name] =
    Set("cons", "empty", "iterate", "from", "continually", "fill", "tabulate", "unfold", "concat", "range").map(
      TermName(_))

  override def newPhase(prev: Phase): Phase = new StdPhase(prev) {
    override def apply(unit: CompilationUnit): Unit = {
      if (!pluginData.rewriteConfig.anyEnabled)
        new StandardsTraverser(unit).traverse(unit.body)
    }
  }

  private class StandardsTraverser(unit: CompilationUnit) extends Traverser {
    lazy val is212OnlySource = unit.source.path.split("[/\\\\]").contains("scala-2.12")
    lazy val sourceString = new String(unit.source.content)

    var inClass = false
    var inMacroExpansion: Boolean = false
    var enclosingDefs: List[DefTree] = Nil
    val enclosingTrees = mutable.ArrayStack[Tree](EmptyTree)

    private lazy val DiscouragedAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.discouraged")
    private def alarmOnCaseClass(mods: Modifiers): Boolean =
      mods.isCase && !(mods.isSealed && mods.hasAbstractFlag) && !mods.isFinal

    private def checkSelection(tree: Select): Unit = {
      val sym = tree.symbol

      if (sym != null && tree.symbol.hasAnnotation(DiscouragedAnnotation)) {
        val annInfo = sym.getAnnotation(DiscouragedAnnotation).get
        alarm(
          CodeStyleNonErrorMessages.DISCOURAGED_CONSTRUCT,
          tree.pos,
          annInfo.stringArg(0).get,
          annInfo.stringArg(1).get)
      }

      if (isScala2_12) {
        val qual = tree.qualifier
        val pos = tree.pos
        if (qual.tpe != null && pos.isDefined && pos.start != pos.end && pos == tree.qualifier.pos) {
          if (IntegralToFloating(sym)) {
            if (IsIntegralDivision(qual))
              alarm(Scala213MigrationMessages.INTEGRAL_DIVISION_TO_FLOATING, tree.pos, sym.name)
            val qualSym = qual.tpe.typeSymbol
            if (
              qualSym == IntClass && sym == IntToFloat ||
              qualSym == LongClass && (sym == LongToFloat || sym == LongToDouble)
            )
              alarm(
                Scala213MigrationMessages.INT_TO_FLOAT,
                tree.pos,
                qualSym.name,
                sym.tpe.finalResultType.typeSymbol.name,
                sym.name)
          }
        }

        if (sym.name == Parallelizable_par.name && !is212OnlySource && sym.overrideChain.contains(Parallelizable_par)) {
          if (!sourceString.contains("ParCollectionConverters._"))
            alarm(Scala213MigrationMessages.IMPORT_PARCOLLECTIONS, tree.pos)
        }
      }
    }

    object IsIntegralDivision extends Traverser {
      lazy val ScalaIntegralValueClasses: Set[Symbol] = Set(CharClass, ByteClass, ShortClass, IntClass, LongClass)

      private var res = false
      def apply(t: Tree): Boolean = {
        res = false
        traverse(t)
        res
      }

      private def isInt(t: Tree) = ScalaIntegralValueClasses(t.tpe.typeSymbol)
      override def traverse(tree: Tree): Unit = tree match {
        case Apply(Select(q, nme.DIV), _) if isInt(q) =>
          res = true
        case Apply(Select(a1, _), List(a2)) if isInt(tree) && isInt(a1) && isInt(a2) =>
          traverse(a1)
          traverse(a2)
        case Select(q, _) if isInt(tree) && isInt(q) =>
          traverse(q)
        case _ =>
      }
    }

    private def isInferred(tpt: Tree) = tpt match {
      case tt: TypeTree => tt.original == null
      case _            => false
    }

    override def traverse(tree: Tree): Unit = {
      val inMacroExpansionSaved = inMacroExpansion
      if (tree.hasAttachment[analyzer.MacroExpansionAttachment]) {
        inMacroExpansion = true
      }

      val enclosingDefsSaved = tree match {
        case d: DefTree => enclosingDefs ::= d; enclosingDefs.tail
        case _          => enclosingDefs
      }
      enclosingTrees.push(tree)
      try
        tree match {
          // Try to avoid complaining about constructs that scala introduced in for-desugaring
          case Apply(Select(qual, _), Function(vd :: Nil, _) :: Nil)
              if vd.name.startsWith(nme.CHECK_IF_REFUTABLE_STRING) =>
            // i.e. we're not traversing the full Select, so we won't detect if the symbol is objectionable.
            traverse(qual)
            traverse(vd)
          case sel: Select =>
            checkSelection(sel)
            super.traverse(tree)
          case cd: ClassDef
              if !cd.mods.hasFlag(ABSTRACT) && (cd.symbol.baseClasses.contains(appClass) || cd.symbol.baseClasses
                .contains(optimusAppClass)) =>
            alarm(CodeStyleNonErrorMessages.CLASS_EXTENDS_APP, tree.pos, cd.symbol)
          case ClassDef(mods, _, _, _) if alarmOnCaseClass(mods) =>
            if (!inClass)
              alarm(CodeStyleNonErrorMessages.NON_FINAL_CASE_CLASS, tree.pos)
            else
              alarm(CodeStyleNonErrorMessages.NON_FINAL_INNER_CASE_CLASS, tree.pos)
            super.traverse(tree)
          case _: ClassDef =>
            val oldInClass = inClass
            inClass = true
            super.traverse(tree)
            inClass = oldInClass
          case vd: ValOrDefDef
              if vd.symbol != null && vd.symbol.isImplicit && isInferred(vd.tpt) && !isLocal(vd.symbol) =>
            alarm(StagingNonErrorMessages.UNTYPED_IMPLICIT(vd.symbol.tpe.finalResultType), tree.pos)
            super.traverse(tree)
          case Application(fun, targs, argss) =>
            checkApplication(tree, fun, targs, argss)
            traverse(fun)
            traverseTrees(targs)
            traverseTreess(argss)
          case dd: DefDef =>
            checkDefDef(dd)
            super.traverse(tree)
          case imp: Import if imp.expr.symbol == jdkCollectionConverters =>
            imp.selectors.filter(_.name != nme.WILDCARD).foreach { sel =>
              alarm(
                Scala213MigrationMessages.COLLECTION_CONVERTER_IMPORT_MUST_BE_WILDCARD,
                imp.pos.withPoint(sel.namePos))
              ()
            }
            super.traverse(tree)
          case Match(sel, cases) =>
            cases.foreach(c => checkCase(sel, c))
            super.traverse(tree)
          case _ => super.traverse(tree)
        }
      finally {
        inMacroExpansion = inMacroExpansionSaved
        enclosingDefs = enclosingDefsSaved
        enclosingTrees.pop()
      }
    }

    def checkDefDef(dd: DefDef): Unit = {
      if (isScala2_12) {
        val sym = dd.symbol

        if (
          sym.paramss.lastOption
            .getOrElse(Nil)
            .exists(p =>
              p.isImplicit && p.isSynthetic && p.name.startsWith("evidence") && p.tpe.typeSymbol ==
                definitions.FunctionClass(1))
        ) {
          alarm(Scala213MigrationMessages.VIEW_BOUND, dd.pos)
        }

        // Optimus macros collapse range positions to offset, so skip this check as it depends on range positions.
        val isProcedureUnit = !inMacroExpansion && settings.Yrangepos.value && (dd.tpt match {
          case tt: TypeTree =>
            !sym.isSynthetic && // e.g., default getters
            !sym.isParamAccessor && // @entity param accessors are not synthetic
            !sym.isAccessor && // field of type Unit
            !isValAccessor(sym) && { // generated for @stored @entity class fields
              // Abstract `def f` and `def f()` have tpt.pos == dd.pos (range positions).
              // All others have offset position: `def f { }`, `def f() { }`, `def f(x: Int)`, `def f(x: Int) { }`
              (tt.tpe.typeSymbol == UnitClass) && tt.original != null && (tt.pos.isOffset || tt.pos == dd.pos) ||
              // def this() { this(1) }
              sym.isAuxiliaryConstructor && {
                var proc = true
                var i = dd.rhs.pos.start
                var cs = unit.source.content
                while (cs(i) != ')') {
                  if (cs(i) == '=') proc = false
                  i -= 1
                }
                proc
              }
            }
          case _ => false
        })
        if (isProcedureUnit)
          alarm(Scala213MigrationMessages.PROCEDURE_SYNTAX, dd.pos)

        val nilaryOverrideMismatch = {
          val skip = sym.typeParams.nonEmpty || sym.isSynthetic || sym.isAccessor || sym.isConstructor ||
            sym.name.containsChar('$') || sym.owner.name.toString.contains("stateMachine$async") ||
            !sym.isOverridingSymbol || sym.overrides.exists(sym =>
              sym.isJavaDefined || definitions.isUniversalMember(sym)) || {
              sym.name.toString match {
                case BeanPropertyName(rest) =>
                  sym.owner.tpe
                    .member(TermName(rest.updated(0, rest(0).toLower)).localName)
                    .annotations
                    .exists(_.tpe.typeSymbol.name.toString.contains("BeanProperty"))
                case _ => false
              }
            }

          if (skip) 0
          else {
            val base = sym.allOverriddenSymbols.last
            // checking `paramss` instead of `sym.paramss`: in namer, a nullary method overriding a nilary one
            // obtains an empty parameter list. so even if paramss == List(Nil), the method can be nullary.
            if (dd.vparamss == List(Nil) && base.paramss == Nil) 1
            else if (dd.vparamss == Nil && base.paramss == List(Nil)) 2
            else 0
          }
        }
        if (nilaryOverrideMismatch > 0) {
          val baseMsg = if (nilaryOverrideMismatch == 1) "without a parameter list" else "with an empty parameter list"
          alarm(Scala213MigrationMessages.NILARY_OVERRIDE, dd.pos, sym.allOverriddenSymbols.last.fullName, baseMsg)
        }
      }
    }

    def checkApplication(tree: Tree, fun: Tree, targs: List[Tree], argss: List[List[Tree]]): Unit = {
      val sym = fun.symbol

      if ((sym == Predef_augmentString || sym == Predef_wrapString) && !argss.exists(_.exists(_.pos.isOffset))) {
        enclosingTrees(1) match {
          case _: Select  => // okay, e.g. "".exists(f)
          case _: Apply   => // okay, e.g. def foo(s: Seq[Char]); foo("")
          case _: Typed   => // okay, "": Seq[Char]
          case _: DefTree => // okay, typically a `val x$1 = augmentString(s)` in an application with default args
          case _          =>
            // dangerous:
            // xs.flatMap(x => if (cond) "s" else List("")  // should be List("s")
            // (sb: StringBuffer) => sb ++ "foo" (should be ++=)
            //
            // benign:
            // "abc" ++ "def" // just use +
            // List("a", "b", "").flatMap(x => a + ",") // just use .map(...).mkString
            alarm(StagingErrors.AUGMENT_STRING, fun.pos)
        }
      }

      if (isAtLeastScala2_13) {
        if (sym.name == IterableOnceOps_to.name && sym.overrideChain.contains(IterableOnceOps_to)) {
          val sym = tree.tpe.typeSymbol
          if (sym.isNonBottomSubClass(StreamClass) || sym.isNonBottomSubClass(LazyListClass))
            alarm(CodeStyleNonErrorMessages.DISCOURAGED_CONSTRUCT, fun.pos, sym.name, AnnotatingComponent.lazyReason)
        }

        if (sym == GrowablePlusEquals) fun match {
          case Select(qual, _) if qual.tpe.typeSymbol == OptimusDoubleBuilderClass =>
            alarm(Scala213MigrationMessages.DOUBLE_BUILDER_PLUSEQ, fun.pos)
          case _ =>
        }

        if (lazyListFactoryFunctions(sym.name)) fun match {
          case Select(qual, _) =>
            val qs = qual.tpe.typeSymbol
            if (qs == StreamModuleClass || qs == LazyListModuleClass)
              alarm(CodeStyleNonErrorMessages.DISCOURAGED_CONSTRUCT, fun.pos, sym.name, AnnotatingComponent.lazyReason)
          case _ =>
        }
      }

      if (isScala2_12) {
        if (
          fun.tpe.paramss.lengthCompare(1) > 0 && sym.owner.isNonBottomSubClass(
            GenTraversableOnceClass) && !is212OnlySource
        )
          fun.tpe.paramss.last match {
            case List(cbf) if cbf.isImplicit && cbf.tpe.typeSymbol.isNonBottomSubClass(CanBuildFromClass) =>
              if (argss.length == fun.tpe.paramss.length) argss.last match {
                case List(arg) if arg.pos.start != arg.pos.end =>
                  alarm(Scala213MigrationMessages.EXPLICIT_CBF_ARGUMENT, fun.pos)
                case List(arg @ TypeApply(_, List(targ))) if arg.pos.start == arg.pos.end /* synthetic*/ =>
                  val mapToNonMapOp = fun match {
                    case Select(qual, _) if qual.tpe.typeSymbol.isNonBottomSubClass(CollectionMapClass) =>
                      !tree.tpe.typeSymbol.isNonBottomSubClass(CollectionMapClass)
                    case _ => false
                  }
                  if (mapToNonMapOp) targ.tpe match {
                    case ExistentialType(_ :: _, tp) if tp.typeSymbol == definitions.TupleClass(2) =>
                      alarm(Scala213MigrationMessages.MAP_CBF_EXISTENTIAL, fun.pos)
                    case _ =>
                  }
                case _ =>
              }
            case _ =>
          }

        if (sym.name == GenTraversableOnce_to.name && sym.overrideChain.contains(GenTraversableOnce_to))
          targs match {
            case List(tt: TypeTree) if tt.original != null =>
              alarm(Scala213MigrationMessages.TO_CONVERSION_TYPE_ARG, fun.pos)
            case _ =>
          }

        if (argss == List(Nil) && tree.hasAttachment[InfixAttachment.type])
          alarm(Scala213MigrationMessages.NILARY_INFIX, fun.pos)

        val isAutoApplication = tree.hasAttachment[AutoApplicationAttachment.type] ||
          sym.paramss.lastOption.flatMap(_.headOption).exists(_.isImplicit) && (tree match {
            case Apply(f, _) => f.hasAttachment[AutoApplicationAttachment.type]
            case _           => false
          })

        if (isAutoApplication) {
          enclosingDefs
            .collectFirst {
              case dd: DefDef   => dd.symbol
              case cd: ClassDef => cd.symbol
            }
            .foreach(enclSym => {
              if (!enclSym.isSynthetic && !enclSym.isAccessor) {
                def skip = sym.isConstructor ||
                  sym.overrideChain.exists(o => o.isJavaDefined || definitions.isUniversalMember(o))
                def okFor213 =
                  allowAutoApplicationNames(sym.name) && allowAutoApplication.exists(m => sym.overrideChain.contains(m))
                // assert(foo.size() == x) is transformed by scalatest to `$org_scalatest_tmp.size` without ()
                def scalaTestAssert = fun match {
                  case Select(Ident(n), _) => n.toString.startsWith("$org_scalatest")
                  case _                   => false
                }
                if (!skip && !okFor213 && !scalaTestAssert)
                  alarm(Scala213MigrationMessages.AUTO_APPLICATION, fun.pos, sym.name.toString)
              }
            })
        }

        if (!isAutoApplication && isNullaryIn213(sym))
          alarm(Scala213MigrationMessages.NULLARY_IN_213, fun.pos, sym.name.toString)

        // TypeApply(fun, targs).tpe is different than fun.tpe
        def funTpe(t: Tree): Option[Type] = t match {
          case Apply(f, _) => funTpe(f)
          case _           => Option(t.tpe)
        }

        for ((params, args) <- funTpe(tree).map(_.paramss.zip(argss)).getOrElse(Nil))
          foreachParamsAndArgs(params, args) { (param, arg) =>
            if (
              Predef_fallbackStringCBF != NoSymbol && arg.symbol == Predef_fallbackStringCBF && param.tpe.typeArgs.headOption
                .map(_.typeSymbol)
                .contains(NothingClass)
            )
              alarm(Scala213MigrationMessages.PREDEF_FALLBACK_STRING_CBF, fun.pos)
          }

        fun match {
          case Select(qual, _)
              if fun.symbol == TraversableLike_++ && qual.tpe.typeSymbol.isNonBottomSubClass(
                CollectionMapClass) && tree.tpe.typeSymbol.isNonBottomSubClass(CollectionMapClass) =>
            def argKeepExistential(tp: Type) = tp match {
              case ExistentialType(_, u) => u.typeArgs.head
              case _                     => tp.typeArgs.head
            }
            val receiverKey = argKeepExistential(qual.tpe.baseType(CollectionMapClass))
            val resultKey = argKeepExistential(tree.tpe.baseType(CollectionMapClass))
            if (!(receiverKey =:= resultKey)) {
              val argKey = targs.head.tpe.baseType(definitions.TupleClass(2)).typeArgs.headOption.getOrElse("?")
              alarm(Scala213MigrationMessages.MAP_CONCAT_WIDENS, fun.pos, receiverKey, argKey)
            }
          case _ =>
        }

        tree match {
          case NeedsUnsorted(qual) =>
            alarm(Scala213MigrationMessages.NEEDS_UNSORTED, qual.pos)
          case _ =>
        }
      }
    }
    def checkCase(selector: Tree, caseDef: CaseDef): Unit = {
      object traverser extends Traverser {
        var selType = selector.tpe
        override def traverse(tree: Tree): Unit = tree match {
          case Annotated(annot, tree) => traverse(tree)

          case Apply(fun, arg :: Nil)
              if fun.tpe.isInstanceOf[MethodType] && !fun.tpe.params.head.info.typeSymbol
                .isNonBottomSubClass(definitions.ListClass) && arg.tpe.typeSymbol == definitions.ConsClass =>
            alarm(CodeStyleNonErrorMessages.OVERLY_SPECIFIC_LIST_PATTERN, arg.pos)
            super.traverse(tree)

          case Apply(fun, (u @ UnApply(unapplyFun, _)) :: Nil)
              if fun.tpe.isInstanceOf[MethodType] && !fun.tpe.params.head.info.typeSymbol
                .isNonBottomSubClass(definitions.ListClass) && u.tpe.typeSymbol == definitions.ListClass =>
            alarm(CodeStyleNonErrorMessages.OVERLY_SPECIFIC_LIST_PATTERN, u.pos)
            super.traverse(tree)
          case _ =>
            super.traverse(tree)
        }
      }
      caseDef.pat match {
        case p @ Apply(fun, args) if fun.tpe.resultType.typeSymbol == definitions.ConsClass =>
          if (!selector.tpe.typeSymbol.isNonBottomSubClass(definitions.ListClass)) {
            alarm(CodeStyleNonErrorMessages.OVERLY_SPECIFIC_LIST_PATTERN, p.pos)
          }
        case u @ UnApply(unapplyFun, _)
            if !selector.tpe.typeSymbol.isNonBottomSubClass(
              definitions.ListClass) && u.tpe.typeSymbol == definitions.ListClass =>
          alarm(CodeStyleNonErrorMessages.OVERLY_SPECIFIC_LIST_PATTERN, u.pos)
        case _ =>
      }
      traverser.traverse(caseDef.pat)
    }
  }

  private def isLocal(sym: Symbol) =
    sym.isLocalToBlock ||
      sym.ownerChain.tail.exists(o => o.isMethod || o.isAnonymousFunction || o.isAnonymousClass || o.isLocalToBlock)
}

object PostTyperCodingStandardsComponent {
  val BeanPropertyName = "(?:get|set|is)(.+)".r
}
