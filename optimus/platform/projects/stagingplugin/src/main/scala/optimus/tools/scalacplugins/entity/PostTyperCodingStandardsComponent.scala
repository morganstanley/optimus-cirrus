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

import optimus.scalacompat.isScala2_12
import optimus.tools.scalacplugins.entity.reporter._

import scala.collection.mutable
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

  override def newPhase(prev: Phase): Phase = new StdPhase(prev) {
    override def apply(unit: CompilationUnit) {
      if (!pluginData.rewriteConfig.anyEnabled)
        standardsTraverser.traverse(unit.body)
    }
  }

  private object standardsTraverser extends Traverser {
    var inClass = false
    var inMacroExpansion: Boolean = false
    var enclosingDefs: List[DefTree] = Nil
    val enclosingTrees = mutable.ArrayStack[Tree](EmptyTree)

    private lazy val DiscouragedAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.discouraged")
    private def alarmOnCaseClass(mods: Modifiers): Boolean =
      mods.isCase && !(mods.isSealed && mods.hasAbstractFlag) && !mods.isFinal

    private def checkDiscouraged(tree: Select): Unit = {
      val sym = tree.symbol
      if (sym != null && tree.symbol.hasAnnotation(DiscouragedAnnotation)) {
        val annInfo = sym.getAnnotation(DiscouragedAnnotation).get
        alarm(
          CodeStyleNonErrorMessages.DISCOURAGED_CONSTRUCT,
          tree.pos,
          annInfo.stringArg(0).get,
          annInfo.stringArg(1).get)
      }
    }

    private def isInferred(tpt: Tree) = tpt match {
      case tt: TypeTree => tt.original == null
      case _            => false
    }

    private def notEntirelyPrivate(mods: Modifiers): Boolean =
      mods.isPublic || mods.isProtected || mods.hasAccessBoundary

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
            checkDiscouraged(sel)
            super.traverse(tree)
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
              if vd.symbol != null && vd.symbol.isImplicit &&
                notEntirelyPrivate(vd.mods) && isInferred(vd.tpt) && !isLocal(vd.symbol) =>
            alarm(StagingNonErrorMessages.UNTYPED_IMPLICIT(vd.name), tree.pos)
            super.traverse(tree)
          case Application(fun, targs, argss) =>
            checkApplication(tree, fun, targs, argss)
            traverse(fun)
            traverseTrees(targs)
            traverseTreess(argss)
          case dd: DefDef =>
            checkDefDef(dd)
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
        // Abstract `def f` and `def f()` have tpt.pos == dd.pos (range positions).
        // All others have offset position: `def f { }`, `def f() { }`, `def f(x: Int)`, `def f(x: Int) { }`
        val isProcedureUnit = !inMacroExpansion && settings.Yrangepos.value && (dd.tpt match {
          case tt: TypeTree =>
            !sym.isSynthetic && // e.g., default getters
            !sym.isParamAccessor && // @entity param accessors are not synthetic
            !sym.isAccessor && // field of type Unit
            !isValAccessor(sym) && // generated for @stored @entity class fields
            tt.tpe.typeSymbol == UnitClass &&
            tt.original != null &&
            (tt.pos.isOffset || tt.pos == dd.pos)
          case _ => false
        })
        if (isProcedureUnit)
          alarm(Scala213MigrationMessages.PROCEDURE_SYNTAX, dd.pos)

        val nilaryOverrideMismatch = {
          val skip = sym.typeParams.nonEmpty || sym.isSynthetic || sym.isAccessor || sym.isConstructor ||
            sym.name.containsChar('$') || sym.owner.name.containsChar('$') || // synthetics, like stateMachine$async
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

      if (sym.name.toString == "mock" && targs.nonEmpty) {
        val clazz = rootMirror.getClassIfDefined(targs.head.tpe.toLongString)
        if (clazz.isCaseClass && clazz.isFinal)
          alarm(CodeStyleNonErrorMessages.MOCK_FINAL_CASE_CLASS, tree.pos)
      }

      if ((sym == Predef_augmentString || sym == Predef_wrapString) && !argss.exists(_.exists(_.pos.isOffset))) {
        enclosingTrees(1) match {
          case _: Select => // okay, e.g. "".exists(f)
          case _: Apply => // okay, e.g. def foo(s: Seq[Char]); foo("")
          case _: Typed => // okay, "": Seq[Char]
          case _ =>
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

      if (isScala2_12) {
        if (sym.name == GenTraversableOnce_to.name && sym.overrideChain.contains(GenTraversableOnce_to))
          targs match {
            case List(tt: TypeTree) if tt.original != null =>
              alarm(Scala213MigrationMessages.TO_CONVERSION_TYPE_ARG, fun.pos)
            case _ =>
          }

        if (argss == List(Nil) && tree.hasAttachment[InfixAttachment.type])
          alarm(Scala213MigrationMessages.NILARY_INFIX, fun.pos)

        if (argss == List(Nil) && tree.hasAttachment[AutoApplicationAttachment.type]) {
          enclosingDefs
            .collectFirst { case dd: DefDef => dd }
            .foreach(dd => {
              val enclMeth = dd.symbol
              if (!enclMeth.isSynthetic && !enclMeth.isAccessor) {
                def skip = sym.isConstructor || sym.paramss != List(Nil) ||
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

        val isNullaryIn213 = nullaryIn213Names(sym.name) && nullaryIn213.exists(m => sym.overrideChain.contains(m))
        if (isNullaryIn213 && !tree.hasAttachment[AutoApplicationAttachment.type])
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
          case Select(qual, _) if fun.symbol == TraversableLike_++ && qual.tpe.typeSymbol.isNonBottomSubClass(CollectionMapClass) =>
            val b1 = targs.head.tpe
            b1.baseType(definitions.TupleClass(2)) match {
              case NoType =>
              case tp =>
                val k1 = tp.typeArgs.head
                val k = qual.tpe.baseType(CollectionMapClass).typeArgs.head.upperBound
                alarm(Scala213MigrationMessages.MAP_CONCAT_WIDENS, fun.pos, k, k1)
            }
          case _ =>
        }
      }
    }
  }

  private def isLocal(sym: Symbol) =
    sym.isLocalToBlock ||
      sym.ownerChain.tail.exists(o => o.isMethod || o.isAnonymousFunction || o.isAnonymousClass || o.isLocalToBlock)
}

object PostTyperCodingStandardsComponent {
  val BeanPropertyName = "(?:get|set|is)(.+)".r
}
