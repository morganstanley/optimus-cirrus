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
import optimus.tools.scalacplugins.entity.reporter.OptimusPluginReporter

import scala.collection.mutable
import scala.reflect.internal.util.SourceFile
import scala.tools.nsc.Global
import scala.tools.nsc.Phase
import scala.tools.nsc.Reporting.MessageFilter.MessagePattern
import scala.tools.nsc.Reporting.Suppression
import scala.tools.nsc.plugins.PluginComponent
import scala.util.matching.Regex

// `GeneralAPICheckComponent` runs in both the staging and the entity plugin, so its checks are also applied in
// modules that don't use the entity plugin.
// The subclass `OptimusAPICheckComponent` performs optimus specific checks and is only added to the entity plugin.
class GeneralAPICheckComponent(
    val pluginData: PluginData,
    val global: Global,
    val phaseInfo: OptimusPhaseInfo
) extends PluginComponent
    with OptimusPluginReporter
    with WithOptimusPhase {
  import global._

  override def newPhase(prev: Phase): Phase = new StdPhase(prev) {
    override def apply(unit: CompilationUnit): Unit = {
      val nw = new CollectNoWarnsWithFilters(unit, Set(DeprecatingId, DeprecatingLightId))
      (new GeneralAPICheck(nw)).traverse(unit.body)
      nw.complainAboutUnused()
    }
  }

  private lazy val NowarnAnnotation = rootMirror.getRequiredClass("scala.annotation.nowarn")
  private val NowarnFilterOptimus = """^msg=(\d{5})(?:\s+(.*))?$""".r
  private val NowarnFilterOptimusMsg = """^(\d{5})\s+(.*)$""".r
  private val DeprecatingId = OptimusNonErrorMessages.DEPRECATING.id.sn
  private val DeprecatingLightId = OptimusNonErrorMessages.DEPRECATING_LIGHT.id.sn

  object CollectNoWarnsWithFilters {
    private val ValueName = newTermName("value")
    case class OptimusNowarnWithFilters(id: Int, start: Int, end: Int, nowarnPos: Position, filter: Regex) {
      def matchesPos(pos: Position): Boolean =
        pos.isDefined && start <= pos.start && pos.end <= end
    }
  }

  // used to collect and handle `@nowarn("msg=10500 fqn.Pattern")` which silences the warning
  // only if the deprecated symbol's name matches the pattern
  // `nowarnsFor` selects the handled message ids (e.g. 10500, 10501)
  class CollectNoWarnsWithFilters(unit: CompilationUnit, noWarnsFor: Set[Int]) {
    import CollectNoWarnsWithFilters._
    import global._

    private val compilerSuppressions = OptimusPluginReporter.suppressions(global)

    private val nowarns = mutable.ArrayBuffer.empty[OptimusNowarnWithFilters]
    private val unusedNoWarn = mutable.HashMap.empty[Regex, (Position, String)]

    collect()

    private def collect(): Unit = {
      val sups = compilerSuppressions.getOrElse(unit.source, Nil).toList
      for (sup <- sups; filter <- sup.filters) filter match {
        case MessagePattern(r) =>
          r.regex match {
            case NowarnFilterOptimusMsg(id, nameFilters) if noWarnsFor(id.toInt) =>
              val filterRegex =
                nameFilters
                  .split("\\s+")
                  // allow an arbitrary number of $something at the end
                  .map(s => "^" + s + "(\\$.*)*$")
                  // group together in an OR list
                  .mkString("(", "|", ")")
                  .r
              unusedNoWarn += filterRegex -> (sup.annotPos, nameFilters)
              nowarns += OptimusNowarnWithFilters(id.toInt, sup.start, sup.end, sup.annotPos, filterRegex)

            case _ =>
          }

        case _ =>
      }
    }

    // (matchingPositionAndRegex, matchingPositionOnly)
    def matchingSuppressions(
        callerPos: Position,
        calleeSym: Symbol): (List[OptimusNowarnWithFilters], List[OptimusNowarnWithFilters]) = {
      val matchingPos = nowarns.filter(_.matchesPos(callerPos)).toList
      val res = matchingPos.partition(_.filter.findFirstIn(calleeSym.fullName).isDefined)
      res._1.foreach(sup => unusedNoWarn -= sup.filter)
      res
    }

    def matches(callerPos: Position, calleeSym: Symbol) = matchingSuppressions(callerPos, calleeSym)._1.nonEmpty

    def complainAboutUnused(): Unit = unusedNoWarn.values.foreach { case (pos, msg) =>
      alarm(OptimusNonErrorMessages.UNUSED_NOWARN, pos, msg)
    }
  }

  // Extractor for Apply and TypeApply. Applied.unapply matches any tree, not just applications
  object Application {
    def unapply(t: GenericApply): Some[(Tree, List[Tree], List[List[Tree]])] = {
      val applied = treeInfo.dissectApplied(t)
      Some((applied.core, applied.targs, applied.argss))
    }
  }

  // Traversal to perform checks on symbols (abstract `checkUndesiredProperties` method), similar to the compiler's
  // RefChecks.
  abstract class AbstractAPICheck extends Traverser {
    import global._

    var inPattern: Boolean = false
    @inline final def savingInPattern[A](value: Boolean)(body: => A): A = {
      val saved = inPattern
      try {
        inPattern = value
        body
      } finally inPattern = saved
    }

    def preTraverse(tree: Tree): Boolean // return true if should continue
    def checkUndesiredProperties(sym: Symbol, pos: Position): Unit
    def checkAnnotationProperties(ann: AnnotationInfo, annotated: Option[Symbol]): Unit = ()
    def checkCompanionApply(classSym: Symbol): Boolean = false

    final override def traverse(tree: Tree): Unit = visit(tree)

    private def visit(tree: Tree): Unit = {
      val sym = tree.symbol

      applyRefchecksToAnnotations(tree)

      if (preTraverse(tree)) tree match {

        case CaseDef(pat, guard, body) =>
          savingInPattern(value = true) {
            traverse(pat)
          }
          traverse(guard)
          traverse(body)

        case LabelDef(_, _, rhs) if treeInfo.hasSynthCaseSymbol(tree) =>
          savingInPattern(value = true) {
            traverse(rhs)
          }

        case Apply(fun, args) if fun.symbol.isLabel && treeInfo.isSynthCaseSymbol(fun.symbol) =>
          savingInPattern(value = false) {
            // https://github.com/scala/bug/issues/7756 If we were in a translated pattern, we can now switch out of
            // pattern mode, as the label apply signals that we are in the user-supplied code in the case body.
            //
            //         Relies on the translation of:
            //            (null: Any) match { case x: List[_] => x; x.reverse; case _ => }'
            //         to:
            //            <synthetic> val x2: List[_] = (x1.asInstanceOf[List[_]]: List[_]);
            //                  matchEnd4({ x2; x2.reverse}) // case body is an argument to a label apply.
            super.traverse(tree)
          }
        case vd @ ValDef(_, _, _, rhs) if treeInfo.hasSynthCaseSymbol(tree) =>
          // https://github.com/scala/bug/issues/7716 Don't refcheck the tpt of the synthetic val that holds the selector.
          traverse(rhs)

        case TypeTree() =>
          val existentialParams = new mutable.ListBuffer[Symbol]
          object check extends TypeTraverser {
            def traverse(tp: Type): Unit = tp match {
              case _: MethodType if inPattern =>
              // `case Some(x)` becomes `TypeTree((v: T): Some[T])(x @ _)`
              // don't traverse the method type
              case tp @ ExistentialType(tparams, tpe) =>
                existentialParams ++= tparams
                mapOver(tp)
              case tp: TypeRef =>
                val tpWithWildcards = deriveTypeWithWildcards(existentialParams.toList)(tp)
                checkTypeRef(tpWithWildcards, tree)
                mapOver(tp)
              case _ =>
                mapOver(tp)
            }
          }
          check(tree.tpe)

        case Ident(name) =>
          checkUndesiredProperties(sym, tree.pos)

        case Application(fun @ Select(mod, nme.apply), targs, argss)
            if mod.symbol != null && mod.symbol.isModule && fun.symbol != null && fun.symbol.isSynthetic &&
              checkCompanionApply(mod.symbol.companionClass) =>
          checkUndesiredProperties(mod.symbol.companionClass, tree.pos)
          traverse(mod)
          traverseTrees(targs)
          traverseTreess(argss)

        case x @ Select(qual, _) =>
          checkSelect(x)
          qual.pos match {
            case NoPosition => checkUndesiredProperties(qual.symbol, x.pos)
            case _          => super.traverse(x)
          }

        case _: Import =>
        // let it slide here, since we'll generate an error when it's used

        case _ =>
          super.traverse(tree)
      }
    }

    private def checkSelect(tree: Select): Unit = {
      val sym = tree.symbol

      checkUndesiredProperties(sym, tree.pos)
    }

    private def checkTypeRef(tp: Type, tree: Tree): Unit = tp match {
      case TypeRef(pre, sym, args) =>
        tree match {
          // https://github.com/scala/bug/issues/7783 don't warn about inferred types in first case
          case tt: TypeTree if tt.original == null =>
          case _                                   => checkUndesiredProperties(sym, tree.pos)
        }
      case _ =>
    }

    private def checkAnnotations(tpes: List[Type], tree: Tree): Unit = tpes foreach { tp =>
      checkTypeRef(tp, tree)
    }

    private def applyRefchecksToAnnotations(tree: Tree): Unit = {
      def applyChecks(annots: List[AnnotationInfo], annotated: Option[Symbol] = None): Unit = {
        checkAnnotations(annots map (_.atp), tree)
        traverseTrees(annots flatMap (_.args))
        annots.foreach(checkAnnotationProperties(_, annotated))
      }

      tree match {
        case m: MemberDef =>
          val sym = m.symbol
          applyChecks(sym.annotations, annotated = Some(sym))

        case tpt @ TypeTree() =>
          if (tpt.original != null) {
            tpt.original foreach {
              case dc: TypeTreeWithDeferredRefCheck =>
                // https://github.com/scala/bug/issues/2416
                applyRefchecksToAnnotations(dc.check())
              case _ =>
            }
          }

          // similar to what the compiler does in RefChecks.applyRefchecksToAnnotations
          if (!inPattern) tree.tpe.foreach {
            case tp: AnnotatedType =>
              applyChecks(tp.annotations)
            case _ =>
          }

        case _ =>

      }
    }

  }

  // API checks to run in the staging plugin (GeneralAPICheckComponent)
  class GeneralAPICheck(noWarns: CollectNoWarnsWithFilters) extends AbstractAPICheck {
    lazy val DeprecatingAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.deprecating")
    lazy val DeprecatingNewAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.deprecatingNew")

    lazy val scalaDeprecated = rootMirror.requiredClass[scala.deprecated]
    lazy val javaDeprecated = rootMirror.requiredClass[java.lang.Deprecated]

    def isDeprecatingOrNew(sym: Symbol): Boolean =
      sym.annotations.exists(a => a.matches(DeprecatingAnnotation) || a.matches(DeprecatingNewAnnotation))

    private val ScopeExtractor = ".*\\(SCOPE=([\\w\\-\\.]+)\\).*".r

    val scopeId: Option[String] = settings.defines.value.collectFirst {
      case s if s.startsWith("-DscopeId=") => s.substring("-DscopeId=".length)
    }

    def preTraverse(tree: global.Tree): Boolean = true

    override def checkCompanionApply(classSym: Symbol): Boolean = isDeprecatingOrNew(classSym)

    def checkUndesiredProperties(calleeSym: Symbol, callerPos: Position): Unit = {
      // If symbol is deprecated, and the point of reference is not enclosed
      // in a deprecated member issue a warning.
      val deprecatingNew = calleeSym.getAnnotation(DeprecatingNewAnnotation)
      val either = deprecatingNew.orElse(calleeSym.getAnnotation(DeprecatingAnnotation))
      if (either.nonEmpty) {
        val annInfo = either.get
        annInfo.stringArg(0) match {
          case Some(msg) =>
            val typeAndFullName = s"${calleeSym.kindString} ${calleeSym.fullName}"
            val (matching, matchingPosOnly) = noWarns.matchingSuppressions(callerPos, calleeSym)
            if (
              currentOwner.isSynthetic ||
              currentOwner.ownerChain
                .exists(x => isDeprecatingOrNew(x) || isDeprecatingOrNew(x.companion))
            ) {
              alarm(
                OptimusNonErrorMessages.DEPRECATING_LIGHT,
                callerPos,
                typeAndFullName,
                msg + " (accessed within another deprecated definition)"
              )
            } else if (matching.nonEmpty) {
              // note that the @nowarn would be for DEPRECATING 10500 but we still honor it for DEPRECATING_NEW -
              // see comment on DEPRECATING_NEW for rationale
              alarm(OptimusNonErrorMessages.DEPRECATING_LIGHT, callerPos, typeAndFullName, msg)
              val contactInfo = msg match {
                case ScopeExtractor(id) => s"[$id]"
                case _                  => "appropriate"
              }
              matching.foreach { sup =>
                alarm(OptimusNonErrorMessages.NOWARN_DEPRECATION, sup.nowarnPos, typeAndFullName, contactInfo)
              }
            } else {
              val suffix =
                if (matchingPosOnly.isEmpty) " (no appropriate @nowarn found)"
                else " (@nowarn was found but didn't match or was malformed)"
              if (deprecatingNew.nonEmpty) {
                alarm(
                  OptimusNonErrorMessages.DEPRECATING_NEW,
                  callerPos,
                  typeAndFullName,
                  msg + suffix
                )
              } else {
                alarm(
                  OptimusNonErrorMessages.DEPRECATING,
                  callerPos,
                  typeAndFullName,
                  msg + suffix
                )
              }
            }
          case _ =>
            alarm(
              OptimusErrors.DEPRECATING_USAGE,
              callerPos,
              s"the annotation on $calleeSym does not have a constant `suggestion` argument."
            )
        }
      }
    }

    override def checkAnnotationProperties(ann: AnnotationInfo, annotated: Option[Symbol]): Unit = {
      def checkNowarn(ann: AnnotationInfo): Unit = {
        val arg = ann.stringArg(0) orElse ann.assocs.collectFirst { case (_, LiteralAnnotArg(Constant(value))) =>
          value.toString
        }
        def default =
          "nowarn requires a message filter (`msg=regex` or `cat=CATEGORY` for compiler warnings, `msg=12345` for optimus warnings)"
        val diag = arg match {
          case Some(NowarnFilterOptimus(id, fqns)) =>
            val idi = id.toInt
            val hasFqns = fqns != null && !fqns.isEmpty
            if (idi == DeprecatingId || idi == OptimusErrors.INCORRECT_ADVANCED_USAGE.id.sn) {
              if (!hasFqns)
                s"""@nowarn with msg=$idi requires a pattern matching the full name of the deprecating symbols, e.g., @nowarn("msg=$idi optimus.OldThing")"""
              else ""
            } else if (idi == DeprecatingLightId || idi == OptimusNonErrorMessages.DEPRECATING_NEW.id.sn) {
              s"""to silence warnings for usages of @deprecating or @deprecatingNew symbols, use @nowarn("msg=$DeprecatingId optimus.OldThing")"""
            } else if (hasFqns)
              s"""To silence optimus warnings, use @nowarn("msg=$idi") with only the ID in the `msg` filter"""
            else ""

          case Some(msg) =>
            if (msg.startsWith("cat=") || msg.startsWith("msg=")) ""
            else default

          case None =>
            default
        }
        if (!diag.isEmpty)
          alarm(OptimusErrors.NOWARN, ann.pos, diag)
      }

      def checkDeprecating(ann: AnnotationInfo, sym: Option[Symbol]): Unit = {
        sym match {
          case None =>
            alarm(
              OptimusErrors.DEPRECATING_USAGE,
              ann.pos,
              "Only definitions (classes, methods, etc) can be annotated `@deprecating`.")
          case Some(s) =>
            ann.stringArg(0) match {
              case None | Some("") =>
                alarm(
                  OptimusErrors.DEPRECATING_USAGE,
                  ann.pos,
                  "the `suggestion` needs to be a non-empty string constant.")
              case Some(arg) =>
                scopeId.foreach { sid =>
                  // add (SCOPE=scope.id) to the message to tell who maintains the deprecated definition
                  val newArg = Literal(Constant(s"$arg (SCOPE=$sid)"))
                  val newArgs = newArg :: ann.args.tail
                  s.removeAnnotation(ann.symbol)
                  s.addAnnotation(ann.symbol, newArgs)
                }
            }
        }
      }

      if (ann.matches(NowarnAnnotation)) checkNowarn(ann)
      else if (ann.matches(DeprecatingAnnotation) || ann.matches(DeprecatingNewAnnotation))
        checkDeprecating(ann, annotated)
      else if (ann.matches(scalaDeprecated) || ann.matches(javaDeprecated))
        alarm(OptimusNonErrorMessages.SCALA_JAVA_DEPRECATED, ann.pos, ann.symbol.name.toString)
    }
  }
}
