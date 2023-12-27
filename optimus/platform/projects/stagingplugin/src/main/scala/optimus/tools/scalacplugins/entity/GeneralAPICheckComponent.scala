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

import optimus.tools.scalacplugins.entity.reporter.OptimusAlarms

import java.util.regex.PatternSyntaxException
import optimus.tools.scalacplugins.entity.reporter.OptimusErrors
import optimus.tools.scalacplugins.entity.reporter.OptimusNonErrorMessages
import optimus.tools.scalacplugins.entity.reporter.OptimusPluginReporter

import scala.collection.mutable
import scala.tools.nsc.Global
import scala.tools.nsc.Phase
import scala.tools.nsc.plugins.PluginComponent
import scala.util.matching.Regex

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
      val nw = new CollectNoWarns(
        unit,
        Set(OptimusNonErrorMessages.DEPRECATING.id.sn, OptimusNonErrorMessages.DEPRECATING_LIGHT.id.sn))
      (new GeneralAPICheck(nw)).traverse(unit.body)
      nw.complainAboutUnused()
    }
  }

  lazy val NowarnAnnotation = rootMirror.getRequiredClass("scala.annotation.nowarn")

  object CollectNoWarns {
    private val ValueName = newTermName("value")
    private val NoWarnMessage = "^msg=(\\d\\d\\d\\d\\d)\\s(.*)$".r
  }

  class CollectNoWarns(unit: CompilationUnit, noWarnsFor: Int => Boolean) extends Traverser {
    import global._
    import CollectNoWarns._
    val unusedNoWarn = mutable.HashMap.empty[Regex, (Position, String)]
    val posNoWarnAB = mutable.ArrayBuffer.empty[((Int, Position), (String, Regex))]
    val symNoWarn = mutable.HashMap.empty[(Int, Symbol), (String, Regex)]
    val nowarnPositions = mutable.HashSet.empty[Position] // so we can check for duplicates created for destructuring

    traverse(unit.body)
    val posNoWarn = posNoWarnAB.toSeq // for the benefit of scala 2.13

    override def traverse(tree: Tree): Unit = tree match {
      case md: MemberDef if md.hasSymbolField && md.symbol.hasAnnotation(NowarnAnnotation) =>
        val annots = md.symbol.annotations.filter(_.matches(NowarnAnnotation))
        annots.foreach { a =>
          // The nowarn argument will either be an ordinal string or a (lone) member of a key-value map
          val arg0 = a.stringArg(0) orElse a.assocs.collectFirst { case (ValueName, LiteralAnnotArg(Constant(value))) =>
            value.toString
          }
          if (!arg0.exists(a => a.startsWith("msg=") || a.startsWith("cat=")))
            alarm(OptimusErrors.NOWARN, a.pos, "missing restriction pattern (eg. msg=12345 or cat=CATEGORY)")
          arg0.foreach {
            case NoWarnMessage(id, nws) if noWarnsFor(id.toInt) =>
              try {
                val nwx =
                  nws
                    .split("\\s+")
                    .
                    // allow an arbitrary number of $something at the end
                    map(s => "^" + s + "(\\$.*)*$")
                    .
                    // group together in an OR list
                    mkString("(", "|", ")")
                    .r
                val v = (nws, nwx)
                val i = id.toInt
                unusedNoWarn += nwx -> (a.pos, nws)
                nowarnPositions += a.pos
                val pos = md.pos
                symNoWarn += ((i, md.symbol) -> v)
                if (md.symbol.companion != NoSymbol)
                  symNoWarn += ((i, md.symbol.companion) -> v)
                if (pos.isDefined)
                  posNoWarnAB += (i, pos) -> v
              } catch {
                case p: PatternSyntaxException =>
                  alarm(OptimusErrors.NOWARN, a.pos, s"Illegal deprecation specification ${p.getPattern}")
              }
            case _ =>
          }
        }

        super.traverse(md)
      case _ =>
        super.traverse(tree)
    }
    def complainAboutUnused() = unusedNoWarn.values.foreach { case (pos, msg) =>
      alarm(OptimusNonErrorMessages.UNUSED_NOWARN, pos, msg)
    }
  }

  abstract class AbstractAPICheck(noWarns: CollectNoWarns) extends Traverser {
    import global._
    import noWarns._
    lazy val DeprecatingAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.deprecating")
    lazy val AllowedInAnnotation = rootMirror.getRequiredClass("optimus.graph.allowedIn")

    var inPattern: Boolean = false
    @inline final def savingInPattern[A](value: Boolean)(body: => A): A = {
      val saved = inPattern
      try {
        inPattern = value
        body
      } finally inPattern = saved
    }

    private var pathStack: Seq[Tree] = Seq.empty
    def pathed[T](ts: Tree*)(f: Tree => T): T = {
      pathStack = ts.reverse ++ pathStack
      val ret = f(ts.last)
      pathStack = pathStack.drop(ts.size)
      ret
    }
    protected def path: Seq[Tree] = pathStack
    def currentTree: Option[Tree] = pathStack.headOption

    def preTraverse(tree: Tree): Boolean // return true if should continue
    def checkUndesiredProperties(sym: Symbol, pos: Position): Unit

    final override def traverse(tree: Tree) = pathed(tree)(visit)

    private def visit(tree: Tree): Unit = {
      val sym = tree.symbol

      applyRefchecksToAnnotations(tree)

      if (preTraverse(tree)) tree match {

        case CaseDef(pat, guard, body) =>
          val pat1 = savingInPattern(true) {
            traverse(pat)
          }
          traverse(guard)
          traverse(body)

        case LabelDef(_, _, rhs) if treeInfo.hasSynthCaseSymbol(tree) =>
          savingInPattern(true) {
            traverse(rhs)
          }

        case Apply(fun, args) if fun.symbol.isLabel && treeInfo.isSynthCaseSymbol(fun.symbol) =>
          savingInPattern(false) {
            // https://github.com/scala/bug/issues/7756 If we were in a translated pattern, we can now switch out of
            // pattern mode, as the label apply signals that we are in the user-supplied code in the case body.
            //
            //         Relies on the translation of:
            //            (null: Any) match { case x: List[_] => x; x.reverse; case _ => }'
            //         to:
            //            <synthetic> val x2: List[_] = (x1.asInstanceOf[List[_]]: List[_]);
            //                  matchEnd4({ x2; x2.reverse}) // case body is an argument to a label apply.
            pathed(tree)(super.traverse)
          }
        case vd @ ValDef(_, _, _, rhs) if treeInfo.hasSynthCaseSymbol(tree) =>
          // https://github.com/scala/bug/issues/7716 Don't refcheck the tpt of the synthetic val that holds the selector.
          traverse(rhs)

        case tpt @ TypeTree() =>
          val existentialParams = new mutable.ListBuffer[Symbol]
          doTypeTraversal(tree) {
            case tp @ ExistentialType(tparams, tpe) =>
              existentialParams ++= tparams
            case tp: TypeRef =>
              val tpWithWildcards = deriveTypeWithWildcards(existentialParams.toList)(tp)
              checkTypeRef(tpWithWildcards, tree)
            case _ =>
          }
        case Ident(name) =>
          checkUndesiredProperties(sym, tree.pos)

        case x @ Select(qual, _) =>
          checkSelect(x)
          qual.pos match {
            case NoPosition => checkUndesiredProperties(qual.symbol, x.pos)
            case _          => pathed(x)(super.traverse)
          }
        case _: Import =>
        // let it slide here, since we'll generate an error when it's used

        case _ =>
          pathed(tree)(super.traverse)
      }
    }

    private def doTypeTraversal(tree: Tree)(f: Type => Unit) = if (!inPattern) tree.tpe foreach f

    private def checkSelect(tree: Select) = {
      val sym = tree.symbol

      checkUndesiredProperties(sym, tree.pos)
    }

    private def checkTypeRef(tp: Type, tree: Tree) = tp match {
      case TypeRef(pre, sym, args) =>
        tree match {
          // https://github.com/scala/bug/issues/7783 don't warn about inferred types in first case
          case tt: TypeTree if tt.original == null =>
          case _                                   => checkUndesiredProperties(sym, tree.pos)
        }
      case _ =>
    }

    private def checkAnnotations(tpes: List[Type], tree: Tree) = tpes foreach { tp =>
      checkTypeRef(tp, tree)
    }

    private def applyRefchecksToAnnotations(tree: Tree): Unit = {
      def applyChecks(annots: List[AnnotationInfo]) = {
        checkAnnotations(annots map (_.atp), tree)
        traverseTrees(annots flatMap (_.args))
      }

      tree match {
        case m: MemberDef =>
          val sym = m.symbol
          applyChecks(sym.annotations)

        case tpt @ TypeTree() =>
          if (tpt.original != null) {
            tpt.original foreach {
              case dc: TypeTreeWithDeferredRefCheck =>
                // https://github.com/scala/bug/issues/2416
                applyRefchecksToAnnotations(dc.check())
              case _ =>
            }
          }

          doTypeTraversal(tree) {
            case tp: AnnotatedType =>
              applyChecks(tp.annotations)
            case tp =>
          }
        case _ =>

      }
    }

  }

  class GeneralAPICheck(noWarns: CollectNoWarns) extends AbstractAPICheck(noWarns) {

    import noWarns._

    private val ScopeExtractor = ".*\\(SCOPE=([\\w\\-\\.]+)\\).*".r

    val scopeId = settings.defines.value.collectFirst {
      case s if s.startsWith("-DscopeId=") => s.substring("-DscopeId=".length)
    }

    override def preTraverse(tree: global.Tree): Boolean = {
      checkDeprecatingAnnotationFormat(tree)
      true
    }

    private val DepId = OptimusNonErrorMessages.DEPRECATING.id.sn
    def checkUndesiredProperties(calleeSym: Symbol, callerPos: Position): Unit = {
      // If symbol is deprecated, and the point of reference is not enclosed
      // in a deprecated member issue a warning.
      if (calleeSym.hasAnnotation(DeprecatingAnnotation)) {
        val annInfo = calleeSym.getAnnotation(DeprecatingAnnotation).get
        annInfo.stringArg(0) match {
          case Some(msg) =>
            val typeAndFullName = s"${calleeSym.kindString} ${calleeSym.fullName}"
            val noWarnRegexes: Seq[(Regex, Position)] = {
              posNoWarn.collect {
                case ((DepId, p), (_, r)) if p.includes(callerPos) => (r, p)
              } ++
                path.collect {
                  case callerTree: MemberDef
                      if callerTree.hasSymbolField && symNoWarn.contains((DepId, callerTree.symbol)) =>
                    (symNoWarn((DepId, callerTree.symbol))._2, callerTree.pos)
                }
            }.distinct
            // Check for a match now, so we can record that the nowarn was used, even we don't emit a deprecating warning.
            // We're allowing redundant nowarns along the traversal path (hence fold rather than exists), because they're awfully common.
            val matchedNowarnPositions = noWarnRegexes.collect {
              case (r, p) if r.findFirstIn(calleeSym.fullName).isDefined =>
                unusedNoWarn -= r
                p
            }
            val matched = matchedNowarnPositions.nonEmpty
            if (
              currentOwner.ownerChain
                .exists(x => x.hasAnnotation(DeprecatingAnnotation) || x.companion.hasAnnotation(DeprecatingAnnotation))
            ) {
              alarm(
                OptimusNonErrorMessages.DEPRECATING_LIGHT,
                callerPos,
                typeAndFullName,
                msg + "  (accessed within another deprecated method)"
              )
            } else if (noWarnRegexes.isEmpty) {
              alarm(
                OptimusNonErrorMessages.DEPRECATING,
                callerPos,
                typeAndFullName,
                msg + " (no appropriate @nowarn found)"
              )
            } else if (matched) {
              alarm(OptimusNonErrorMessages.DEPRECATING_LIGHT, callerPos, typeAndFullName, msg)
              val contactInfo = msg match {
                case ScopeExtractor(id) => s"[$id]"
                case _                  => "appropriate"
              }
              matchedNowarnPositions.foreach { p =>
                alarm(OptimusNonErrorMessages.NOWARN_DEPRECATION, p, typeAndFullName, contactInfo)
              }
            } else {
              alarm(
                OptimusNonErrorMessages.DEPRECATING,
                callerPos,
                typeAndFullName,
                msg + " (@nowarn was found but didn't match or was malformed))"
              )
            }
          case _ =>
            alarm(
              OptimusErrors.DEPRECATING_USAGE,
              callerPos,
              s"$calleeSym was missing compile time literal `suggestion` argument."
            )
        }
      }
    }

    private def checkDeprecatingAnnotationFormat(tree: Tree): Unit = {
      tree match {
        case deftree: DefTree if tree.hasSymbolField =>
          val sym = deftree.symbol
          // Add scopeId to deprecation if set
          sym.getAnnotation(DeprecatingAnnotation).foreach { deprecation =>
            if (!deprecation.stringArg(0).exists(_.length > 0))
              alarm(OptimusErrors.DEPRECATING_USAGE, tree.pos, "Missing compile time literal `suggestion` argument.")
            else
              scopeId.foreach { sid =>
                val arg0 = deprecation.stringArg(0).get
                val newArg0 = Literal(Constant(s"$arg0 (SCOPE=$sid)"))
                val newArgs = newArg0 :: deprecation.args.tail
                sym.removeAnnotation(DeprecatingAnnotation)
                sym.addAnnotation(DeprecatingAnnotation, newArgs)
              }
          }
        case _ =>
      }
    }
  }
}
