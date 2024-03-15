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

import optimus.tools.scalacplugins.entity.reporter.{OptimusPluginReporter, StagingErrors, StagingNonErrorMessages}

import scala.tools.nsc._
import scala.tools.nsc.plugins.PluginComponent
import scala.tools.nsc.transform.Transform

class StagingComponent(
    val pluginData: PluginData,
    val global: Global,
    val phaseInfo: OptimusPhaseInfo,
    stagingEnabled: => Boolean = true)
    extends PluginComponent
    with Transform
    with OptimusPluginReporter
    with WithOptimusPhase {
  import global._

  object names {
    val optimus = newTermName("optimus")
    val StagingImports = newTermName("StagingImports") // magic name
  }
  object tpnames {
    val staged = newTypeName("staged")
  }

  lazy val DefaultSeq_toVarargsSeq: Symbol = {
    val mod = rootMirror.getModuleIfDefined("optimus.scala212.DefaultSeq")
    mod.info.decl(newTermName("toVarargsSeq"))
  }

  // Since this necessarily runs pre-typer, we have no idea what the actual annotation is.
  // This used to go through some contortions to support @platform.staged and @optimus.platform.staged,
  // but since that's already somewhat best effort and error-prone we may as well just admit to the unreliability.
  def isAnnotationNamed(tree: Tree, expectedName: TypeName) = {
    tree match {
      case Apply(Select(New(Ident(`expectedName`)), nme.CONSTRUCTOR), _) => true
      case _                                                             => false
    }
  }

  def newTransformer(unit: CompilationUnit) = new Staging(unit)

  class Staging(unit: CompilationUnit) extends Transformer {
    import global._

    def hasClass(args: List[Tree]): Boolean = {
      val Literal(Constant(className: String)) = args.head
      try {
        val sym = rootMirror.getClassByName(className)
        sym.info != NoType
      } catch {
        case ex: MissingRequirementError => false
      }
    }

    def scalaVersionRange(args: List[Tree]): Boolean = {
      val Literal(Constant(range: String)) = args.head
      val (lower, upper) = range.split(':').toList match {
        case x :: Nil if range.last == ':' => (x, "")
        case x :: Nil if range.head == ':' => ("", x) // defensive: impossible as split should parse to "" :: x
        case x :: Nil                      => (x, x)
        case x :: y :: Nil                 => (x, y)
        case _                             => abort(s"Illegal range specified in scalaVersionRange")
      }
      (lower == "" || ScalaVersionData.scalaVersion >= lower) && (upper == "" || ScalaVersionData.scalaVersion <= upper)
    }

    def hasModule(args: List[Tree]): Boolean = {
      val Literal(Constant(className: String)) = args.head
      try {
        val sym = rootMirror.getModuleByName(className)
        sym.info != NoType
      } catch {
        case ex: MissingRequirementError => false
      }
    }

    private[this] def getMember(args: List[Tree], getSymbol: String => Symbol): Symbol = {
      val Literal(Constant(fullName: String)) = args.head

      val idx = fullName.lastIndexOf(".")
      if (idx == -1) {
        NoSymbol
      } else {
        val className = fullName.substring(0, idx)
        val methodName = fullName.substring(idx + 1, fullName.length)

        try {
          val klass = getSymbol(className)
          definitions.getMember(klass, newTermName(methodName))
        } catch {
          case ex: Exception =>
            NoSymbol
        }
      }
    }

    def hasClassMember(args: List[Tree]) = getMember(args, rootMirror.getClassByName) != NoSymbol
    def hasModuleMember(args: List[Tree]) = getMember(args, rootMirror.getModuleByName) != NoSymbol

    val StagingMarkersBase = rootMirror.getRequiredClass("optimus.platform.internal.StagingMarkersBase")
    val Implemented = StagingMarkersBase.info.decl(newTermName("Implemented")).tpe.typeSymbol
    val NotImplemented = StagingMarkersBase.info.decl(newTermName("NotImplemented")).tpe.typeSymbol
    val MarkerToBeDeleted = StagingMarkersBase.info.decl(newTermName("MarkerToBeDeleted")).tpe.typeSymbol

    def hasStagingMarker(args: List[Tree]): Boolean = {
      val sym = getMember(args, rootMirror.getClassByName)
      sym.info.resultType.typeSymbol match {
        case NoSymbol | NotImplemented =>
          false
        case Implemented =>
          true
        case MarkerToBeDeleted =>
          val Literal(Constant(fullName: String)) = args.head
          alarm(StagingErrors.STAGING_DEPRECATED, args.head.pos, fullName)
          true
        case t =>
          alarm(StagingNonErrorMessages.UNKNOWN_STAGING_MARKER, args.head.pos, sym.tpe)
          false
      }
    }

    val funcs = Map(
      "hasClass" -> hasClass _,
      "hasModule" -> hasModule _,
      "hasClassMember" -> hasClassMember _,
      "hasModuleMember" -> hasModuleMember _,
      "hasStagingMarker" -> hasStagingMarker _,
      "scalaVersionRange" -> scalaVersionRange _
    )

    def eval(tree: Tree): Boolean = tree match {
      case Literal(Constant(false)) => false
      case Literal(Constant(true))  => true
      case Apply(Ident(name), args) if funcs contains name.toString =>
        funcs(name.toString)(args)
      case Apply(Select(lhs, nme.ZAND), rhs :: Nil) =>
        eval(lhs) && eval(rhs)
      case Apply(Select(lhs, nme.ZOR), rhs :: Nil) =>
        eval(lhs) || eval(rhs)
      case Select(qual, nme.UNARY_!) =>
        !eval(qual)
      case _ => {
        alarm(StagingErrors.INVALID_STAGING_PREDICATE, tree.pos, tree)
        true
      }
    }

    def expand(tree: Tree) = tree match {
      case Block(stats, expr) =>
        (stats map transform) :+ transform(expr)
      case _ => transform(tree) :: Nil
    }

    override def transform(tree: Tree): Tree =
      if (!stagingEnabled) otherTransforms(tree)
      else
        tree match {
          case ModuleDef(_, name, t @ Template(_, _, init :: If(ann @ Annotated(annot, cond), ttree, ftree) :: Nil))
              if name.decodedName.toString == "StagingImports" && isAnnotationNamed(annot, tpnames.staged) =>
            val stats =
              if (eval(cond))
                expand(ttree)
              else
                expand(ftree)
            if (stats.isEmpty)
              Block()
            else if (stats.size == 2)
              stats.head
            else {
              alarm(StagingErrors.MUTIPLE_STAGING_OBJECT, tree.pos)
              EmptyTree
            }
          case _: Template =>
            super.transform(tree) match {
              case Template(parents, self, List(Block(stats, expr))) =>
                treeCopy.Template(tree, parents, self, stats :+ expr)
              case t => t
            }
          case If(Annotated(annot, cond), ttree, ftree) if isAnnotationNamed(annot, tpnames.staged) =>
            if (eval(cond)) transform(ttree)
            else transform(ftree)
          case _ =>
            otherTransforms(tree)
        }

    private var suppressVarargsTransform: Boolean = false
    private def otherTransforms(tree: Tree): Tree = tree match {
      case Typed(expr, ws @ Ident(tpnme.WILDCARD_STAR))
          if !suppressVarargsTransform && pluginData.varargs213Compat && DefaultSeq_toVarargsSeq != NoSymbol =>
        // Automatically convert s.c.Seq to s.c.i.Seq when targetting Scala 2.13. This will be removed
        // in favour of explicit rewrites of source code once 2.12 support is dropped.
        // TODO (OPTIMUS-48502): Drop this once we drop 2.12 support and then adopt 2.13 meaning of Seq
        val expr1 = atPos(expr.pos.makeTransparent)(
          gen.mkMethodCall(gen.mkAttributedRef(DefaultSeq_toVarargsSeq), super.transform(expr) :: Nil))
        treeCopy.Typed(tree, expr1, ws)
      // Scala 2.12 and 2.13 have a bug where unstable arguments used as _* parameters to eta expanded methods
      // cause a compilation error, so we can't do the DefaultSeq_toVarargsSeq rewrite on these
      case Typed(_, Function(Nil, EmptyTree)) =>
        val prev = suppressVarargsTransform
        suppressVarargsTransform = true
        try super.transform(tree)
        finally suppressVarargsTransform = prev
      case _ =>
        super.transform(tree)
    }
  }

  // this enables us to access pluginData in OptimusMacroReporter by casting Transform$Phase to a PluginDataAccess
  class StagingComponentPhase(prev: scala.tools.nsc.Phase) extends Phase(prev) with PluginDataAccess {
    val pluginData = StagingComponent.this.pluginData
  }
  override def newPhase(prev: scala.tools.nsc.Phase): StdPhase = new StagingComponentPhase(prev)
}
