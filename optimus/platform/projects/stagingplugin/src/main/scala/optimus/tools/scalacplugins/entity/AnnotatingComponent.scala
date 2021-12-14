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

import scala.tools.nsc._
import scala.tools.nsc.plugins._

/**
 * Adds annotations to pre-existing (i.e. library) symbols.
 * Currently only supports @deprecated but could easily be modified to add other annotations.
 */
class AnnotatingComponent(
    val pluginData: PluginData,
    val global: Global,
    val phaseInfo: OptimusPhaseInfo,
) extends PluginComponent
    with WithOptimusPhase {
  import global._

  /**
   * lookup class / object or term member
   *   - a.C: class C and object C
   *   - a.C.foo: term members `foo` in class or object C
   */
  private def lookup(name: String): Seq[Symbol] = {
    def templates(n: String) = Seq(rootMirror.getModuleIfDefined(n), rootMirror.getClassIfDefined(n)).filter(_.exists)
    val tps = templates(name)
    if (tps.nonEmpty) tps
    else {
      val dot = name.lastIndexOf(".")
      val owners = templates(name.substring(0, dot))
      owners.flatMap(_.info.member(TermName(name.substring(dot + 1))).alternatives)
    }
  }

  private def useInstead(alt: String, neu: Boolean = false) =
    s"${if (neu) "[NEW]" else ""}use $alt instead (deprecated by optimus staging)"

  private val deprecations = Seq[(String, String)](
    // "org.something.Deprecated" -> useInstead("org.something.ToUseInstead")
  )

  private val deprecatings = Seq[(String, String)](
    "scala.Predef.StringFormat.formatted" -> useInstead(
      "`formatString.format(value)` or the `f\"\"` string interpolator",
      neu = true)
  )

  private lazy val deprecatedAnno = rootMirror.getRequiredClass("scala.deprecated")
  private lazy val deprecatingAnno = rootMirror.getClassIfDefined("optimus.platform.annotations.deprecating")
  private lazy val constAnno = rootMirror.getClassIfDefined("scala.annotation.ConstantAnnotation")

  def newPhase(prev: scala.tools.nsc.Phase): StdPhase = new StdPhase(prev) {
    private def add(targets: Seq[(String, String)], annot: Symbol): Unit = annot match {
      case annotClass: ClassSymbol =>
        targets.foreach {
          case (oldName, msg) =>
            lookup(oldName).foreach { oldSym =>
              addAnnotationInfo(oldSym, annotClass, List(msg))
            }
        }

      case _ => // annot class not found on classpath, do nothing
    }
    def apply(unit: global.CompilationUnit): Unit = {
      add(deprecations, deprecatedAnno)
      add(deprecatings, deprecatingAnno)
    }
  }

  private def addAnnotationInfo(target: Symbol, annotCls: ClassSymbol, args: List[Any]): Unit =
    if (target.exists) {
      val isConst = constAnno.exists && annotCls.isNonBottomSubClass(constAnno)
      def annotArgs = args.iterator.map(a => Literal(Constant(a))).toList
      def annotAssocs = {
        def toConstArg(a: Any): ClassfileAnnotArg = a match {
          case a: Array[_] => ArrayAnnotArg(a.map(toConstArg))
          case c           => LiteralAnnotArg(Constant(c))
        }
        args
          .zip(annotCls.primaryConstructor.info.params)
          .iterator
          .map {
            case (arg, paramSym) => (paramSym.name, toConstArg(arg))
          }
          .toList
      }

      val anno = AnnotationInfo(annotCls.tpe, if (isConst) Nil else annotArgs, if (isConst) annotAssocs else Nil)
      target.addAnnotation(anno)
    }
}
