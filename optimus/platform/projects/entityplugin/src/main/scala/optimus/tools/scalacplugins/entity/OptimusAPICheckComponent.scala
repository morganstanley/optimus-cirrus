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

import optimus.tools.scalacplugins.entity.alarms.AlarmSuppressions
import optimus.tools.scalacplugins.entity.reporter.OptimusErrors
import optimus.tools.scalacplugins.entity.reporter.OptimusNonErrorMessages

import scala.tools.nsc._
import scala.collection.mutable

class OptimusAPICheckComponent(val plugin: EntityPlugin, override val phaseInfo: OptimusPhaseInfo)
    extends GeneralAPICheckComponent(plugin.pluginData, plugin.global, phaseInfo)
    with EntityPluginComponent
    with TypedUtils
    with WithOptimusPhase {
  import global._

  override def newPhase(prev: Phase): Phase = new EntityPluginStdPhase(prev) {
    override def apply0(unit: CompilationUnit): Unit = {
      val nw = new CollectNoWarnsWithFilters(unit, Set(OptimusErrors.INCORRECT_ADVANCED_USAGE.id.sn))
      new OptimusAPICheck(unit, nw).traverse(unit.body)
      nw.complainAboutUnused()
    }
  }

  // Optimus-specific API checks to run in the entity plugin (OptimusAPICheckComponent)
  class OptimusAPICheck(unit: CompilationUnit, nw: CollectNoWarnsWithFilters) extends AbstractAPICheck {
    import global._

    private var inNodeSync = false

    var ignoranceWithin: List[String] = Nil

    override def preTraverse(tree: global.Tree): Boolean = tree match {
      case Import(expr, selectors) if expr.symbol == optimusLanguage =>
        selectors.foreach {
          _.name match {
            case language.autopar => plugin.addLanguageOption(tree.pos.source)(LanguageOption.autopar)
            case _                =>
          }
        }
        true

      case dd @ DefDef(mods, termNames.CONSTRUCTOR, _, _, _, _) =>
        if (hasAsyncAnnotation(tree) || hasNodeAnnotation(tree) && !isEmbeddable(dd.symbol.owner))
          alarm(OptimusErrors.AT_NODE_NOT_SUPPORTED_FOR_CONSTRUCTORS, tree.pos)
        true

      case dd @ DefDef(_, _, _, _, _, rhs)
          if dd.symbol.hasAnnotation(NodeSyncAnnotation) || dd.symbol.nameString.endsWith("$withNode") =>
        val saved = inNodeSync
        inNodeSync = true
        traverse(rhs)
        inNodeSync = saved
        false

      case Apply(TypeApply(Select(_, names.ignoreWithin), _), Literal(Constant(arg: String)) :: within :: Nil) =>
        val saved = ignoranceWithin
        ignoranceWithin = arg :: saved
        traverse(within)
        ignoranceWithin = saved
        false

      case _ =>
        true
    }

    private val alarmed = new mutable.HashSet[(Int, Position)]

    private def allowedToCall(callerPos: Position, calleeSym: Symbol): Boolean = {
      import AlarmSuppressions._

      val nowarn = nw.matches(callerPos, calleeSym)

      def canCall: Boolean = {
        val isNodeGet = calleeSym match {
          case m: MethodSymbol => m.fullName == NodeGet || m.overrides.exists(_.fullName == NodeFutureGet$)
          case _               => false
        }
        !isNodeGet || currentOwner.ownerChain.exists(e => allowedToCallNodeGet(e.fullName))
      }

      nowarn || canCall
    }

    // Used to enforce deprecation of JSR310 (javax.time)
    private val javaxtime = rootMirror.getPackageIfDefined("javax.time").moduleClass

    override def checkUndesiredProperties(calleeSym: Symbol, callerPos: Position): Unit = {
      if (hasMiscFlag(calleeSym, MiscFlags.NODESYNC_ONLY) && !inNodeSync) {
        if (allowedToCall(callerPos, calleeSym))
          alarm(
            OptimusNonErrorMessages.INCORRECT_ADVANCED_USAGE_LIGHT,
            callerPos,
            s"${calleeSym.fullName} in ${currentOwner.fullName}")
        else
          alarm(OptimusErrors.INCORRECT_ADVANCED_USAGE, callerPos, s"${calleeSym.fullName} in ${currentOwner.fullName}")
      } else if (javaxtime.exists && calleeSym.hasTransOwner(javaxtime)) {
        val encClassName = currentOwner.enclClass.fullName
        // Raise alarm unless class is allowed
        if (!AlarmSuppressions._20512.exists(encClassName.startsWith))
          alarmDedup(OptimusErrors.JSR310_USAGE, callerPos, alarmed)
      }
    }
  }
}
