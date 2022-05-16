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

import optimus.tools.scalacplugins.entity.reporter.CodeStyleNonErrorMessages
import optimus.tools.scalacplugins.entity.reporter.OptimusPluginReporter

import scala.tools.nsc.Global
import scala.tools.nsc.Phase
import scala.tools.nsc.plugins.PluginComponent

class PostTyperCodingStandardsComponent(
    val pluginData: PluginData,
    val global: Global,
    val phaseInfo: OptimusPhaseInfo
) extends PluginComponent
    with OptimusPluginReporter
    with WithOptimusPhase {
  import global._

  override def newPhase(prev: Phase): Phase = new StdPhase(prev) {
    override def apply(unit: CompilationUnit) {
      standardsTraverser.traverse(unit.body)
    }
  }

  private object standardsTraverser extends Traverser {
    var inClass = false
    private lazy val DiscouragedAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.discouraged")
    private def alarmOnCaseClass(mods: Modifiers): Boolean =
      mods.isCase && !(mods.isSealed && mods.hasAbstractFlag) && !mods.isFinal

    private def checkDiscouraged(tree: Tree): Unit = {
      val sym = tree.symbol
      if (sym != null && tree.symbol.hasAnnotation(DiscouragedAnnotation)) {
        val annInfo = sym.getAnnotation(DiscouragedAnnotation).get
        alarm(
          CodeStyleNonErrorMessages.DISOUCRAGED_STATEMENT,
          tree.pos,
          annInfo.stringArg(0).get,
          annInfo.stringArg(1).get)
      }
    }

    override def traverse(tree: Tree): Unit = {
      checkDiscouraged(tree)
      tree match {
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

        case _ => super.traverse(tree)
      }
    }
  }
}
