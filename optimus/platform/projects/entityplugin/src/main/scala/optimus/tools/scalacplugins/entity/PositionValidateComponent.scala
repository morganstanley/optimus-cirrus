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

import scala.tools.nsc._

class PositionValidateComponent(val plugin: EntityPlugin, val phaseInfo: OptimusPhaseInfo)
    extends EntityPluginComponent
    with TypedUtils
    with WithOptimusPhase {
  import global._

  override def newPhase(prev: Phase): Phase = new EntityPluginStdPhase(prev) {
    def apply0(unit: CompilationUnit): Unit = {
      if (plugin.settings.posValidate) new PositionValidate().traverse(unit.body)
      new PoisonedPlaceholderFinder().traverse(unit.body)
    }
  }

  class PoisonedPlaceholderFinder() extends Traverser {
    override def traverse(tree: Tree): Unit = {
      tree match {
        case s: ValOrDefDef if s.symbol.hasAnnotation(PoisonedAnnotation) =>
        // Don't bother recursing into already poisoned def. This is needed so that supers redirection works.
        case PoisonedPlaceholder(msg) => alarm(OptimusErrors.POISONED_PLACEHOLDER, tree.pos, msg.getOrElse(""))
        case _                        => super.traverse(tree)
      }
    }
  }

  class PositionValidate() extends Traverser {
    override def traverse(tree: Tree): Unit = {

      if (tree.pos == null || tree.pos == NoPosition) {
        alarm(
          OptimusNonErrorMessages.NOPOSITION,
          if (
            tree.symbol == null || tree.symbol == NoSymbol || tree.symbol.owner == null || tree.symbol.owner == NoSymbol
          )
            NoPosition
          else tree.symbol.owner.pos,
          tree
        )
      }
      super.traverse(tree)
    }
  }
}
