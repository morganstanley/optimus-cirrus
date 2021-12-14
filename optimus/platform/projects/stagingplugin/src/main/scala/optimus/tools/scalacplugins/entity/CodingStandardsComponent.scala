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
import plugins.PluginComponent
import reporter.{ OptimusPluginReporter, StagingErrors, StagingNonErrorMessages }

class CodingStandardsComponent(
    val pluginData: PluginData,
    val global: Global,
    val phaseInfo: OptimusPhaseInfo,
) extends PluginComponent
    with OptimusPluginReporter
    with WithOptimusPhase {
  import global._

  override def newPhase(prev: Phase): Phase = new StdPhase(prev) {
    override def apply(unit: CompilationUnit) {
      standardsTraverser traverse unit.body
    }
  }

  private object standardsTraverser extends Traverser {

    var inDef = false

    override def traverse(tree: Tree) {
      tree match {

        case vdd: ValOrDefDef =>
          import vdd._
          if(!inDef && mods.isImplicit && mods.isPublic && tpt.isEmpty)
            alarm(StagingNonErrorMessages.UNTYPED_IMPLICIT(name), tree.pos)
          val wasInDef = inDef
          inDef = true
          super.traverse(tree)
          inDef = wasInDef

        case Import(pre, _) =>
          def loop(tree: Tree) {
            def fail() { alarm(StagingErrors.NO_PACKAGE_OBJECT_IMPORT, pre.pos) }

            tree match {
              case Ident(nme.PACKAGEkw)     => fail()
              case Select(_, nme.PACKAGEkw) => fail() // could go around again but meh
              case Select(qual, _)          => loop(qual)
              case _                        =>
            }
          }
          loop(pre)

        case _ => super.traverse(tree)
      }
    }
  }
}
