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

import optimus.tools.scalacplugins.entity.reporter.CodeStyleErrors

import scala.tools.nsc._
import plugins.PluginComponent
import reporter.{OptimusPluginReporter, StagingErrors, StagingNonErrorMessages}

class CodingStandardsComponent(
    val pluginData: PluginData,
    val global: Global,
    val phaseInfo: OptimusPhaseInfo
) extends PluginComponent
    with OptimusPluginReporter
    with WithOptimusPhase {
  import global._

  override def newPhase(prev: Phase): Phase = new StdPhase(prev) {
    override def apply(unit: CompilationUnit): Unit = {
      if (!pluginData.rewriteConfig.anyEnabled)
        standardsTraverser traverse unit.body
    }
  }

  private object standardsTraverser extends Traverser {
    private case class TraversalState(inDefDef: Boolean, inLastExpr: Boolean, inDef: Boolean)
    private object TraversalState {
      def initial: TraversalState = TraversalState(inDefDef = false, inLastExpr = false, inDef = false)
    }

    private object ScalaCollectionWildcardImport {
      def unapply(t: Tree): Boolean = t match {
        case Import(Ident(c), imps) => c == nme.collection && imps.exists(_.name == nme.WILDCARD)
        case Import(Select(Ident(s), c), sel) if s == nme.scala_ => unapply(Import(Ident(c), sel))
        case _                                                   => false
      }
    }

    private var state = TraversalState.initial
    override def traverse(tree: Tree): Unit = {
      tree match {
        case Return(retVal) if state.inLastExpr => alarm(CodeStyleErrors.RETURN_STATEMENT, retVal.pos)

        case vdd: ValOrDefDef =>
          var alreadyTraversed = false
          vdd match {
            case dd: DefDef =>
              // Setting everything to the correct value before traversing the function (preserving old values for after the
              // function traversal is done
              val oldState = state
              state = state.copy(inDefDef = true, inLastExpr = true, inDef = true)
              super.traverse(dd)
              state = oldState
              alreadyTraversed = true
            case ValDef(_, _, _, Return(retVal)) => alarm(CodeStyleErrors.RETURN_STATEMENT, retVal.pos)
            case _                               => super.traverse(tree)
          }

          // might have already traversed this tree above if it was a defdef, don't want to again
          if (!alreadyTraversed) {
            val oldState = state
            state = state.copy(inDef = true)
            super.traverse(tree)
            state = oldState
          }

        case ScalaCollectionWildcardImport() =>
          alarm(StagingErrors.NO_COLLECTION_WILDCARD_IMPORT, tree.pos)

        case Import(pre, _) =>
          def loop(tree: Tree): Unit = {
            def fail(): Unit = { alarm(StagingErrors.NO_PACKAGE_OBJECT_IMPORT, pre.pos) }

            tree match {
              case Ident(nme.PACKAGEkw)     => fail()
              case Select(_, nme.PACKAGEkw) => fail() // could go around again but meh
              case Select(qual, _)          => loop(qual)
              case _                        =>
            }
          }
          loop(pre)

        // all cases below are cases that need special consideration when determining if a return is in the last expression
        // (only care about them when they are in functions, because if it is outside, there will be no return)

        // use super.traverse if you don't care about what is inside the thing you are traversing, use traverse to use this function

        // inLastExpr will be true if if it was in the last expression already and it still is

        case cd: CaseDef if state.inDefDef =>
          // does not necessarily exit the last expression so keep the value of the parent when traversing
          super.traverse(cd.pat)
          super.traverse(cd.guard)
          traverse(cd.body)

        case i: If if state.inDefDef =>
          super.traverse(i.cond)
          traverse(i.thenp)
          traverse(i.elsep)

        case Block(stats, expr) if state.inDefDef =>
          // first parts of a block are by definition not the last expression
          val oldState = state
          state = state.copy(inLastExpr = false)
          super.traverseTrees(stats)
          state = oldState
          traverse(expr)

        case _ => super.traverse(tree)
      }
    }
  }
}
