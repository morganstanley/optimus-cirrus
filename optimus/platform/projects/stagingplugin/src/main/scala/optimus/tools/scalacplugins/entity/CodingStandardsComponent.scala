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

    // used to check if return is in last expression (and therefore should cause error)
    var inLastExpr = false
    var inDefDef = false

    override def traverse(tree: Tree) {
      tree match {
        case Return(retVal) if inLastExpr => alarm(CodeStyleErrors.RETURN_STATEMENT, retVal.pos);

        case vdd: ValOrDefDef =>
          import vdd._
          if (!inDef && mods.isImplicit && mods.isPublic && tpt.isEmpty)
            alarm(StagingNonErrorMessages.UNTYPED_IMPLICIT(name), tree.pos)

          var alreadyTraversed = false
          vdd match {
            case dd: DefDef =>
              // Setting everything to the correct value before traversing the function (preserving old values for after the
              // function traversal is done
              val wasInDefDef = inDefDef
              inDefDef = true
              val wasInLastExpr = inLastExpr
              inLastExpr = true
              val wasInDef = inDef
              inDef = true
              super.traverse(dd)
              alreadyTraversed = true
              inDef = wasInDef
              inLastExpr = wasInLastExpr
              inDefDef = wasInDefDef
            case ValDef(_, _, _, Return(retVal)) => alarm(CodeStyleErrors.RETURN_STATEMENT, retVal.pos)
            case _                               => super.traverse(tree)
          }

          // might have already traversed this tree above if it was a defdef, don't wnat to again
          if (!alreadyTraversed) {
            val wasInDef = inDef
            inDef = true
            super.traverse(tree)
            inDef = wasInDef
          }

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

        // all cases below are cases that need special consideration when determining if a return is in the last expression
        // (only care about them when they are in functions, because if it is outside, there will be no return)

        // use super.traverse if you don't care about what is inside the thing you are traversing, use traverse to use this function

        // inLastExpr will be true if if it was in the last expression already and it still is

        case cd: CaseDef if inDefDef =>
          // does not necessarily exit the last expression so keep the value of the parent when traversing
          super.traverse(cd.pat)
          super.traverse(cd.guard)
          traverse(cd.body)

        case i: If if inDefDef =>
          val wasInLastExpr = inLastExpr
          val isElseDefined = i.elsep match {
            case Literal(Constant(())) => false
            case _                     => true
          }

          // if can only be last expression if it has a corresponding else
          inLastExpr = isElseDefined && wasInLastExpr
          super.traverse(i.cond)
          traverse(i.thenp)
          if (isElseDefined) traverse(i.elsep)
          inLastExpr = wasInLastExpr

        case Block(stats, expr) if inDefDef =>
          // first parts of a block are by definition not the last expression
          val wasInLastExpr = inLastExpr
          inLastExpr = false
          super.traverseTrees(stats)
          inLastExpr = wasInLastExpr
          val wasInLastExpression2 = inLastExpr
          traverse(expr)
          inLastExpr = wasInLastExpression2

        case _ => super.traverse(tree)
      }
    }
  }
}
