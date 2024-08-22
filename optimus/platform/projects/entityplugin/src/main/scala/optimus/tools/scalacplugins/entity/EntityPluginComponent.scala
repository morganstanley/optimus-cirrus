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

import scala.collection.mutable
import scala.tools.nsc.Global
import scala.tools.nsc.Phase
import scala.tools.nsc.plugins.PluginComponent
import scala.tools.nsc.transform.Transform
import scala.util.matching.Regex

trait TreeGrep {
  final protected def defer[T](global: Global)(unit: global.CompilationUnit)(f: => T): T = {
    // 1. Set a breakpoint here,
    // 2. When the breakpoint is hit, call setUnitRegex("something matching source file name")
    // 3. Remove this break point and set one at either of the reporter.echo lines, which will be hit whenever
    //    the source file matches.
    val doBreak = shouldBreak(global, unit)
    if (doBreak)
      // 4. When you're at a phase that interests you in the compilation of a source that interests you,
      //    call symGrep("matches name or symbol"), and you'll get back a sequence of matches by line number
      //    with a linked list of enclosing ASTs.
      global.reporter.echo(s"About to enter ${global.phase} for $unit")
    val ret = f
    if (doBreak)
      global.reporter.echo(s"Leaving ${global.phase} for $unit") //
    ret
  }
  // Set regexp to match source file name.
  final def setUnitRegex(regex: String) = {
    TreeGrep.unitRegex = if (regex eq null) null else regex.r
  }
  private var _global: Global = null
  private var _unit: Global#CompilationUnit = null
  private def shouldBreak(global: Global, unit: Global#CompilationUnit): Boolean =
    if ((TreeGrep.unitRegex ne null) && TreeGrep.unitRegex.findFirstMatchIn(unit.toString).isDefined) {
      // This code reduces the amount of boilerplate we'll have to type to call symGrep
      _global = global
      _unit = unit
      true
    } else
      false

  // For each symbol match, return its line number and a linked list of containing ASTs
  final def symGrep(regex: String): collection.Seq[(Int, List[Global#Tree])] = {
    val g: Global = _global
    symGrep(g)(_unit.asInstanceOf[g.CompilationUnit])(regex)
  }
  private def symGrep(global: Global)(unit: global.CompilationUnit)(
      regex: String): collection.Seq[(Int, List[global.Tree])] = {
    import global._
    val r = regex.r
    val b = unit.body
    val paths = mutable.ArrayBuffer.empty[(Int, List[Tree])]

    val traverser = new Traverser {
      var path: List[Tree] = Nil
      override def traverse(tree: Tree): Unit = {
        path = tree :: path
        if (getName(global)(tree).exists(s => r.findFirstMatchIn(s).isDefined))
          paths += ((tree.pos.line, path))
        else {
          super.traverse(tree)
          path = path.tail
        }
      }
    }
    traverser.traverse(b)
    paths
  }

  private def getName(global: Global)(tree: global.Tree): Option[String] = {
    import global._
    if (tree.hasSymbolField && tree.symbol != NoSymbol)
      Some(tree.symbol.nameString)
    else
      tree match {
        case v: NameTreeApi @unchecked => Some(v.name.toString)
        case _                         => None
      }
  }
}

object TreeGrep {
  // Set this to a regex matching the file name if you want to break.
  private var unitRegex: Regex = null
}

trait EntityPluginComponent extends PluginComponent with PluginDataAccess {
  val plugin: EntityPlugin

  override final val global: plugin.global.type = plugin.global
  override final val pluginData: PluginData = plugin.pluginData
  def entitySettings: EntitySettings = plugin.settings

  trait PhaseGrep extends TreeGrep {
    self: global.GlobalPhase =>
    def apply0(unit: global.CompilationUnit): Unit
    final override def apply(unit: global.CompilationUnit): Unit = defer(global)(unit)(apply0(unit))
  }

  abstract class EntityPluginStdPhase(prev: Phase) extends StdPhase(prev) with PhaseGrep

  abstract class EntityPluginGlobalPhase(prev: Phase) extends global.GlobalPhase(prev) with PhaseGrep
}

trait TransformGrep extends TreeGrep {
  self: Transform =>

  protected def newTransformer0(unit: global.CompilationUnit): global.Transformer
  final override def newTransformer(unit: global.CompilationUnit): global.Transformer =
    defer(global)(unit)(newTransformer0(unit))
}
