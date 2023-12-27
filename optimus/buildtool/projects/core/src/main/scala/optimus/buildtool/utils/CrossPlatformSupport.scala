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
package optimus.buildtool.utils

import scala.util.matching.Regex

object CrossPlatformSupport {
  // Windows parsing and expansion rules:
  //   - https://stackoverflow.com/questions/4094699/how-does-the-windows-command-interpreter-cmd-exe-parse-scripts/4095133#4095133
  //   - https://stackoverflow.com/questions/4094699/how-does-the-windows-command-interpreter-cmd-exe-parse-scripts/7970912#7970912
  private val simpleWindowsVariablePattern = PatternReplace("""%([\w\d_-]+)%""".r, "\\${$1}")
  private val windowsVariablePatterns = Seq(simpleWindowsVariablePattern)

  def convertToLinuxVariables(expr: String): String =
    windowsVariablePatterns.foldLeft(expr) { case (newExpr, PatternReplace(r, stmt)) =>
      r.replaceAllIn(newExpr, stmt)
    }
  def convertToLinuxVariables(expr: (String, String)): (String, String) =
    expr match {
      case (key, value) => key -> convertToLinuxVariables(value)
    }

  // Unix shell expansion rules:
  //   - https://www.gnu.org/software/bash/manual/html_node/Shell-Parameter-Expansion.html
  private final case class PatternReplace(pattern: Regex, replaceStmt: String)
  private val simpleUnixVariablePattern = PatternReplace("""\$([\w_]+[\w\d_]*)([^$]*)""".r, "%$1%$2")
  private val simpleBracketedUnixVariablePattern = PatternReplace("""\$\{([\w_]+[\w\d_]*)\}""".r, "%$1%")
  private val unixVariablePatterns = Seq(simpleBracketedUnixVariablePattern, simpleUnixVariablePattern)

  def convertToWindowsVariables(expr: String): String =
    unixVariablePatterns.foldLeft(expr) { case (newExpr, PatternReplace(r, stmt)) =>
      r.replaceAllIn(newExpr, stmt)
    }
  def convertToWindowsVariables(expr: (String, String)): (String, String) =
    expr match {
      case (key, value) => key -> convertToWindowsVariables(value)
    }

  // Runtime evaluation with automatic conversion
  def evaluateVar(expr: String, env: Map[String, String], os: OS = OS.current): String =
    // Currently the OS does not matter as we resolve both
    ParameterExpansionParser.eval(expr, env)

  def evaluateKeyVar(expr: (String, String), env: Map[String, String], os: OS = OS.current): (String, String) =
    expr match {
      // Exclude self from evaluation
      case (key, value) => key -> evaluateVar(value, env - key, os)
    }

  def evaluateVarList(exprs: Seq[String], env: Map[String, String], os: OS = OS.current): Seq[String] =
    exprs.map(evaluateVar(_, env, os))

  def evaluateVarMap(exprs: Map[String, String], env: Map[String, String], os: OS = OS.current): Map[String, String] =
    exprs.map(evaluateKeyVar(_, env, os))
}
