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
package optimus.buildtool.runconf.compile

import optimus.buildtool.runconf.RunConf
import optimus.buildtool.runconf.Template

import scala.collection.immutable.Seq

final case class CompileResult(problems: Seq[Problem], runConfs: Seq[RunConf], templates: Seq[Template]) {
  def withProblems(newProblems: Seq[Problem]): CompileResult = copy(problems = newProblems ++ problems)
  def hasErrors: Boolean = problems.exists(_.level == Level.Error)
}

object CompileResult {

  def fromProblem(problem: Problem) = CompileResult(Seq(problem), Seq.empty, Seq.empty)

  val empty = CompileResult(Seq.empty, Seq.empty, Seq.empty)

}
