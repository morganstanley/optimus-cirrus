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
package optimus.buildtool.compilers.zinc.reporter

import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.Severity
import optimus.buildtool.artifacts.MessagePosition
import xsbti.Problem

/**
 * A converter that doesn't actually do anything.
 *
 * TODO (OPTIMUS-52851): remove in favour of ProblemConverter
 */
object NoOpConverter extends ConvertsProblems {
  override def apply(raisedAt: Severity, problem: Problem): CompilationMessage =
    CompilationMessage(MessagePosition(problem.position()), problem.message(), raisedAt)
  override def reapply(prevMessage: CompilationMessage): CompilationMessage = prevMessage
}
