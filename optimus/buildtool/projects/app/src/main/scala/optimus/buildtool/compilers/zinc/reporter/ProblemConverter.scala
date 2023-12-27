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
import optimus.buildtool.artifacts.NotAnOptimusMessage
import optimus.buildtool.artifacts.OptimusMessage
import optimus.buildtool.config.ConfigurableWarnings
import optimus.buildtool.config.MessageConsequences
import xsbti.Problem

/**
 * A configurable message converter.
 */
class ProblemConverter(val config: ConfigurableWarnings) extends ConvertsProblems {
  import MessageConsequences._
  override def apply(raisedAt: Severity, problem: Problem): CompilationMessage = {
    val rawMsg = problem.message()
    val parsedMessage = OptimusMessageParser.parse(rawMsg)
    val pos = MessagePosition(problem.position())
    val (isNew, newSeverity) = config.isNewOrFatal(raisedAt, parsedMessage) match {
      // new warning fatal-ness is determined downstream, once we know which files were modified.
      case NewWarning  => (true, raisedAt)
      case _: NonFatal => (false, raisedAt)
      case _: Fatal    => (false, Severity.Error)

    }

    parsedMessage match {
      case NotAnOptimusMessage(msg) =>
        CompilationMessage(pos, msg, newSeverity, None, false, isNew)
      case OptimusMessage(msg, alarmId, isSuppressed) =>
        CompilationMessage(pos, msg, newSeverity, Some(alarmId.toString), isSuppressed, isNew)
    }
  }
}
