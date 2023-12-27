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
package optimus.buildtool.config

import optimus.buildtool.artifacts.MaybeOptimusMessage
import optimus.buildtool.artifacts.NotAnOptimusMessage
import optimus.buildtool.artifacts.OptimusMessage
import optimus.buildtool.artifacts.Severity

object ConfigurableWarnings {
  val empty = ConfigurableWarnings(false, Set.empty, Set.empty)
}

final case class ConfigurableWarnings(fatalWarnings: Boolean, nonfatal: Set[Int], newWarnings: Set[Int]) {
  import MessageConsequences._

  // The default warning consequences
  private val default: MessageConsequences = if (fatalWarnings) FatalWarning else Warning

  def isNewOrFatal(raisedAt: Severity, msg: MaybeOptimusMessage): MessageConsequences = {
    raisedAt match {
      // These ones are the same as before
      case Severity.Error => Error
      case Severity.Info  => Info

      // warnings have special cases
      case Severity.Warning =>
        msg match {
          // for non optimus messages, we use whatever is the default
          case NotAnOptimusMessage(_) => default

          // if a message is locally suppressed, it's nonfatal
          case OptimusMessage(_, _, true) => LocallySuppressed

          // otherwise we'll do some more checks
          case OptimusMessage(_, id, false) =>
            if (nonfatal.contains(id)) Scoped
            else if (newWarnings.contains(id)) NewWarning
            else default
        }
    }
  }
}

// Enum for message fatality
sealed trait MessageConsequences
object MessageConsequences {
  sealed trait Fatal extends MessageConsequences
  final case object Error extends Fatal
  final case object FatalWarning extends Fatal

  sealed trait NonFatal extends MessageConsequences
  final case object Info extends NonFatal
  final case object Warning extends NonFatal
  final case object LocallySuppressed extends NonFatal
  final case object Scoped extends NonFatal

  // New warnings may or may not be fatal... We have to explicitly look at message positions etc. so the final
  // determination has to be done in StandardBuilder
  final case object NewWarning extends MessageConsequences
}
