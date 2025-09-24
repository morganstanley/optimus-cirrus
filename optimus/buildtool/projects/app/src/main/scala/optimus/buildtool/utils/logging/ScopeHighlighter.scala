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
package optimus.buildtool.utils.logging

import ch.qos.logback.classic.pattern.ClassicConverter
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.pattern.color.ANSIConstants._
import optimus.buildtool.utils.OptimusBuildToolProperties

class ScopeHighlighter extends ClassicConverter {
  private val ContainsScopeIdAndCompletion =
    """^\[([^.\]]+\.[^.\]]+\.[^.\]]+\.[^\]]+)\](.*?)(?:\((completed .* scopes)\))?$""".r

  private val messageHighlighting = OptimusBuildToolProperties.getOrTrue("messageHighlighting")
  private val scopeColor = OptimusBuildToolProperties.getOrElse("scopeColor", BOLD)
  private val completedColor = OptimusBuildToolProperties.getOrElse("completedColor", BOLD + MAGENTA_FG)

  private val PreScope = ESC_START + scopeColor + ESC_END
  private val PreCompletion = ESC_START + completedColor + ESC_END
  private val Post = ESC_START + "0;" + DEFAULT_FG + ESC_END

  override def convert(event: ILoggingEvent): String = if (messageHighlighting) {
    event.getMessage match {
      case ContainsScopeIdAndCompletion(scopeStr, rest, null) =>
        s"[$PreScope$scopeStr$Post]$rest"
      case ContainsScopeIdAndCompletion(scopeStr, rest, completion) =>
        s"[$PreScope$scopeStr$Post]$rest($PreCompletion$completion$Post)"
      case msg => msg
    }
  } else event.getMessage
}
