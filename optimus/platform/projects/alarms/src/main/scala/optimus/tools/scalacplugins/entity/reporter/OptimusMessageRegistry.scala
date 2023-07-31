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
package optimus.tools.scalacplugins.entity.reporter

import java.util.regex.Pattern

/**
 * This is the place we generate optimus specified error/warning message
 *
 * TODO (OPTIMUS-0000): Because currently, plugin don't depend on core project, so we duplicate some code here.
 */
private[optimus] object OptimusMessageRegistry {
  private val prefix = "Optimus"
  private val PluginMessage = s"(?s)$prefix: \\((\\d+)\\) (.*)".r

  def getMessage(alarm: OptimusAlarmBase) = generateOptimusMessage(alarm.id.sn, alarm.message)

  private def generateOptimusMessage(id: Int, message: String) =
    s"$prefix: ($id) $message"

  /**
   * Used in the test framework, get the meaningful payload in the error/warning message
   */
  val suppressedPrefix = s"${OptimusAlarms.SuppressedTag} $prefix"
  private val SuppressedPluginMessage = s"(?s)${Pattern.quote(suppressedPrefix)}: \\((\\d+)\\) (.*)".r

  def getOriginalMessage(msg: String): (Int, String) = msg match {
    case PluginMessage(sn, m)           => (sn.toInt, m)
    case SuppressedPluginMessage(sn, m) => (sn.toInt, m)
    case other                          => (-1, other)
  }

  private val prefixes = List(prefix, suppressedPrefix)
  def isOptimusMessage(msg: String) = prefixes.exists(msg.startsWith)

  /**
   * This is used in an annotation as error message (see optimus/platform/package.scala) We have to put a static string
   * here, otherwise we will get unexpected error message from scala refcheck phase
   */
  final val Illegal_Property_Level_Tweak = "Optimus: (1000) Illegal property-level tweak (Is tweak=true missing?)"
}
