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
package optimus.stratosphere.config

import optimus.stratosphere.logger.Logger
import org.fusesource.jansi.Ansi.Color

object ConfigSourceLocation extends Enumeration {
  private def color(highlight: Boolean): Color =
    if (highlight) Color.RED else Color.DEFAULT
  final private[config] case class ConfigSourceLocationVal(shortName: String, name: String, highlight: Boolean)
      extends super.Val {
    def short(text: String)(implicit consoleColors: ConsoleColors): String =
      Logger.addColorTo(s"($shortName) $text", color(highlight))
    def pprint(text: String)(implicit consoleColors: ConsoleColors): String =
      if (highlight) Logger.addColorTo(s"($name) $text", color(highlight)) else text
    def legend(implicit consoleColors: ConsoleColors): String =
      Logger.addColorTo(s"$shortName - $name", color(highlight))
  }
  implicit def valueToPlanetVal(x: Value): ConfigSourceLocationVal = x.asInstanceOf[ConfigSourceLocationVal]
  val Global: ConfigSourceLocationVal = ConfigSourceLocationVal("G", "Global", highlight = true)
  val Local: ConfigSourceLocationVal = ConfigSourceLocationVal("L", "Local", highlight = true)
  val Workspace: ConfigSourceLocationVal = ConfigSourceLocationVal("W", "Workspace", highlight = false)
  val Default: ConfigSourceLocationVal = ConfigSourceLocationVal("D", "Default", highlight = false)
  val EnvironmentOverride: ConfigSourceLocationVal =
    ConfigSourceLocationVal("O", "Environment override", highlight = true)

  def legend(implicit consoleColors: ConsoleColors): String =
    ConfigSourceLocation.values
      .map(_.legend)
      .mkString("Config location: ", ", ", "")

}
