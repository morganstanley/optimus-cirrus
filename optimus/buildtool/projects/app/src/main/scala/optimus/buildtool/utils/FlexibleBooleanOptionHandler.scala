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

import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.OptionDef
import org.kohsuke.args4j.spi.OptionHandler
import org.kohsuke.args4j.spi.Parameters
import org.kohsuke.args4j.spi.Setter

// Similar to ExplicitOptionHandler, but assume any parameter not matching one of the acceptable values is part
// of the next option/argument.
class FlexibleBooleanOptionHandler(
    parser: CmdLineParser,
    option: OptionDef,
    setter: Setter[Boolean]
) extends OptionHandler[Boolean](parser, option, setter) {

  private val acceptableValues = Map(
    "true" -> true,
    "on" -> true,
    "yes" -> true,
    "1" -> true,
    "false" -> false,
    "off" -> false,
    "no" -> false,
    "0" -> false
  )

  override def parseArguments(params: Parameters): Int = {
    val arg = if (params.size == 0) None else acceptableValues.get(params.getParameter(0).toLowerCase)

    setter.addValue(arg.getOrElse(true))
    arg.size
  }

  override def getDefaultMetaVariable: String = "VALUE"
}
