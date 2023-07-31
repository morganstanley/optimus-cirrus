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
package optimus.stratosphere.handlers

import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.OptionDef
import org.kohsuke.args4j.spi.OptionHandler
import org.kohsuke.args4j.spi.Parameters
import org.kohsuke.args4j.spi.Setter

/**
 * Allows to parse strings into Option[String]. When the arg is not specified, we get None instead of default null.
 */
class OptionOfStringOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[_ >: Option[String]])
    extends OptionHandler[Option[String]](parser, option, setter) {

  override def parseArguments(params: Parameters): Int = {
    setter.addValue(Option(params.getParameter(0)))
    1
  }

  override def getDefaultMetaVariable(): String =
    "VAL" // there's a constant for this value in the actual trunk of args4j
}
