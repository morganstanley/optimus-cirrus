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
package optimus.buildtool.format

import com.typesafe.config.Config
import optimus.buildtool.config.RegexConfiguration
import optimus.buildtool.format.ConfigUtils._

final case class RulesStructure(rules: Option[RegexConfiguration])

object RulesStructure {

  val Empty = RulesStructure(rules = None)

  private val origin = RulesConfig

  def load(loader: ObtFile.Loader): Result[RulesStructure] =
    loader(origin).flatMap(load)

  def load(config: Config): Result[RulesStructure] =
    load(config, origin)

  def load(config: Config, obtFile: ObtFile): Result[RulesStructure] =
    Result.tryWith(obtFile, config) {
      val rules = RegexConfigurationCompiler.load(config, obtFile)
      rules
        .map(RulesStructure.apply)
        .withProblems(config.checkExtraProperties(obtFile, Keys.regexDefinition))
    }
}
