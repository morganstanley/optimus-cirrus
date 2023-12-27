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

import ConfigUtils._
import com.typesafe.config.Config
import optimus.buildtool.config.AgentConfiguration
import spray.json._

object AgentConfigurationCompiler {
  def load(config: Config, origin: ObtFile): Result[Option[AgentConfiguration]] = {
    Success(
      config.optionalString("agent.agentClass").map { mainClass =>
        AgentConfiguration(
          mainClass,
          config
            .setOrEmpty("agent.excludes")
            .map { e =>
              // we may need to support a more flexible config format here in future
              ScopeDefinition.loadScopeId(e)
            }
        )
      }
    )
      .withProblems(
        if (config.hasPath("agent"))
          config.getConfig("agent").checkExtraProperties(origin, Keys.agent)
        else Nil
      )
  }

  def asJson(ac: AgentConfiguration) =
    JsObject(
      "agentClass" -> JsString(ac.agentClass),
      "excludes" -> JsArray(
        ac.excluded
          .map(s => ScopeDefinitionCompiler.asJson(s))
          .toSeq: _*
      )
    )

}
