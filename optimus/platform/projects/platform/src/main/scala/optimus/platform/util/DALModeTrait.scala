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
package optimus.platform.util

import optimus.config.RuntimeConfiguration
import optimus.config.{RuntimeComponents => CoreRuntimeComponents}
import optimus.platform.EvaluationContext
import optimus.platform.RuntimeEnvironment
import optimus.platform.dal.RuntimeProperties
import optimus.platform.dal.config.DalEnv
import optimus.platform.dsi.protobufutils.DALProtoClient
import optimus.platform.runtime.RuntimeComponents

trait DALModeTrait {
  def runtimeComponents: CoreRuntimeComponents = EvaluationContext.env.config
  def runtimeInfo: RuntimeConfiguration = runtimeComponents.runtimeConfig
  def getDALMode: String = dalMode
  def dalMode: String = runtimeInfo.mode
  def dalEnv: DalEnv = DalEnv(dalMode)

  def isProdDalEnv: Boolean = RuntimeConfiguration.isProdEnv(runtimeInfo.env)

  def isMockDalEnv: Boolean = {
    val env = runtimeInfo.env
    val mode = dalMode
    (env == RuntimeEnvironment.KnownNames.EnvMock && mode == "mock") || (env == RuntimeEnvironment.KnownNames.EnvTest && mode == "test")
  }

  def isAsyncDalEnabled: Boolean =
    runtimeComponents match {
      case rc: RuntimeComponents => rc.asyncConfigOpt.exists(_.async)
      case _                     => (new DALProtoClient.Configuration).asyncConfig.async
    }

  def getAppId: Option[String] = {
    runtimeInfo.getString(RuntimeProperties.DsiAppIdProperty)
  }

}

object DALUtil extends DALModeTrait
