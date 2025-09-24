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
import com.typesafe.config.Config
import com.typesafe.config.ConfigException
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigResolveOptions
import optimus.stratosphere.common.CommonDirectoryStructure
import optimus.stratosphere.logger.CentralLogger

import scala.util.Try

final case class BaseConfigs(defaultConfig: Config, localUserConfig: Config, globalUserConfig: Config)

object BaseConfigs {

  private def baseConfigResolveOptions = ConfigResolveOptions.defaults().setAllowUnresolved(true)

  def apply(directoryStructure: CommonDirectoryStructure)(implicit logger: CentralLogger): BaseConfigs = {
    // config without workspace-specific files
    def defaultConfig: Config = StratoWorkspace(NoWorkspace, Some(logger)).config
    // Load user custom configuration file
    def localUserConfig(directoryStructure: CommonDirectoryStructure): Config =
      Try {
        ConfigFactory.parseFile(directoryStructure.customConfFile.toFile)
      }.map(_.resolve(baseConfigResolveOptions))
        .fold(configParsingErrorHandler, identity)
    // Load User global configuration file
    def globalUserConfig(directoryStructure: CommonDirectoryStructure): Config =
      Try {
        ConfigFactory.parseFile(directoryStructure.userConfFile.toFile)
      }.map(_.resolve(baseConfigResolveOptions))
        .fold(configParsingErrorHandler, identity)

    def configParsingErrorHandler: PartialFunction[Throwable, Config] = {
      case e @ (_: ConfigException.IO | _: ConfigException.UnresolvedSubstitution | _: ConfigException.Parse) =>
        logger.warning(e.getMessage)
        ConfigFactory.empty()
      case e: RuntimeException =>
        logger.warning(s"Unhandled exception:\n${e.getMessage}")
        ConfigFactory.empty()
    }

    BaseConfigs(defaultConfig, localUserConfig(directoryStructure), globalUserConfig(directoryStructure))
  }
}
