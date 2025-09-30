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
package optimus.stratosphere.utils

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigRenderOptions
import com.typesafe.config.ConfigResolveOptions
import com.typesafe.config.ConfigValueFactory
import optimus.stratosphere.bootstrap.config.StratosphereConfig
import optimus.stratosphere.config
import optimus.stratosphere.config.Extractor
import optimus.stratosphere.config.StratoWorkspaceCommon
import optimus.stratosphere.filesanddirs.PathsOpts._

import java.nio.file.Path
import scala.util.Try

object ConfigUtils {

  private def parseProperty(propertyName: String, value: Any, prepend: Boolean = false, append: Boolean = false)(
      implicit workspace: StratoWorkspaceCommon): Config = {

    def textValue = ConfigValueFactory.fromAnyRef(value).render(ConfigRenderOptions.concise.setFormatted(true))
    def configFromProperty(v: Any) = {
      val text = if (prepend) {
        s"""$propertyName = $v$${?$propertyName}"""
      } else if (append) {
        s"""$propertyName = $${?$propertyName}$v"""
      } else {
        s"$propertyName = $v"
      }

      ConfigFactory.parseString(text)
    }
    val config = Try(configFromProperty(value)).getOrElse(configFromProperty(textValue))
    config
  }

  def loadConfig(configFile: Path)(implicit workspace: StratoWorkspaceCommon): Config = {
    val modifyingNoWorkspaceConf =
      workspace.directoryStructure.customConfFile == configFile && workspace.isOutsideOfWorkspace
    require(!modifyingNoWorkspaceConf, "Trying to set the workspace config while outside of workspace")
    configFile.file.readLock {
      ConfigFactory.parseFile(configFile.toFile)
    }
  }

  def hasProperty[A: Extractor](configFile: Path)(propertyName: String): Boolean =
    readConfig[Option[A]](configFile)(propertyName).isDefined

  def updateConfigProperty(
      configFile: Path)(propertyName: String, value: Any, prepend: Boolean = false, append: Boolean = false)(implicit
      workspace: StratoWorkspaceCommon): Unit = {
    val currentConfig = loadConfig(configFile)
    val updatedConfig = parseProperty(propertyName, value, prepend, append).withFallback(currentConfig)
    workspace.update(propertyName)(value)
    configFile.file.writeConfig(updatedConfig)
  }

  def writeConfigProperties(configFile: Path)(properties: (String, Any)*)(implicit
      workspace: StratoWorkspaceCommon): Unit = {
    val currentConfig = ConfigFactory.empty()
    val updatedConfig = properties.foldLeft[Config](currentConfig) { case (config, (key, value)) =>
      parseProperty(key, value).withFallback(config)
    }
    configFile.file.writeConfig(updatedConfig)
  }

  def removeConfigProperties(configFile: Path)(propertyNames: String*): Unit = {
    val initialConfig = configFile.file.readLock { ConfigFactory.parseFile(configFile.toFile) }
    val config = propertyNames.foldLeft(initialConfig)((config, property) => config.withoutPath(property))
    configFile.file.writeConfig(config)
  }

  def readProperty[A: Extractor](config: Config)(propertyName: String): A =
    implicitly[Extractor[A]].extract(config, propertyName)

  def readConfig[A: Extractor](configFile: Path)(propertyName: String): A = {
    val allowUnresolved = ConfigResolveOptions.defaults().setAllowUnresolved(true)
    val config = configFile.file.readLock { ConfigFactory.parseFile(configFile.toFile).resolve(allowUnresolved) }
    readProperty(config)(propertyName)
  }

  def readWithDefault[A: Extractor](configFile: Path)(propertyName: String, default: => A): A =
    readConfig[Option[A]](configFile)(propertyName).getOrElse(default)

  def repositoryConfigName(workspace: config.StratoWorkspace): String = {
    val workspaceRoot = workspace.directoryStructure.stratosphereWorkspaceDir
    StratosphereConfig.getRepositoryConfigName(workspaceRoot)
  }
}
