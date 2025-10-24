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
package optimus.stratosphere.common

import com.typesafe.config.Config
import optimus.stratosphere.filesanddirs.PathsOpts._
import optimus.stratosphere.filesanddirs.PropertiesUtils

import java.nio.file.Paths
import scala.util.matching.Regex

sealed trait PluginInfo {
  def name: String
  def version: String
  def location: String
  def isEnabled: Boolean
  def optional: Boolean
  def isExtension: Boolean
  def removeBundledScala: Boolean
}

final case class PluginFromArtifactory(
    name: String,
    version: String,
    location: String,
    isEnabled: Boolean,
    optional: Boolean,
    isExtension: Boolean,
    removeBundledScala: Boolean)
    extends PluginInfo {

  def artifactoryPath(artifactoryRoot: String): String = artifactoryRoot + "/" + location
}

final case class PluginFromFileSystem(
    name: String,
    version: String,
    location: String,
    isEnabled: Boolean,
    optional: Boolean,
    isExtension: Boolean,
    removeBundledScala: Boolean)
    extends PluginInfo {
  require(optional || Paths.get(location).exists(), s"Plugin not found at $location, please check the configuration.")
}

object PluginInfo {
  val AfVersion: Regex = """.*[-|_](\d.*?)\.zip""".r

  private val versionKey = "plugin.version"
  private val nameKey = "plugin.name"
  private val locationKey = "plugin.location"
  private val localKey = "plugin.local"

  def fromConfig(config: Config, isOptional: Boolean = false, isExtension: Boolean = false): PluginInfo = {
    val name: String = config.getString("name")
    val isEnabled: Boolean = if (config.hasPath("enabled")) config.getBoolean("enabled") else true
    val removeBundledScala =
      if (config.hasPath("remove-bundled-scala")) config.getBoolean("remove-bundled-scala") else false
    if (config.hasPath("path")) {
      val path: String = config.getString("path")
      val props = PropertiesUtils.fromFile(path + "/content.properties")
      val locationPrefix = if (props.getProperty(localKey, "false").toBoolean) path + "/" else ""
      val pluginLocation = s"$locationPrefix/${props.getProperty(locationKey)}"

      PluginFromFileSystem(
        name,
        props.getProperty(versionKey),
        pluginLocation,
        isEnabled,
        isOptional,
        isExtension,
        removeBundledScala)
    } else {
      val artifactoryLocation = config.getString("artifactory-path")
      val version =
        if (config.hasPath("version")) {
          config.getString("version")
        } else {
          artifactoryLocation match {
            case PluginInfo.AfVersion(version) => version
          }
        }
      PluginFromArtifactory(name, version, artifactoryLocation, isEnabled, isOptional, isExtension, removeBundledScala)
    }
  }
}
