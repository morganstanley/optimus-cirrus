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
package optimus.buildtool.config

import optimus.buildtool.config.NamingConventions._
import optimus.buildtool.config.NpmConfiguration.NpmBuildMode

import scala.collection.immutable.Seq

trait NpmConfiguration {
  def mode: NpmBuildMode
  def libs: Seq[String]
  def npmCommandTemplate: Map[String, String] // OS type (eg. "windows", "linux") to command template
  def npmBuildCommands: Option[Seq[String]]
  def fingerprint: Seq[String]
}

object NpmConfiguration {
  sealed trait NpmBuildMode {
    def name: String
    override def toString: String = name
  }
  object NpmBuildMode {
    case object Production extends NpmBuildMode {
      override val name = "prod"
    }
    case object Development extends NpmBuildMode {
      override val name = "dev"
    }
  }

  private[buildtool] val ModeKey = "mode"
  private[buildtool] val MavenKey = NamingConventions.MavenLibsKey
  private[buildtool] val NpmCommandTemplateKey = "npmCommandTemplate"
  private[buildtool] val NpmBuildCommandsKey = "npmBuildCommands"

  private def getVariant(group: String, name: String, from: Seq[String]): Option[String] =
    from
      .find(node => node.contains(s"$group.$name") && node.contains("variant"))
      .map(node => node.substring(node.lastIndexOf(".") + 1))

  def getNpmVariant(webLibs: Seq[String]): Option[String] = getVariant(NpmGroup, NpmName, webLibs)

  def getPnpmVariant(webLibs: Seq[String]): Option[String] = getVariant(PnpmGroup, PnpmName, webLibs)
}
