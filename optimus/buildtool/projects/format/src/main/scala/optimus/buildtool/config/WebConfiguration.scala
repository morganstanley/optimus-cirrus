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

import optimus.buildtool.config.NpmConfiguration.NpmBuildMode

import scala.collection.immutable.Seq

final case class WebConfiguration(
    mode: NpmBuildMode,
    libs: Seq[String],
    npmCommandTemplate: Map[String, String], // OS type (eg. "windows", "linux") to command template
    npmBuildCommands: Option[Seq[String]])
    extends NpmConfiguration {

  def fingerprint: Seq[String] = {
    Seq(s"[Mode]$mode") ++
      libs.sorted.map { d => s"[Libs]$d" } ++
      npmCommandTemplate.toSeq.sorted.map { case (k, v) => s"[Template:$k]$v" } ++
      npmBuildCommands.getOrElse(Nil).map(s => s"[Command]$s")
  }

  def nodeVariant: Option[String] = NpmConfiguration.getNpmVariant(libs)
  def pnpmVariant: Option[String] = NpmConfiguration.getPnpmVariant(libs)
}
