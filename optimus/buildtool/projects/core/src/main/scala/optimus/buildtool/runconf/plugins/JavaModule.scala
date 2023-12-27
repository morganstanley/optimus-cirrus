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
package optimus.buildtool.runconf.plugins

import optimus.buildtool.config.StaticConfig

import java.nio.file.Path
import java.nio.file.Paths

final case class JavaModule(
    meta: Option[String] = None,
    project: Option[String] = None,
    version: Option[String] = None,
    homeOverride: Option[String] = None
) {
  override def toString: String = {
    s"""
       |  meta = $meta
       |  project = $project
       |  version = $version
       |  homeOverride = $homeOverride
       |""".stripMargin
  }

  def isDefined: Boolean =
    (meta.isDefined && project.isDefined && version.isDefined) || homeOverride.isDefined

  def isPartiallyDefined: Boolean = this != JavaModule.undefined

  private def afsPath: Option[Path] =
    for {
      m <- meta
      p <- project
      v <- version
    } yield Paths.get(s"${StaticConfig.string("afsRoot")}dist/$m/PROJ/$p/$v/exec")

  def pathOption: Option[Path] = homeOverride map (Paths.get(_)) orElse afsPath

}

object JavaModule {
  final val undefined = JavaModule(None, None, None, None)
}
