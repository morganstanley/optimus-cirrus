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

import optimus.buildtool.files.JarAsset
import optimus.buildtool.format.OrderingUtils

final case class ScalaVersionConfig(scalaVersion: ScalaVersion, scalaJars: Seq[JarAsset]) {
  def scalaMajorVersion: String = scalaVersion.scalaMajorVersion
}

final case class ScalaVersion(value: String) extends AnyVal {
  def scalaMajorVersion: String = value.split('.').take(2).mkString(".")
  override def toString: String = value
}

object ScalaVersion {
  implicit val scalaVersionOrdering: Ordering[ScalaVersion] = (x: ScalaVersion, y: ScalaVersion) => {
    OrderingUtils.versionOrdering.compare(x.value, y.value)
  }
}
