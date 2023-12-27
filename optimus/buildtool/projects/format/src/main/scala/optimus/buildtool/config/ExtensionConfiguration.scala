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

import optimus.buildtool.files.FileAsset

final case class ExtensionConfiguration(extensions: Map[String, ExtensionProperties]) {
  def modeForExtension(file: FileAsset): Option[OctalMode] = {
    val ext = NamingConventions.suffix(file)
    extensions.get(ext).map(_.mode)
  }
}

object ExtensionConfiguration {

  val Empty = ExtensionConfiguration(extensions = Map.empty)

  def merge(
      current: Option[ExtensionConfiguration],
      parent: Option[ExtensionConfiguration]): Option[ExtensionConfiguration] =
    (current, parent) match {
      case (Some(c), Some(p)) => {
        val mergedExtension = p.extensions ++ c.extensions
        Some(ExtensionConfiguration(mergedExtension))
      }
      case _ => current.orElse(parent)
    }
}

final case class ExtensionProperties(
    tpe: Option[String],
    mode: OctalMode
)
