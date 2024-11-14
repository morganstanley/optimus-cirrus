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
import optimus.stratosphere.utils.ConfigUtils

final case class PluginBundle(
    name: String,
    enabled: Boolean,
    names: Seq[String],
    dirs: Seq[String]
)

object PluginBundle {
  def apply(config: Config): PluginBundle = {
    val name: String = config.getString("name")
    val enabled: Boolean = config.getBoolean("enabled")
    val entries: Seq[Map[String, String]] = ConfigUtils.readProperty(config)("entries")
    val names = entries.map(_.apply("name"))
    val dirs = entries.map(_.apply("path"))

    PluginBundle(
      name,
      enabled,
      names,
      dirs
    )
  }
}
