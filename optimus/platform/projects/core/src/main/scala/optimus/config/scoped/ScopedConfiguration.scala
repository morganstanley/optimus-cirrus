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
package optimus.config.scoped

import optimus.config.scoped.json.JsonScopedConfiguration
import optimus.graph.NodeTaskInfo
import org.apache.commons.lang3.StringUtils
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.OptionDef
import org.kohsuke.args4j.spi.OneArgumentOptionHandler
import org.kohsuke.args4j.spi.Setter

import java.io.File
import java.io.InputStream

trait ScopedConfiguration {
  def scopedPlugins: Map[NodeTaskInfo, ScopedSchedulerPlugin]
}

object ScopedConfiguration {
  def from(i: InputStream): ScopedConfiguration = JsonScopedConfiguration.from(i)
  def from(f: File): ScopedConfiguration = JsonScopedConfiguration.from(f)
  def from(a: Array[Byte]): ScopedConfiguration = JsonScopedConfiguration.from(a)
  def from(s: String): ScopedConfiguration = JsonScopedConfiguration.from(s)

  private[scoped] def fileFromString(arg: String): Option[ScopedConfiguration] =
    if (StringUtils.isBlank(arg)) None
    else {
      if (arg.startsWith("jcp:/")) Some(from(getClass.getResourceAsStream(arg.drop(4))))
      else Some(from(new File(arg)))
    }

  class OptionOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Option[ScopedConfiguration]])
      extends OneArgumentOptionHandler[Option[ScopedConfiguration]](parser, option, setter) {
    override def parse(arg: String): Option[ScopedConfiguration] = fileFromString(arg)
  }
}
