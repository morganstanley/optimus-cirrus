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
package optimus.utils

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.OptionDef
import org.kohsuke.args4j.spi.Messages
import org.kohsuke.args4j.spi.PathOptionHandler
import org.kohsuke.args4j.spi.Setter

private[optimus] class PathHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[_ >: Path])
    extends PathOptionHandler(parser, option, setter) {
  override def parse(argument: String): Path = {
    val res =
      try {
        Paths.get(argument)
      } catch {
        case _: Throwable => throw new CmdLineException(owner, Messages.ILLEGAL_PATH, argument)
      }
    if (!Files.exists(res))
      throw new CmdLineException(parser, new IllegalArgumentException(s"$argument doesn't exist"))
    if (!Files.isReadable(res))
      throw new CmdLineException(parser, new IllegalArgumentException(s"$argument is not readable"))
    if (!Files.isWritable(res))
      throw new CmdLineException(parser, new IllegalArgumentException(s"$argument is not writable"))
    res
  }
}
