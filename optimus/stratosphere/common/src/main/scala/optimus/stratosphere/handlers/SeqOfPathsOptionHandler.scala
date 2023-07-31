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
package optimus.stratosphere.handlers

import java.nio.file.Path
import java.nio.file.Paths

import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.OptionDef
import org.kohsuke.args4j.spi.OneArgumentOptionHandler
import org.kohsuke.args4j.spi.Setter
import scala.collection.compat._
import scala.collection.immutable.Seq

class SeqOfPathsOptionHandler(parser: CmdLineParser, optionDef: OptionDef, setter: Setter[Seq[Path]])
    extends OneArgumentOptionHandler[Seq[Path]](parser, optionDef, setter) {

  override def parse(arg: String): Seq[Path] = {
    arg.trim
      .split(",")
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(x => Paths.get(x))
      .to(Seq)
  }
}
