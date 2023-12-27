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
package optimus.buildtool.compilers.zinc.reporter

import optimus.buildtool.artifacts.MaybeOptimusMessage
import optimus.buildtool.artifacts.NotAnOptimusMessage
import optimus.buildtool.artifacts.OptimusMessage

import scala.util.parsing.combinator.RegexParsers

object OptimusMessageParser extends RegexParsers {
  override val skipWhitespace = false

  // component regexen
  private val supp = """\[SUPPRESSED\]\s*""".r
  private val optimus: Parser[String ~ String ~ String] = """\s*Optimus: \(""".r ~ """\d+""".r ~ """(?s)\).*""".r

  def message: Parser[OptimusMessage] = (supp ?) ~ optimus ^^ { case supp ~ (lft ~ i ~ rght) =>
    OptimusMessage(lft + i + rght, i.toInt, supp.nonEmpty)
  }

  def parse(msg: String): MaybeOptimusMessage = parse(message, msg).getOrElse(NotAnOptimusMessage(msg))
}
