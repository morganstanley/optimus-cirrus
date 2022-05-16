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

import org.apache.commons.lang3.StringUtils
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.OptionDef
import org.kohsuke.args4j.spi.OneArgumentOptionHandler
import org.kohsuke.args4j.spi.Setter

object Args4JOptionHandlers {

  private val NoArgStr = "NO_ARG"

  abstract class DelimitedArgumentOptionHandler[A](
      parser: CmdLineParser,
      option: OptionDef,
      setter: Setter[collection.Seq[A]])
      extends OneArgumentOptionHandler[collection.Seq[A]](parser, option, setter) {
    override def parse(arg: String): collection.Seq[A] = arg.split(",").iterator.map(convert(_)).toSeq

    def convert(s: String): A
  }

  class DelimitedStringOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[collection.Seq[String]])
      extends DelimitedArgumentOptionHandler[String](parser, option, setter) {
    def convert(s: String): String = s
  }

  abstract class GroupedDelimitedArgumentOptionHandler[A](
      parser: CmdLineParser,
      option: OptionDef,
      setter: Setter[collection.Seq[A]])
      extends OneArgumentOptionHandler[collection.Seq[A]](parser, option, setter) {
    override def parse(arg: String): collection.Seq[A] = arg.split(";").iterator.map(convert(_)).toSeq

    def convert(s: String): A
  }

  class GroupedDelimitedStringOptionHandler(
      parser: CmdLineParser,
      option: OptionDef,
      setter: Setter[collection.Seq[collection.Seq[String]]])
      extends GroupedDelimitedArgumentOptionHandler[collection.Seq[String]](parser, option, setter) {
    override def convert(s: String): collection.Seq[String] = s.split(",")
  }

  abstract class OptionOptionHandler[A](parser: CmdLineParser, option: OptionDef, setter: Setter[Option[A]])
      extends OneArgumentOptionHandler[Option[A]](parser, option, setter) {
    override def parse(arg: String): Option[A] =
      if (StringUtils.isBlank(arg) || arg == NoArgStr) None else Some(convert(arg))
    def convert(s: String): A
  }

  class StringHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Option[String]])
      extends OptionOptionHandler[String](parser, option, setter) {
    override def convert(s: String): String = s
  }

  class PositiveIntOptionOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Option[Int]])
      extends OptionOptionHandler[Int](parser, option, setter) {
    override def convert(s: String): Int = {
      val value = s.toInt
      if (value <= 0) {
        // this exception will be properly handled by args4j
        throw new NumberFormatException(s"Expected positive integer while there was $value passed.")
      }
      value
    }
  }

  class TupleStringOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[(String, String)])
      extends OneArgumentOptionHandler[(String, String)](parser, option, setter) {

    override def parse(argument: String): (String, String) = {
      val Array(first, second) = argument.split(",")
      (first, second)
    }
  }
}
