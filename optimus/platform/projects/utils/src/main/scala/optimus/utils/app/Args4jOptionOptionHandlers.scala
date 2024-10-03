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
package optimus.utils.app

import optimus.utils.app.Args4jOptionHandlers._
import org.apache.commons.lang3.StringUtils
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.OptionDef
import org.kohsuke.args4j.spi.OneArgumentOptionHandler
import org.kohsuke.args4j.spi.Setter

import java.io.File
import java.net.URI
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration
import java.time.Instant

/*
 * Args4j handlers for Option[A] kind.
 */

abstract class OptionOptionHandler[A](parser: CmdLineParser, option: OptionDef, setter: Setter[Option[A]])
    extends OneArgumentOptionHandler[Option[A]](parser, option, setter) {
  override def parse(arg: String): Option[A] =
    if (StringUtils.isBlank(arg) || arg == NoArgStr) None else Some(convert(arg))
  def convert(s: String): A
}

final class StringOptionOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Option[String]])
    extends OptionOptionHandler[String](parser, option, setter) {
  override def convert(s: String): String = s
}

final class PositiveIntOptionOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Option[Int]])
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

final class IntegerOptionOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Option[Int]])
    extends OptionOptionHandler[Int](parser, option, setter) {
  override def convert(s: String): Int = s.toInt
}

final class DoubleOptionOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Option[Double]])
    extends OptionOptionHandler[Double](parser, option, setter) {
  override def convert(s: String): Double = s.toDouble
}

final class ByteOptionOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Option[Byte]])
    extends OptionOptionHandler[Byte](parser, option, setter) {
  def convert(s: String): Byte = s.toByte
}

final class LongOptionOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Option[Long]])
    extends OptionOptionHandler[Long](parser, option, setter) {
  def convert(s: String): Long = s.toLong
}

final class PathOptionOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Option[Path]])
    extends OptionOptionHandler[Path](parser, option, setter) {
  def convert(s: String): Path = Paths.get(s)
}

final class SimpleInstantOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Option[Instant]])
    extends OptionOptionHandler[Instant](parser, option, setter) {
  override def convert(s: String): Instant = stringToInstant(s)
}

final class IsoInstantOptionOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Option[Instant]])
    extends OptionOptionHandler[Instant](parser, option, setter) {
  override def convert(s: String): Instant = Instant.parse(s)
}

final class BooleanOptionOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Option[Boolean]])
    extends OptionOptionHandler[Boolean](parser, option, setter) {
  override def convert(s: String): Boolean = s.toBoolean
}

final class URIOptionOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Option[URI]])
    extends OptionOptionHandler[URI](parser, option, setter) {
  def convert(s: String): URI = new URI(s)
}

final class FileOptionOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Option[File]])
    extends OptionOptionHandler[File](parser, option, setter) {
  def convert(s: String): File = new File(s)
}

final class DurationOptionOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Option[Duration]])
    extends OptionOptionHandler[Duration](parser, option, setter) {
  override def convert(arg: String): Duration = Duration.parse(arg)
}

final class SeqStringOptionOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Option[Seq[String]]])
    extends OptionOptionHandler[Seq[String]](parser, option, setter) {
  def convert(s: String): Seq[String] = s.split(",").toList
}
