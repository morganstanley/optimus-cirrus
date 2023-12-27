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

import optimus.utils.CollectionUtils._
import optimus.utils.app.Args4jOptionHandlers.NoArgStr
import optimus.utils.datetime.ZonedDateTimeOps
import org.apache.commons.lang3.StringUtils
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.OptionDef
import org.kohsuke.args4j.spi.OneArgumentOptionHandler
import org.kohsuke.args4j.spi.Setter

import java.nio.file.Path
import java.nio.file.Paths
import java.time.LocalDate
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import scala.util.Try

/*
 * Args4j handlers for Seq[A] kind.
 */

abstract class BaseDelimitedArgumentOptionHandler[A](
    parser: CmdLineParser,
    option: OptionDef,
    setter: Setter[Seq[A]],
    delimiter: String)
    extends OneArgumentOptionHandler[Seq[A]](parser, option, setter) {
  override def parse(arg: String): Seq[A] =
    if (arg == NoArgStr) Nil else arg.split(delimiter).iterator.map(convert).toList

  def convert(s: String): A
}

/////////// Comma-delimited types ///////////

abstract class CommaDelimitedArgumentOptionHandler[A](parser: CmdLineParser, option: OptionDef, setter: Setter[Seq[A]])
    extends BaseDelimitedArgumentOptionHandler(parser, option, setter, ",")

final class DelimitedByteOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Seq[Byte]])
    extends CommaDelimitedArgumentOptionHandler[Byte](parser, option, setter) {
  def convert(s: String): Byte = augmentString(s).toByte
}

final class DelimitedIntOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Seq[Int]])
    extends CommaDelimitedArgumentOptionHandler[Int](parser, option, setter) {
  def convert(s: String): Int = augmentString(s).toInt
}

final class DelimitedLongOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Seq[Long]])
    extends CommaDelimitedArgumentOptionHandler[Long](parser, option, setter) {
  def convert(s: String): Long = augmentString(s).toLong
}

final class DelimitedFloatOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Seq[Float]])
    extends CommaDelimitedArgumentOptionHandler[Float](parser, option, setter) {
  def convert(s: String): Float = augmentString(s).toFloat
}

final class DelimitedDoubleOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Seq[Double]])
    extends CommaDelimitedArgumentOptionHandler[Double](parser, option, setter) {
  def convert(s: String): Double = augmentString(s).toDouble
}

final class DelimitedStringOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Seq[String]])
    extends CommaDelimitedArgumentOptionHandler[String](parser, option, setter) {
  def convert(s: String): String = s
}

final class DelimitedPathOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Seq[Path]])
    extends CommaDelimitedArgumentOptionHandler[Path](parser, option, setter) {
  def convert(s: String): Path = Paths.get(s)
}

final class DelimitedTuple2StringOptionHandler(
    parser: CmdLineParser,
    option: OptionDef,
    setter: Setter[Seq[(String, String)]])
    extends CommaDelimitedArgumentOptionHandler[(String, String)](parser, option, setter) {
  def convert(s: String): (String, String) = s.split("\\+") match {
    case Array(a, b) => (a, b)
    case _ =>
      throw new IllegalArgumentException(
        "Unable to parse as Tuple2[String, String]... A string of the format: A+B is expected")
  }
}

final class DelimitedTuple2StringDoubleOptionHandler(
    parser: CmdLineParser,
    option: OptionDef,
    setter: Setter[Seq[(String, Double)]])
    extends CommaDelimitedArgumentOptionHandler[(String, Double)](parser, option, setter) {
  def convert(s: String): (String, Double) = s.split("\\+") match {
    case Array(a, b) => (a, b.toDouble)
    case _ =>
      throw new IllegalArgumentException(
        "Unable to parse as Tuple2[String, Double]... A string of the format: A+B is expected")
  }
}

final class DelimitedTuple3StringOptionHandler(
    parser: CmdLineParser,
    option: OptionDef,
    setter: Setter[Seq[(String, String, String)]])
    extends CommaDelimitedArgumentOptionHandler[(String, String, String)](parser, option, setter) {
  def convert(s: String): (String, String, String) = s.split("\\+") match {
    case Array(a, b, c) => (a, b, c)
    case _ =>
      throw new IllegalArgumentException(
        "Unable to parse as Tuple3[String, String, String]... A string of the format: A+B+C is expected")
  }
}

final class DelimitedLocalDateOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Seq[LocalDate]])
    extends CommaDelimitedArgumentOptionHandler[LocalDate](parser, option, setter) {
  def convert(s: String): LocalDate = LocalDate.parse(s)
}

final class DelimitedZonedDateTimeOptionHandler(
    parser: CmdLineParser,
    option: OptionDef,
    setter: Setter[Seq[ZonedDateTime]])
    extends CommaDelimitedArgumentOptionHandler[ZonedDateTime](parser, option, setter) {
  def convert(d: String): ZonedDateTime = ZonedDateTimeOps.parseTreatingOffsetAndZoneIdLikeJSR310(d)
}

/////////// Semicolon-delimited types ///////////

abstract class SemicolonDelimitedArgumentOptionHandler[A](
    parser: CmdLineParser,
    option: OptionDef,
    setter: Setter[Seq[A]])
    extends BaseDelimitedArgumentOptionHandler(parser, option, setter, ";")

final class SeqStringOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Seq[String]])
    extends SemicolonDelimitedArgumentOptionHandler[String](parser, option, setter) {
  def convert(s: String): String = s
}

final class SeqPathOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Seq[Path]])
    extends SemicolonDelimitedArgumentOptionHandler[Path](parser, option, setter) {
  def convert(s: String): Path = Paths.get(s)
}

final class SeqLocalDateOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Seq[LocalDate]])
    extends SemicolonDelimitedArgumentOptionHandler[LocalDate](parser, option, setter) {
  override def convert(s: String): LocalDate =
    Try(LocalDate.parse(s)).getOrElse(LocalDate.parse(s, DateTimeFormatter.BASIC_ISO_DATE))
}

final class SeqIntegerOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Seq[Integer]])
    extends SemicolonDelimitedArgumentOptionHandler[Integer](parser, option, setter) {
  override def convert(s: String): Integer =
    Try(Integer.parseInt(s))
      .getOrThrow(s"Invalid character $s in argument. Expected characters 0 to 9.")
}

abstract class SemicolonDelimitedEnumerationOptionHandler[T <: Enumeration](
    parser: CmdLineParser,
    option: OptionDef,
    setter: Setter[Seq[T#Value]],
    values: T)
    extends SemicolonDelimitedArgumentOptionHandler[T#Value](parser, option, setter) {
  def convert(s: String): T#Value = values.withName(s)
}

/////////// Other types ///////////

abstract class DelimitedArgumentOptionOptionHandler[A](
    parser: CmdLineParser,
    option: OptionDef,
    setter: Setter[Option[Seq[A]]])
    extends OneArgumentOptionHandler[Option[Seq[A]]](parser, option, setter) {
  override def parse(arg: String): Option[Seq[A]] =
    if (StringUtils.isBlank(arg) || arg == NoArgStr) None else Some(delimit(arg))

  def delimit(arg: String): Seq[A] =
    if (arg == NoArgStr) Nil else arg.split(",").toVector.map(convert)

  def convert(s: String): A
}

// this regex is using positive lookahead, tells to only split around comma , if there are no double quotes or if there are even number of double quotes ahead of it
final class DelimitedStringIgnoreDoubleQuoteOptionHandler(
    parser: CmdLineParser,
    option: OptionDef,
    setter: Setter[Seq[String]])
    extends CommaDelimitedArgumentOptionHandler[String](parser, option, setter) {
  def convert(s: String): String = s
  override def parse(arg: String): Seq[String] =
    if (arg == NoArgStr) Nil else arg.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)").toVector.map(convert)
}

final class WhitespaceDelimitedStringOptionHandler(
    parser: CmdLineParser,
    option: OptionDef,
    setter: Setter[Seq[String]])
    extends OneArgumentOptionHandler[Seq[String]](parser, option, setter) {
  override def parse(arg: String): Seq[String] = if (arg == NoArgStr) Nil else arg.split(" ").toList
}

final class DelimitedStringOptionSetHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Set[String]])
    extends OneArgumentOptionHandler[Set[String]](parser, option, setter) {
  override def parse(argument: String): Set[String] =
    if (argument == NoArgStr) Set.empty else argument.split(",").toSet
}

final class DelimitedStringDateOptionSetHandler(
    parser: CmdLineParser,
    option: OptionDef,
    setter: Setter[Set[(String, LocalDate)]])
    extends OneArgumentOptionHandler[Set[(String, LocalDate)]](parser, option, setter) {
  override def parse(argument: String): Set[(String, LocalDate)] = {
    val strSet = if (argument == NoArgStr) Set.empty else argument.split(",").toSet
    val strDateSet = strSet.map(_.split(":"))
    strDateSet.map(x => (x.head, LocalDate.parse(x.tail.head)))
  }
}
