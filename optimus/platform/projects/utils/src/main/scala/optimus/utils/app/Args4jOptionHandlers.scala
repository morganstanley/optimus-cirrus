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

import optimus.utils.MemSize
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.OptionDef
import org.kohsuke.args4j.spi.OneArgumentOptionHandler
import org.kohsuke.args4j.spi.OptionHandler
import org.kohsuke.args4j.spi.Parameters
import org.kohsuke.args4j.spi.Setter

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.attribute.PosixFilePermission
import java.nio.file.attribute.PosixFilePermissions
import java.time._
import java.time.format.DateTimeFormatter
import scala.jdk.CollectionConverters._
import scala.util.Success
import scala.util.Try

object Args4jOptionHandlers {
  val NoArgStr = "NO_ARG"

  /** 2 formats supported: yyyy-MM-dd yyyy-MM-ddTHH:mm:ss */
  def stringToInstant(s: String): Instant =
    Instant.ofEpochMilli(
      Instant
        .parse(Try(DateTimeFormatter.ISO_LOCAL_DATE.parse(s)) match {
          case Success(_) => s"${s}T00:00:00Z"
          case _          => s"${s}Z"
        })
        .toEpochMilli)
}

abstract class GroupedDelimitedArgumentOptionHandler[A](
    parser: CmdLineParser,
    option: OptionDef,
    setter: Setter[Seq[A]])
    extends OneArgumentOptionHandler[Seq[A]](parser, option, setter) {
  override def parse(arg: String): Seq[A] = arg.split(";").iterator.map(convert).toSeq

  def convert(s: String): A
}

final class GroupedDelimitedStringOptionHandler(
    parser: CmdLineParser,
    option: OptionDef,
    setter: Setter[Seq[Seq[String]]])
    extends GroupedDelimitedArgumentOptionHandler[Seq[String]](parser, option, setter) {
  override def convert(s: String): Seq[String] = s.split(",")
}

final class TupleStringOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[(String, String)])
    extends OneArgumentOptionHandler[(String, String)](parser, option, setter) {

  override def parse(argument: String): (String, String) = {
    val Array(first, second) = argument.split(",")
    (first, second)
  }
}

final class TupleDoubleOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[(Double, Double)])
    extends OneArgumentOptionHandler[(Double, Double)](parser, option, setter) {

  override def parse(argument: String): (Double, Double) = {
    val Array(first, second) = argument.split(",")
    (convert(first), convert(second))
  }

  def convert(s: String): Double = {
    s.toUpperCase match {
      case "INF"  => Double.PositiveInfinity
      case "-INF" => Double.NegativeInfinity
      case "MAX"  => Double.MaxValue
      case _      => augmentString(s).toDouble
    }
  }
}

final class ExistingPathOptionHandler(parser: CmdLineParser, optionDef: OptionDef, setter: Setter[Path])
    extends OneArgumentOptionHandler[Path](parser, optionDef, setter) {

  override def parse(arg: String): Path = {
    val path = Paths.get(arg)
    if (!Files.exists(path))
      throw new CmdLineException(parser, s"$option path: `$path` does not exist!", null)
    path
  }
}

object FlexibleBooleanOptionHandler {
  private val acceptableValues = Map(
    "true" -> true,
    "on" -> true,
    "yes" -> true,
    "1" -> true,
    "false" -> false,
    "off" -> false,
    "no" -> false,
    "0" -> false
  )
}
// Similar to org.kohsuke.args4j.spi.ExplicitBooleanOptionHandler, but assume any parameter not matching one of the
// acceptable values is part of the next option/argument.
class FlexibleBooleanOptionHandler(
    parser: CmdLineParser,
    option: OptionDef,
    setter: Setter[Boolean]
) extends OptionHandler[Boolean](parser, option, setter) {
  import FlexibleBooleanOptionHandler.acceptableValues

  override def parseArguments(params: Parameters): Int = {
    val arg = if (params.size == 0) None else acceptableValues.get(params.getParameter(0).toLowerCase)

    setter.addValue(arg.getOrElse(true))
    arg.size
  }

  override def getDefaultMetaVariable: String = "VALUE"
}

final class FilePermissionOptionHandler(
    parser: CmdLineParser,
    option: OptionDef,
    setter: Setter[Set[PosixFilePermission]])
    extends OneArgumentOptionHandler[Set[PosixFilePermission]](parser, option, setter) {
  override def parse(argument: String): Set[PosixFilePermission] =
    PosixFilePermissions.fromString(argument).asScala.toSet
}

abstract class MapOptionHandler[KeyType, ValueType](
    parser: CmdLineParser,
    option: OptionDef,
    setter: Setter[Map[KeyType, ValueType]])
    extends OneArgumentOptionHandler[Map[KeyType, ValueType]](parser, option, setter) {
  override def parse(arg: String): Map[KeyType, ValueType] = {
    if (arg == "NO_ARG") Map.empty[KeyType, ValueType]
    else
      arg
        .split(delimiter)
        .map(keyValuePair =>
          keyValuePair.split(keyValueDelimiter) match {
            case Array(key: String, value: String) =>
              (convertKey(key), convertValue(value))
            case _ =>
              throw new IllegalArgumentException(s"Unable to parse \'$keyValuePair\' as a key-value pair")
          })
        .toMap
  }
  def convertKey(s: String): KeyType
  def convertValue(s: String): ValueType
  def delimiter: String = ","
  def keyValueDelimiter: String = ":"
}

final class MemSizeOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[MemSize])
    extends OneArgumentOptionHandler[MemSize](parser, option, setter) {
  override def parse(arg: String): MemSize = MemSize.of(arg)
}

class PositiveIntOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Int])
    extends OneArgumentOptionHandler[Int](parser, option, setter) {
  override def parse(s: String): Int = {
    val value = s.toInt
    if (value <= 0) {
      throw new CmdLineException(parser, s"Expected positive integer, but found: $value", null)
    }
    value
  }
}

abstract class TypedSetOptionHandler[T](parser: CmdLineParser, option: OptionDef, setter: Setter[Set[T]])
    extends OneArgumentOptionHandler[Set[T]](parser, option, setter) {

  override def parse(arg: String): Set[T] = arg.split(delimiter).iterator.map(convert).toSet
  def convert(s: String): T
  def delimiter: String = ","
}

abstract class TypedEnumerationOptionHandler[E <: Enumeration](e: E)(
    parser: CmdLineParser,
    opt: OptionDef,
    setter: Setter[E#Value])
    extends OneArgumentOptionHandler[E#Value](parser, opt, setter) {
  override def parse(argument: String): E#Value = e.withName(argument)
}

abstract class ScalaEnumerationOptionHandler(
    obj: Enumeration,
    parser: CmdLineParser,
    option: OptionDef,
    setter: Setter[Any])
    extends OneArgumentOptionHandler[Any](parser, option, setter) {

  override def parse(argument: String): Any = obj.withName(argument)
}

abstract class EnumerationOptionOptionHandler[T <: Enumeration](
    parser: CmdLineParser,
    option: OptionDef,
    setter: Setter[Option[T#Value]],
    values: T)
    extends OptionOptionHandler[T#Value](parser, option, setter) {
  override def convert(arg: String): T#Value = values.withName(arg)
}

abstract class DelimitedEnumerationOptionHandler[T <: Enumeration](
    parser: CmdLineParser,
    option: OptionDef,
    setter: Setter[Seq[T#Value]],
    values: T)
    extends CommaDelimitedArgumentOptionHandler[T#Value](parser, option, setter) {
  def convert(s: String): T#Value = values.withName(s)
}

final class RangeOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Option[Range]])
    extends OneArgumentOptionHandler[Option[Range]](parser, option, setter) {
  override def parse(arg: String): Option[Range] = Option(arg).map { range =>
    val Array(begin, end) = arg.split("-")
    Range(begin.toInt, end.toInt)
  }
}
