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

import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.OptionDef
import org.kohsuke.args4j.spi.ExplicitBooleanOptionHandler
import org.kohsuke.args4j.spi.OneArgumentOptionHandler
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

trait Args4jOptionHandlers {
  // For backwards compatibility
  type BooleanNamedOptionHandler = ExplicitBooleanOptionHandler
  type OptionHandler[A] = OptionOptionHandler[A]
  type ArgumentOptionOptionHandler[A] = OptionOptionHandler[A]
  type SeqArgumentOptionHandler[A] = SemicolonDelimitedArgumentOptionHandler[A]
  type DelimitedArgumentOptionHandler[A] = CommaDelimitedArgumentOptionHandler[A]
  type StringHandler = StringOptionOptionHandler
  type IntOptionOptionHandler = IntegerOptionOptionHandler

  // Forwarders
  type BaseDelimitedArgumentOptionHandler[A] = optimus.utils.app.BaseDelimitedArgumentOptionHandler[A]
  type BooleanOptionOptionHandler = optimus.utils.app.BooleanOptionOptionHandler
  type ByteOptionOptionHandler = optimus.utils.app.ByteOptionOptionHandler
  type CommaDelimitedArgumentOptionHandler[A] = optimus.utils.app.CommaDelimitedArgumentOptionHandler[A]
  type DelimitedArgumentOptionOptionHandler[A] = optimus.utils.app.DelimitedArgumentOptionOptionHandler[A]
  type DelimitedByteOptionHandler = optimus.utils.app.DelimitedByteOptionHandler
  type DelimitedDoubleOptionHandler = optimus.utils.app.DelimitedDoubleOptionHandler
  type DelimitedEnumerationOptionHandler[T <: Enumeration] = optimus.utils.app.DelimitedEnumerationOptionHandler[T]
  type DelimitedFloatOptionHandler = optimus.utils.app.DelimitedFloatOptionHandler
  type DelimitedIntOptionHandler = optimus.utils.app.DelimitedIntOptionHandler
  type DelimitedLocalDateOptionHandler = optimus.utils.app.DelimitedLocalDateOptionHandler
  type DelimitedLongOptionHandler = optimus.utils.app.DelimitedLongOptionHandler
  type DelimitedPathOptionHandler = optimus.utils.app.DelimitedPathOptionHandler
  type DelimitedStringDateOptionSetHandler = optimus.utils.app.DelimitedStringDateOptionSetHandler
  type DelimitedStringIgnoreDoubleQuoteOptionHandler = optimus.utils.app.DelimitedStringIgnoreDoubleQuoteOptionHandler
  type DelimitedStringOptionHandler = optimus.utils.app.DelimitedStringOptionHandler
  type DelimitedStringOptionSetHandler = optimus.utils.app.DelimitedStringOptionSetHandler
  type DelimitedTuple2StringDoubleOptionHandler = optimus.utils.app.DelimitedTuple2StringDoubleOptionHandler
  type DelimitedTuple2StringOptionHandler = optimus.utils.app.DelimitedTuple2StringOptionHandler
  type DelimitedTuple3StringOptionHandler = optimus.utils.app.DelimitedTuple3StringOptionHandler
  type DelimitedZDTOptionHandler = optimus.utils.app.DelimitedZDTOptionHandler
  type DelimitedZonedDateTimeOptionHandler = optimus.utils.app.DelimitedZonedDateTimeOptionHandler
  type DoubleOptionOptionHandler = optimus.utils.app.DoubleOptionOptionHandler
  type DurationFromPeriodOptionHandler = optimus.utils.app.DurationFromPeriodOptionHandler
  type DurationFromPeriodOptionOptionHandler = optimus.utils.app.DurationFromPeriodOptionOptionHandler
  type DurationOptionHandler = optimus.utils.app.DurationOptionHandler
  type DurationOptionOptionHandler = optimus.utils.app.DurationOptionOptionHandler
  type EnumerationOptionOptionHandler[T <: Enumeration] = optimus.utils.app.EnumerationOptionOptionHandler[T]
  type ExistingPathOptionHandler = optimus.utils.app.ExistingPathOptionHandler
  type FileOptionOptionHandler = optimus.utils.app.FileOptionOptionHandler
  type FilePermissionOptionHandler = optimus.utils.app.FilePermissionOptionHandler
  type GroupedDelimitedArgumentOptionHandler[A] = optimus.utils.app.GroupedDelimitedArgumentOptionHandler[A]
  type GroupedDelimitedStringOptionHandler = optimus.utils.app.GroupedDelimitedStringOptionHandler
  type InstantHandler = optimus.utils.app.InstantHandler
  type IntegerOptionOptionHandler = optimus.utils.app.IntegerOptionOptionHandler
  type LocalDateOptionHandler = optimus.utils.app.LocalDateOptionHandler
  type LocalDateOptionOptionHandler = optimus.utils.app.LocalDateOptionOptionHandler
  type LocalDateOrTodayOptionOptionHandler = optimus.utils.app.LocalDateOrTodayOptionOptionHandler
  type LocalDateRangeOptionOptionHandler = optimus.utils.app.LocalDateRangeOptionOptionHandler
  type LocalDateTimeOptionHandler = optimus.utils.app.LocalDateTimeOptionHandler
  type LocalDateTimeOptionOptionHandler = optimus.utils.app.LocalDateTimeOptionOptionHandler
  type LocalDateWithDaysAndOffsetOptionOptionHandler = optimus.utils.app.LocalDateWithDaysAndOffsetOptionOptionHandler
  type LocalTimeOptionHandler = optimus.utils.app.LocalTimeOptionHandler
  type LocalTimeOptionOptionHandler = optimus.utils.app.LocalTimeOptionOptionHandler
  type LongOptionOptionHandler = optimus.utils.app.LongOptionOptionHandler
  type MapOptionHandler[KeyType, ValueType] = optimus.utils.app.MapOptionHandler[KeyType, ValueType]
  type OptionOptionHandler[A] = optimus.utils.app.OptionOptionHandler[A]
  type PathOptionOptionHandler = optimus.utils.app.PathOptionOptionHandler
  type PeriodOptionHandler = optimus.utils.app.PeriodOptionHandler
  type PeriodOptionOptionHandler = optimus.utils.app.PeriodOptionOptionHandler
  type PositiveIntOptionOptionHandler = optimus.utils.app.PositiveIntOptionOptionHandler
  type ScalaEnumerationOptionHandler = optimus.utils.app.ScalaEnumerationOptionHandler
  type SemicolonDelimitedArgumentOptionHandler[A] = optimus.utils.app.SemicolonDelimitedArgumentOptionHandler[A]
  type SemicolonDelimitedEnumerationOptionHandler[T <: Enumeration] =
    optimus.utils.app.SemicolonDelimitedEnumerationOptionHandler[T]
  type SeqIntegerOptionHandler = optimus.utils.app.SeqIntegerOptionHandler
  type SeqLocalDateOptionHandler = optimus.utils.app.SeqLocalDateOptionHandler
  type SeqPathOptionHandler = optimus.utils.app.SeqPathOptionHandler
  type SeqStringOptionHandler = optimus.utils.app.SeqStringOptionHandler
  type SeqStringOptionOptionHandler = optimus.utils.app.SeqStringOptionOptionHandler
  type SimpleInstantOptionHandler = optimus.utils.app.SimpleInstantOptionHandler
  type StringOptionOptionHandler = optimus.utils.app.StringOptionOptionHandler
  type TupleStringOptionHandler = optimus.utils.app.TupleStringOptionHandler
  type TypedEnumerationOptionHandler[E <: Enumeration] = optimus.utils.app.TypedEnumerationOptionHandler[E]
  type URIOptionOptionHandler = optimus.utils.app.URIOptionOptionHandler
  type WhitespaceDelimitedStringOptionHandler = optimus.utils.app.WhitespaceDelimitedStringOptionHandler
  type ZoneIdOptionHandler = optimus.utils.app.ZoneIdOptionHandler
  type ZonedDateTimeOptionHandler = optimus.utils.app.ZonedDateTimeOptionHandler
  type ZonedDateTimeOptionOptionHandler = optimus.utils.app.ZonedDateTimeOptionOptionHandler
}

abstract class GroupedDelimitedArgumentOptionHandler[A](
    parser: CmdLineParser,
    option: OptionDef,
    setter: Setter[collection.Seq[A]])
    extends OneArgumentOptionHandler[collection.Seq[A]](parser, option, setter) {
  override def parse(arg: String): collection.Seq[A] = arg.split(";").iterator.map(convert).toSeq

  def convert(s: String): A
}

final class GroupedDelimitedStringOptionHandler(
    parser: CmdLineParser,
    option: OptionDef,
    setter: Setter[collection.Seq[collection.Seq[String]]])
    extends GroupedDelimitedArgumentOptionHandler[collection.Seq[String]](parser, option, setter) {
  override def convert(s: String): collection.Seq[String] = s.split(",")
}

final class TupleStringOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[(String, String)])
    extends OneArgumentOptionHandler[(String, String)](parser, option, setter) {

  override def parse(argument: String): (String, String) = {
    val Array(first, second) = argument.split(",")
    (first, second)
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
    arg
      .split(",")
      .map(keyValuePair =>
        keyValuePair.split(":") match {
          case Array(key: String, value: String) =>
            (convertKey(key), convertValue(value))
          case x =>
            throw new IllegalArgumentException(s"Unable to parse \'${keyValuePair}\' as a key-value pair")
        })
      .toMap
  }
  def convertKey(s: String): KeyType
  def convertValue(s: String): ValueType
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
