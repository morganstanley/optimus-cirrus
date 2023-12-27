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
package optimus.buildtool.format

import com.typesafe.config.Config
import optimus.buildtool.config.ConfigurableWarnings
import spray.json.JsArray
import spray.json.JsBoolean
import spray.json.JsObject
import spray.json.JsString
import spray.json.JsValue

import scala.collection.compat._
import scala.collection.immutable.Seq

/**
 * Warnings configuration
 *
 * To make a scope / bundle / etc. warnings configuration, we use this format:
 * {{{
 * all.scala.warnings {
 *   warnings-are-fatal = true (or false)
 *   overrides = [
 *     "nonfatal:optimus:17001",
 *     "nonfatal:optimus:...",
 *     "new:optimus:1234"
 *   ]
 * }
 * }}}
 * The overrides allow making some warnings fatal or non-fatal for a scope. We might extend it in the future to allow
 * selectively making scala/java warnings non-fatal, or making silenced warnings fatal, for example. The syntax is
 * chosen so as to make the override strings greppable.
 *
 * This case class is for the "raw" warning configuration that should correspond to 1-1 to the .obt file.
 */
final case class WarningsConfiguration(
    fatalWarnings: Option[Boolean],
    overrides: Vector[WarningOverride],
    inheritedOverrides: Vector[WarningOverride] = Vector.empty
) {
  import WarningsConfiguration._
  import WarningOverride._

  def fingerprint: Seq[String] = Seq(asJson.toString())

  // sorted and distinct-ed for fingerprinting
  private def overridesStrings = overrides.map(_.original).distinct.sorted

  def optimusIdsFor(tag: Tag): Set[Int] =
    (overrides.iterator ++ inheritedOverrides.iterator)
      .collect { case WarningOverride(_, `tag`, OptimusMessage(id)) => id }
      .to(Set)

  def allOverrides: Vector[WarningOverride] = overrides ++ inheritedOverrides

  // necessary for auto-formatting
  def asJson: JsObject = {
    def maybe[T <: JsValue](key: String, value: Option[T]): Seq[(String, JsValue)] = value match {
      case Some(inner) => Seq(key -> inner)
      case None        => Seq.empty
    }

    val jsonMap: Seq[(String, JsValue)] =
      maybe(fatalWarningsName, fatalWarnings.map(JsBoolean(_))) ++
        maybe(overridesName, if (overrides.nonEmpty) Some(JsArray(overridesStrings.map(JsString(_)))) else None)

    JsObject(jsonMap: _*)
  }

  def withParent(parent: WarningsConfiguration): WarningsConfiguration = copy(
    fatalWarnings = fatalWarnings.orElse(parent.fatalWarnings),
    inheritedOverrides = inheritedOverrides ++ parent.allOverrides)

  def configured: ConfigurableWarnings =
    ConfigurableWarnings(fatalWarnings.getOrElse(false), optimusIdsFor(Nonfatal), optimusIdsFor(New))
}

final case class WarningOverride(original: String, tag: WarningOverride.Tag, messageId: WarningOverride.MessageId)
object WarningOverride {
  // a very small ADT
  sealed trait Tag
  final case object New extends Tag
  final case object Nonfatal extends Tag
  private val TagR = """(new|nonfatal):(.*)""".r
  private implicit def tagFromStr(str: String): Tag = str match {
    case "new"      => New
    case "nonfatal" => Nonfatal
  }

  sealed trait MessageId
  final case class OptimusMessage(id: Int) extends MessageId
  private val OptimusMessageR = """optimus:([0-9].*)$""".r

  def safelyParse(str: String): Either[String, WarningOverride] = {
    str match {
      case TagR(tag, OptimusMessageR(id)) => Right(WarningOverride(str, tag, OptimusMessage(id.toInt)))
      case doesntParse                    => Left(doesntParse)
    }
  }
}

object WarningsConfiguration {
  val empty = WarningsConfiguration(None, Vector.empty)
  val fatalWarningsName = "warnings-are-fatal"
  val overridesName = "overrides"

  import optimus.buildtool.format.ConfigUtils.ConfOps

  def load(config: Config, origin: ObtFile): Result[WarningsConfiguration] = Result.tryWith(origin, config) {
    if (config.hasPath("warnings")) {
      val warnConf = config.getConfig("warnings")
      val (unparsed, overrides) =
        warnConf.stringListOrEmpty(overridesName).map(WarningOverride.safelyParse).partitionMap(either => either)

      Success(
        WarningsConfiguration(
          fatalWarnings = warnConf.optionalBoolean(fatalWarningsName),
          overrides = overrides.to(Vector)
        ))
        .withProblems(warnConf.checkExtraProperties(origin, Keys.warnings))
        .withProblems(unparsed.map(str =>
          origin.errorAt(warnConf.getValue(overridesName), s"Invalid warning override ${str}")))
    } else Success(WarningsConfiguration.empty)
  }
}
