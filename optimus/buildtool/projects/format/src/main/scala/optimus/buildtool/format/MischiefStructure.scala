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
import optimus.buildtool.format.ConfigUtils._
import optimus.buildtool.config.ScopeId

import scala.collection.compat._
import scala.collection.immutable.Seq
import scala.util.matching.Regex
import scala.jdk.CollectionConverters._

// We *could* have mischief arguments for multiple scopes. We don't currently do it.
final case class MischiefStructure(
    tricks: Map[ScopeId, MischiefArgs]
)

final case class MischiefInvalidator(regexs: Seq[Regex]) {

  /**
   * Check if a particular path should be invalidated.
   */
  def invalidates(path: String): Boolean =
    regexs.exists(_.findFirstIn(path).nonEmpty)
}

final case class MischiefMessage(
    msg: String,
    severity: String,
    file: Option[String],
    line: Option[Int]
)

final case class MischiefArgs(
    invalidateOnly: Option[MischiefInvalidator],
    extraScalacArgs: Seq[String],
    extraScalacMessages: Seq[MischiefMessage]
)

object MischiefStructure {
  private val origin = MischiefConfig
  val extraMsgs = "extraScalacMessages"
  val invalidateOnly = "invalidateOnly"
  val extraScalacArgs = "extraScalacArgs"

  val Empty: MischiefStructure = MischiefStructure(Map.empty)

  def parseExtraMessages(conf: Config): Seq[MischiefMessage] =
    if (conf.hasPath(extraMsgs)) {
      conf
        .getConfigList(extraMsgs)
        .asScala
        .map { c =>
          MischiefMessage(
            msg = c.stringOrDefault("msg", ""),
            severity = c.stringOrDefault("severity", default = "INFO"),
            file = c.optionalString("file"),
            line = c.optionalString("line").map(_.toInt)
          )
        }
        .to(Seq)
    } else Nil

  private def parseArgs(conf: Config, scope: ScopeId): MischiefStructure =
    MischiefStructure(
      tricks = Map(
        scope ->
          MischiefArgs(
            invalidateOnly =
              conf.optionalStringList(invalidateOnly).map(strings => MischiefInvalidator(strings.map(_.r))),
            extraScalacArgs = conf.stringListOrEmpty(extraScalacArgs),
            extraScalacMessages = parseExtraMessages(conf)
          )))

  def load(loader: ObtFile.Loader): Result[MischiefStructure] =
    loader(origin).flatMap(load)

  def load(config: Config): Result[MischiefStructure] = {
    Result
      .tryWith(origin, config) {
        if (config.hasPath("mischief.scope")) {
          val mischiefConf = config.getConfig("mischief")
          val scopeStr = mischiefConf.getString("scope")
          val scope = ScopeId.parseOpt(scopeStr).map(Success(_)).getOrElse {
            origin.errorAt(config.getValue("mischief.scope"), s"Not a valid scope id: ${scopeStr}").failure
          }

          scope
            .map(parseArgs(mischiefConf, _))
            .withProblems(mischiefConf.checkExtraProperties(origin, Keys.mischief))
        } else Success(Empty)
      }
  }

  // These functions check for the existence of a mischief file, and whether the mischief file has a top level
  // active boolean key.
  //
  // If the mischief file exists and DOES NOT have active = false in it, we return true.
  def checkIfActive(loader: ObtFile.Loader): Result[Option[Boolean]] =
    if (!loader.exists(origin)) Success(None) // tocttou because of the design of Loader
    else
      loader(origin).flatMap { config =>
        Result
          .tryWith(origin, config) {
            Success(
              Some(
                config
                  .optionalBoolean("active")
                  .getOrElse(true))
            )
          }
      }

}
