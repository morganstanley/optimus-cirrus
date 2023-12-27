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
import com.typesafe.config.ConfigObject
import optimus.buildtool.artifacts.Severity
import optimus.buildtool.config.CodeFlaggingRule
import optimus.buildtool.config.Pattern
import optimus.buildtool.config.RegexConfiguration
import optimus.buildtool.format.ConfigUtils._

import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq
import scala.collection.compat._

object RegexConfigurationCompiler {
  private val Rules = "rules"
  private val SeverityKey = "severity-level"
  private val PatternsKey = "patterns"

  def load(config: Config, origin: ObtFile): Result[Option[RegexConfiguration]] =
    Result
      .tryWith(origin, config) {
        Result.optional(config.hasPath(Rules)) {
          Result
            .sequence {
              config
                .configs(Rules)
                .map { cfg =>
                  val rule = loadRule(cfg, origin)
                  rule
                }
            }
            .map(rules => RegexConfiguration(rules))
        }
      }

  private def loadRule(config: Config, origin: ObtFile): Result[CodeFlaggingRule] =
    Result
      .tryWith(origin, config) {
        val regexes = loadPatterns(config, origin)
        val severity = loadSeverity(config, origin)

        Result.withProblemsFrom(
          CodeFlaggingRule(
            key = config.getString("key"),
            title = config.getString("title"),
            description = config.getString("description"),
            filePatterns = config.getStringList("file-patterns").asScala.to(Seq),
            severityLevel = severity.getOrElse(Severity.Error),
            regexes = regexes.getOrElse(Seq.empty),
            isNew = config.booleanOrDefault("new", default = false)
          )
        )(regexes, severity)
      }
      .withProblems(config.checkExtraProperties(origin, Keys.codeFlaggingRule))

  private def loadSeverity(config: Config, origin: ObtFile): Result[Severity] = {
    val severity = config.getString(SeverityKey)
    Severity.safelyParse(severity) match {
      case Some(value) => Success(value)
      case None =>
        origin.failure(
          config.getValue(SeverityKey),
          s"Invalid severity '$severity'. Possible values are: ${Severity.values.map(_.toString.toLowerCase).mkString(", ")}")
    }
  }

  private def loadPatterns(config: Config, origin: ObtFile): Result[Seq[Pattern]] =
    Result
      .sequence {
        config.values(PatternsKey).map {
          case co: ConfigObject => loadPattern(co.toConfig, origin)
          case other            => origin.failure(other, "pattern is not an object")
        }
      }

  private def loadPattern(config: Config, origin: ObtFile): Result[Pattern] = {
    Result
      .tryWith(origin, config) {
        Success {
          Pattern(config.getString("pattern"), config.booleanOrDefault("exclude", default = false))
        }
      }
      .withProblems(config.checkExtraProperties(origin, Keys.pattern))
  }
}
