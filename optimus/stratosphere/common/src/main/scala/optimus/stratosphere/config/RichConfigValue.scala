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
package optimus.stratosphere.config

import com.typesafe.config.Config
import optimus.stratosphere.config.ConfigSourceLocation.ConfigSourceLocationVal
import optimus.stratosphere.config.ConfigSourceLocation._
import optimus.stratosphere.logger.Logger

import java.time.LocalDate
import scala.jdk.CollectionConverters._
import scala.util.Try

final case class RichConfigValue[+UnderlyingType](
    key: String,
    value: UnderlyingType,
    location: ConfigSourceLocationVal,
    displayName: Option[String],
    environmentKeyName: Option[String] = None,
    releaseDate: Option[LocalDate] = None) {
  def log(implicit logger: Logger, consoleColors: ConsoleColors): Unit =
    logger.info(location.short(s"$key = $value"))
}

object RichConfigValue extends RichConfigValueImplicits {

  final case class ConfigEntry[UnderlyingType](group: String, value: RichConfigValue[UnderlyingType])

  implicit class ExtractorToRichConfigValueHelper[Type: Extractor](value: Type) {

    def enrich(key: String, displayName: Option[String], environmentKeyName: String)(implicit
        stratoWorkspace: StratoWorkspaceCommon): RichConfigValue[Type] = {
      sys.env
        .get(environmentKeyName)
        .fold(baseEnrich(key, value, displayName, stratoWorkspace))(_ =>
          RichConfigValue(key, value, EnvironmentOverride, displayName, environmentKeyName = Some(environmentKeyName)))
    }

    def enrich(key: String, displayName: Option[String])(implicit
        stratoWorkspace: StratoWorkspaceCommon): RichConfigValue[Type] = {
      baseEnrich(key, value, displayName, stratoWorkspace)
    }
  }

  implicit class RichConfigValueStratoWorkspaceCommon(stratoWorkspace: StratoWorkspaceCommon) {

    def configSourceList: Seq[ConfigEntry[String]] = {
      def firstWordOfKey(entry: java.util.Map.Entry[String, _]): String = entry.getKey.takeWhile(_.isLower)
      val entriesGroups = stratoWorkspace.config.entrySet().asScala.groupBy(firstWordOfKey)
      val (singleElementGroups, multiElementGroups) = entriesGroups.partition { case (_, entries) =>
        entries.size == 1
      }
      // Put all single-element groups into one
      val allProperties = multiElementGroups + ("other" -> singleElementGroups.values.flatten)
      allProperties
        .to(Seq)
        .flatMap { case (group, entriesGroup) =>
          val sorted = entriesGroup.to(Seq).sortBy(_.getKey)
          sorted.map { entry =>
            val key = entry.getKey
            val value = entry.getValue.unwrapped().toString
            ConfigEntry(group, baseEnrich(key, value, None, stratoWorkspace))
          }
        }
    }
  }

  private def baseEnrich[Type: Extractor](
      property: String,
      value: Type,
      maybeDisplayName: Option[String],
      stratoWorkspace: StratoWorkspaceCommon): RichConfigValue[Type] = {
    val isGlobal = isInConfig(stratoWorkspace.globalUserConfig, property, value)
    val isLocal = isInConfig(stratoWorkspace.localUserConfig, property, value)
    val isDefault = isInConfig(stratoWorkspace.defaultConfig, property, value)
    if (isGlobal)
      RichConfigValue(property, value, Global, maybeDisplayName)
    else if (isLocal)
      RichConfigValue(property, value, Local, maybeDisplayName)
    else if (isDefault)
      RichConfigValue(property, value, Default, maybeDisplayName)
    else
      RichConfigValue(property, value, Workspace, maybeDisplayName)
  }

  private def isInConfig[Type: Extractor](config: Config, key: String, value: Type): Boolean = {
    // using hasPathOrNull because config may contain unresolved substitutions, hasPathOrNull
    // only checks if the key exists, hasPath also checks if the value != null
    config.hasPathOrNull(key) && Try(implicitly[Extractor[Type]].extract(config, key) == value).getOrElse(false)
  }
}
