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

import ConfigUtils._
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import java.util.{Map => JMap}
import scala.collection.compat._
import scala.collection.immutable.Seq
import scala.util.control.NonFatal

final case class ProjectProperties(config: Config) {
  def includeConfig(newConfig: Config): ProjectProperties = ProjectProperties(config.withFallback(newConfig))
}

object ProjectProperties {
  val empty: ProjectProperties = ProjectProperties(ConfigFactory.empty)

  private def combineConfig(rawConf: Config, workspaceConfig: Config, parsedProperties: Config): Config = {
    rawConf
      .withValue("workspace", workspaceConfig.root())
      .withValue("properties", parsedProperties.root())
  }

  private val propertyBasedKey = "propertyBased"

  private def addExternalProperties(fullConfig: Config, properties: JMap[String, String]): Result[Config] =
    if (fullConfig.hasPath(propertyBasedKey)) {
      val allPropertiesConfig = fullConfig.getObject(propertyBasedKey).toConfig

      def mergeConfigs(keys: Iterable[String]): Config = {
        val keysToInclude = keys.filter(properties.containsKey)
        val finalPropertiesConfig = keysToInclude.foldLeft(ConfigFactory.empty()) { (result, key) =>
          result.withFallback(allPropertiesConfig.getObject(key))
        }
        finalPropertiesConfig.withFallback(fullConfig.withoutPath(propertyBasedKey)).resolve()
      }

      allPropertiesConfig.keys(WorkspaceConfig).map(mergeConfigs)
    } else Success(fullConfig.resolve())

  def load(
      loader: ObtFile.Loader,
      workspaceConfig: Config,
      properties: JMap[String, String]): Result[ProjectProperties] =
    WorkspaceConfig.tryWith {
      for {
        rawConf <- loader(WorkspaceConfig)
        parsedProperties <- parsePropertiesMap(properties)
        fullConfig = combineConfig(rawConf, workspaceConfig, parsedProperties)
        completeConfig <- addExternalProperties(fullConfig, properties)
      } yield ProjectProperties(completeConfig)
    }

  private def parsePropertiesMap(properties: JMap[String, String]): Result[Config] = {
    try {
      Success(ConfigFactory.parseMap(properties))
    } catch {
      case NonFatal(exception) =>
        import scala.jdk.CollectionConverters._
        // Config can't be parsed if map contains entries like a.b=1 and a=2
        // We want to find such cases...
        def findProblematicPaths(paths: Seq[Seq[String]], depth: Int): Seq[Seq[String]] = {
          if (!paths.exists(_.length > depth)) Nil
          else
            paths
              .groupBy(_.take(depth))
              .iterator
              .flatMap { case (root, nested) =>
                val fromCurrentLevel = if (paths.contains(root)) Seq(root) else Nil
                fromCurrentLevel ++ findProblematicPaths(nested, depth + 1)
              }
              .to(Seq)
        }

        val allPaths = properties.keySet().asScala.to(Seq).map(_.split('.').to(Seq))
        val problematicPaths = findProblematicPaths(allPaths, depth = 1).map(_.mkString("."))
        val newMap = problematicPaths.foldLeft(properties.asScala.toMap) { (props, key) =>
          (props + (s"$key.root" -> props(key))) - key
        }
        val res = Result.tryWith(WorkspaceConfig, 0)(Success(ConfigFactory.parseMap(newMap.asJava)))
        // .. and reports are warnings
        res
          .withProblems(problematicPaths.map(key => Warning(badKeyMsg(key), WorkspaceConfig, 0)))
          .withProblems(Seq(Error(exception.getMessage, WorkspaceConfig, 0)))
    }
  }

  private def badKeyMsg(key: String) =
    s"Property $key has both value and nested properties, it will be exposed as '$key.root'."

}
