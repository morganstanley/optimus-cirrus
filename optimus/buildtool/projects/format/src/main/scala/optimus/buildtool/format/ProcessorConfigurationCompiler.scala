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
import optimus.buildtool.config.ProcessorConfiguration
import optimus.buildtool.processors.ProcessorType

import scala.jdk.CollectionConverters._
import scala.collection.compat._
import scala.collection.immutable.Seq

object ProcessorConfigurationCompiler {

  private val processors = "processors"

  def load(config: Config, origin: ObtFile): Result[Seq[(ProcessorType, ProcessorConfiguration)]] = {
    val configs = if (config.hasPath(processors)) {
      val processorsConfig = config.getConfig(processors)
      Result.sequence {
        processorsConfig.root.keySet.asScala
          .map { name =>
            load(processorsConfig.getConfig(name), origin, name)
          }
          .to(Seq)
      }
    } else Success(Nil)
    configs.map(_.toVector.sortBy(_._2.name))
  }

  private def load(config: Config, origin: ObtFile, name: String): Result[(ProcessorType, ProcessorConfiguration)] =
    Result
      .tryWith(origin, config) {
        def relpath(key: String) = Result.optional(config.hasPath(key))(config.relativePath(key, origin))
        for {
          templateFile <- config.relativePath("template", origin)
          templateHeaderFile <- relpath("templateHeader")
          templateFooterFile <- relpath("templateFooter")
          objectsFile <- relpath("objects")
          installLocation <- config.relativePath("installLocation", origin)
        } yield {
          val cfg = if (config.hasPath("configuration")) {
            config
              .getConfig("configuration")
              .entrySet()
              .asScala
              .map(e => e.getKey -> e.getValue.unwrapped.asInstanceOf[String])
              .toMap
          } else Map.empty[String, String]
          val tpe = config.stringOrDefault("type", name)
          ProcessorType(tpe) -> ProcessorConfiguration(
            name = name,
            templateFile = templateFile,
            templateHeaderFile = templateHeaderFile,
            templateFooterFile = templateFooterFile,
            objectsFile = objectsFile,
            installLocation = installLocation,
            configuration = cfg
          )
        }
      }
      .withProblems(config.checkExtraProperties(origin, Keys.processorConfiguration))

}
