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
import optimus.buildtool.config.ExtensionConfiguration
import optimus.buildtool.config.ExtensionProperties
import optimus.buildtool.format.ConfigUtils._
import scala.jdk.CollectionConverters._

import scala.collection.compat._
import scala.collection.immutable.Seq

object ExtensionConfigurationCompiler {

  private val extensionConfig = "extensions"

  def load(config: Config, origin: ObtFile): Result[Option[ExtensionConfiguration]] =
    Result
      .tryWith(origin, config) {
        Result.optional(config.hasPath(extensionConfig)) {
          val extension = config.getObject(extensionConfig)
          Result
            .sequence {
              extension.asScala
                .map { case (fileExtension, _) =>
                  val extensionConfig = extension.toConfig.getObject(fileExtension).toConfig
                  // The key '' is used here to identify files with no extension because an empty string is unfortunately not valid key in HOCON
                  val keyExtension = if (fileExtension.equals("''")) "" else fileExtension
                  loadProperties(extensionConfig, origin).map { properties =>
                    keyExtension -> properties
                  }
                }
                .to(Seq)
            }
            .map(ext => ExtensionConfiguration(ext.toMap))
        }
      }

  private def loadProperties(config: Config, origin: ObtFile): Result[ExtensionProperties] =
    Result
      .tryWith(origin, config) {
        config
          .loadOctal(origin, "mode")
          .map { mode =>
            ExtensionProperties(
              tpe = config.optionalString("type"),
              mode = mode
            )
          }
          .withProblems(config.checkExtraProperties(origin, Keys.extensionProperties))
      }
}
