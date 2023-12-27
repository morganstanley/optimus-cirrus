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
import com.typesafe.config.ConfigValue
import optimus.buildtool.config.ModuleType
import optimus.buildtool.config.PythonConfiguration
import optimus.buildtool.dependencies.PythonDefinition
import optimus.buildtool.dependencies.PythonDependencies
import optimus.buildtool.dependencies.PythonDependency

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

object PythonConfigurationCompiler {
  def libraryMissing(name: String) =
    s"""Library $name is not defined"""
  val lacksPythonVersion =
    s"""Python version not defined"""

  def load(
      config: Config,
      file: ObtFile,
      pythonDependencies: PythonDependencies): Result[Option[PythonConfiguration]] = {
    val errors: ArrayBuffer[Message] = ArrayBuffer.empty

    def error[A](msg: Message): Option[A] = {
      errors += msg
      None
    }

    def pythonDefinition(specifiedVersion: Option[String]): Option[PythonDefinition] = {
      pythonDependencies
        .pythonVersionForConfig(specifiedVersion)
        .map(pythonDefinition => Some(pythonDefinition))
        .getOrElse(error(Message(lacksPythonVersion, file)))
    }

    def resolveLibrary(value: ConfigValue, moduleType: ModuleType): Option[PythonDependency] = {
      val line = value.origin().lineNumber()
      value.unwrapped() match {
        case name: String =>
          pythonDependencies.findDependency(name, moduleType).orElse(error(Message(libraryMissing(name), file, line)))
        case _ => error(Message(s"Libraries should be defined as strings, but got: ${value.valueType()}", file, line))
      }
    }

    Result.tryWith(file, config) {

      val pythonConfiguration = if (config.hasPath("python")) {
        val pyCfg = config.getConfig("python")
        val pyVersion = if (pyCfg.hasPath("variant")) Some(pyCfg.getString("variant")) else None
        val moduleType = {
          val cfg = if (pyCfg.hasPath("type")) Some(pyCfg.getString("type")) else None
          cfg match {
            case Some(label) =>
              ModuleType.resolve(label).getOrElse {
                error(Message("No such python module type, defaulting to artifactory", file))
                ModuleType.default
              }
            case None => ModuleType.default
          }
        }
        val pyDefinition = pythonDefinition(pyVersion)
        val libs: Set[PythonDependency] =
          if (pyCfg.hasPath("libs"))
            pyCfg
              .getList("libs")
              .asScala
              .toSet
              .flatMap { configValue: ConfigValue =>
                resolveLibrary(configValue, moduleType)
              }
          else Set()

        if (errors.isEmpty)
          pyDefinition.map(python => PythonConfiguration(python, libs, moduleType))
        else None
      } else None

      Success(pythonConfiguration, errors.toIndexedSeq)
    }
  }
}
