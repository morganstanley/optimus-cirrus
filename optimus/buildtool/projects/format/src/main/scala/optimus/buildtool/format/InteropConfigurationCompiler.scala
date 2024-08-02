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
import optimus.buildtool.config.InteropConfiguration
import optimus.buildtool.config.ModuleId
import optimus.buildtool.format.ConfigUtils._

import scala.collection.mutable.ArrayBuffer

object InteropConfigurationCompiler {
  val lacksInteropModule = s"Interop module is not defined"
  def invalidModule(module: String) = s"Invalid scope ID: $module"

  private val interop = "interop"
  private val pythonModule = "py-module"
  private val jvmModule = "jvm-module"

  def load(config: Config, file: ObtFile): Result[Option[InteropConfiguration]] = {
    val errors: ArrayBuffer[Message] = ArrayBuffer.empty

    def addError[A](msg: Message): Option[A] = {
      errors += msg
      None
    }

    def getModuleId(field: String): Option[ModuleId] = {
      val modulePath = s"$interop.$field"
      if (config.hasPath(modulePath)) {
        val moduleStr = config.getString(modulePath)
        ModuleId
          .parseOpt(moduleStr)
          .orElse(addError(Message(invalidModule(moduleStr), file, config.getValue(modulePath).origin().lineNumber())))
      } else None
    }

    Result.tryWith(file, config) {
      Result.optional(config.hasPath(interop)) {
        val interopConfig = config.getConfig(interop)
        (getModuleId(pythonModule), getModuleId(jvmModule)) match {
          case (None, None) =>
            addError(Message(lacksInteropModule, file, config.getValue(interop).origin().lineNumber()))
            Failure(errors.toIndexedSeq).withProblems(interopConfig.checkExtraProperties(file, Keys.interopObtFile))
          case (pythonScope, jvmScope) =>
            Success(InteropConfiguration(pythonScope, jvmScope), errors.toIndexedSeq).withProblems(
              interopConfig.checkExtraProperties(file, Keys.interopObtFile)
            )
        }
      }
    }
  }
}
