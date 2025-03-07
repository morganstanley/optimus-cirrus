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
package optimus.config

import optimus.breadcrumbs.ChainedID

import java.time.Instant

class OptimusConfigurationIOException(message: Option[String], cause: Option[Throwable])
    extends OptimusConfigurationException(message, cause)

class OptimusConfigurationException(val message: Option[String], val cause: Option[Throwable])
    extends RuntimeException(message.orNull, cause.orNull) {
  def this() = this(None, None)
  def this(message: String) = this(Some(message), None)
}

class MissingConfigPropertyException(val property: String)
    extends OptimusConfigurationException("Required property missing from config: " + property)

class MalformedConfigPropertyException(val property: String, val value: String)
    extends OptimusConfigurationException(s"Malformed property in config: ${property} = '${value}'")

object RuntimeConfiguration {
  def isProdEnv(env: String): Boolean = {
    Option(env)
      .map(_.toLowerCase.startsWith("prod"))
      .getOrElse(throw new IllegalArgumentException("RuntimeConfiguration.isProdEnv: env is null"))
  }
}

trait RuntimeConfiguration {
  def env: String
  def rootID: ChainedID
  def mode: String
  def instance: Option[String]
  def specificPropertyMap: Map[String, String]
  final def propertyMap: Map[String, String] = Map("rtcEnv" -> env, "rtcMode" -> mode) ++ specificPropertyMap

  def getProperties(baseName: String): Option[List[String]]
  def getAllProperties(baseName: String): Option[Set[String]]
  def getString(name: String): Option[String]
  def getInt(name: String): Option[Int]
  def getBoolean(name: String): Option[Boolean]
  def getInstant(name: String): Option[Instant]
  def getStringList(name: String): Option[List[String]]
  def getByteVector(name: String): Option[Vector[Byte]]
  def get(name: String): Option[Any]
  def getAttributes(name: String): Map[String, String]
  def getStringListWithAttributes(name: String): List[(String, Map[String, String])]

  def withOverride(name: String, value: Any): RuntimeConfiguration
  def withOverrides(m: Map[String, Any]): RuntimeConfiguration
}

trait HasRuntimeConfiguration {
  val config: RuntimeConfiguration
}
