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
package optimus.platform.dal.config

import java.util.concurrent.atomic.AtomicReference

import msjava.slf4jutils.scalalog.Logger
import msjava.slf4jutils.scalalog.getLogger
import optimus.config.RuntimeConfiguration

// This object contains runtime-config related information for DAL crumbs
// can be converted to a class and extended if needed the ambient context grows
// currently caches a RuntimeConfiguration object and provides extractor methods for DalEnv from said object
object DalConfigurationContext {

  private val config: AtomicReference[Option[RuntimeConfiguration]] =
    new AtomicReference[Option[RuntimeConfiguration]](None)
  private val log: Logger = getLogger(this)

  def init(configuration: RuntimeConfiguration): Unit = config.set(Some(configuration))

  protected def getDalEnv(): Option[DalEnv] = config.get().map(c => DalEnv(c.env))

  /*
  Config info Extractor methods
   */

  // generic method for extracting information if possible and putting it in a map else returning an empty map
  // it is a safe end-point for getting low-importance data (breadcrumb field)
  private def getStringMapFromOption[T](key: String, f: T => String, valueOpt: Option[T]): Map[String, String] = {
    valueOpt
      .map(v => Map(key -> f(v)))
      .getOrElse(Map.empty)
  }

  // adds an environment key-value pair (key = "env" ) to a Map[String,String] if it can and returns it otherwise returns empty map.
  // (for example, "env" -> "devdev:ny")
  def wrap(properties: Map[String, String]) =
    properties ++ getStringMapFromOption[DalEnv]("env", _.underlying, getDalEnv())
}
