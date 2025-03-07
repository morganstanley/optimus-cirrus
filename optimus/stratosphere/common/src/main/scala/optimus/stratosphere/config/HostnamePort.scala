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
import com.typesafe.config.ConfigFactory

final case class HostnamePort(hostname: String, port: Int)

object HostnamePort {

  def apply(config: Config): HostnamePort = {
    val hostname: String = config.getString("hostname")
    val port: Int = config.getInt("port")
    HostnamePort(hostname, port)
  }

  def parse(raw: String): HostnamePort =
    apply(ConfigFactory.parseString(raw))
}
