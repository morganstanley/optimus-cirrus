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
import com.ms.zookeeper.clientutils.ZkEnv
import optimus.config.RuntimeEnvironmentEnum

import scala.jdk.CollectionConverters._

trait DALEnvsProvider {
  def getRuntimeEnvforDALEnv(dalEnv: DalEnv): RuntimeEnvironmentEnum
  def envInfoRuntime(dalEnv: DalEnv): RuntimeEnvironmentEnum

  def brokerVirtualHostname(env: DalEnv): String

  def getZkEnvForDALEnv(mode: String): ZkEnv

  def splunkInstanceQa: String
  def splunkInstanceSafe(dalEnv: String): String
}

object DALEnvs {
  private[config] val singleton: DALEnvsProvider = {
    val loaded = java.util.ServiceLoader.load(classOf[DALEnvsProvider]).asScala.toList
    loaded match {
      case p :: Nil => p
      case Nil      => throw new IllegalArgumentException(s"No ${classOf[DALEnvsProvider].getName} provided")
      case _        => throw new IllegalArgumentException(s"Multiple ${classOf[DALEnvsProvider].getName} provided")
    }
  }

  implicit def asInstance(self: DALEnvs.type): DALEnvsProvider = singleton
}
