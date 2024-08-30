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
package optimus.platform.runtime

import com.ms.zookeeper.clientutils.ZkEnv
import msjava.slf4jutils.scalalog.getLogger
import optimus.utils.PropertyUtils

trait ZkAccessFlagsTrait {
  private val log = getLogger(this)

  val ForceZkEnv = "optimus.platform.runtime.ZkEnvToUse"
  private[optimus] val OnlyAllowedZkEnvs = "optimus.platform.runtime.OnlyAllowedZkEnvs"

  private def resolveForcedZkEnv(envName: String): ZkEnv = {
    log.info(s"Zk Env passed via `-D$ForceZkEnv` flag: $envName.")
    ZkUtils.parseKnownZkEnv(envName)
  }

  private def resolveOnlyAllowedZkEnvs(commaDelimitedZkEnvNames: String): Seq[ZkEnv] = {
    log.info(s"Zk envs passed via -D$OnlyAllowedZkEnvs: $commaDelimitedZkEnvNames")
    commaDelimitedZkEnvNames.split(",").iterator.map(ZkUtils.parseKnownZkEnv).toIndexedSeq
  }

  protected[runtime] def forcedZkEnv(): Option[ZkEnv] =
    PropertyUtils.get(ForceZkEnv).map(resolveForcedZkEnv)

  protected[runtime] def onlyAllowedZkEnvs(): Seq[ZkEnv] =
    PropertyUtils
      .get(OnlyAllowedZkEnvs)
      .map(resolveOnlyAllowedZkEnvs)
      .getOrElse(Nil)
}

object ZkAccessFlags extends ZkAccessFlagsTrait {
  override protected[runtime] def forcedZkEnv(): Option[ZkEnv] = super.forcedZkEnv()
  override protected[runtime] def onlyAllowedZkEnvs(): Seq[ZkEnv] = super.onlyAllowedZkEnvs()
}
