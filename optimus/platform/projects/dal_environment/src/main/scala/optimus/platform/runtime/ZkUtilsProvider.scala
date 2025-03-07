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

import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig
import optimus.platform.dal.config.DalEnv
import com.ms.zookeeper.clientutils.ZkEnv
import msjava.zkapi.ZkaDirectoryWatcher
import msjava.zkapi.internal.ZkaContext
import optimus.config.RuntimeConfiguration
import optimus.dsi.zookeeper.ZNode
import org.apache.curator.framework.CuratorFramework

import scala.jdk.CollectionConverters._

import java.time.Instant

trait ZkUtilsProvider {
  def getZkEnv(env: DalEnv): ZkEnv

  def getRootContext(env: ZkEnv): ZkaContext
  def getRootContext(env: ZkEnv, kerberos: Boolean): ZkaContext
  def getRootContext(dalEnv: DalEnv): ZkaContext
  def getRootContext(dalEnv: DalEnv, kerberos: Boolean): ZkaContext
  def getRootContextAndEnv(env: ZkEnv, kerberos: Boolean): (ZkaContext, ZkEnv)
  def getRootContextAndEnv(dalEnv: DalEnv, kerberos: Boolean): (ZkaContext, ZkEnv)
  def getRootContextForUriAuthority(uriAuthority: String): ZkaContext

  def getContextForConfig(config: RuntimeConfiguration): ZkaContext

  protected[optimus] def createLeaderElectorContext(
      zkEnv: ZkEnv,
      kerberos: Boolean,
      curator: Option[CuratorFramework] = None): ZkaContext

  private[optimus] def createZkaContext(
      rootContext: ZkaContext,
      subPath: String,
      ensurePathCreated: Boolean = true): ZkaContext

  def devContext: ZkaContext
  def devContextNoKerberos: ZkaContext
  def qaContext: ZkaContext
  def qaContextNoKerberos: ZkaContext
  def prodContext: ZkaContext
  def prodContextNoKerberos: ZkaContext

  def prodContextWithoutGuard: ZkaContext
  def qaContextWithoutGuard: ZkaContext

  def parseInstant(raw: Array[Byte]): Instant
  def parseKnownZkEnv(envName: String): ZkEnv

  def getSilverKingZkConfig(zkEnv: ZkEnv, subNodeUnderOptimusOpt: Option[ZNode]): ZooKeeperConfig

  def isCloud: Boolean

  def isEnv(zkaContext: ZkaContext, env: ZkEnv): Boolean

  def isTestMode(mode: String): Boolean

  private[optimus] def unregisterContexts(): Unit

  private[optimus] def legacyZkLeaderElectionBaseNode: String

  private[optimus] def modernZkLeaderElectionBaseNode: String

  private[runtime] def legacyElectionNodeFilter(node: String): Boolean

  private[optimus] def waitZdwForInitialization(
      zdw: ZkaDirectoryWatcher,
      watchedPath: String,
      rootContext: ZkaContext,
      timeout: Long): Boolean

  private[optimus] def createVersioningServerZkUri(rootContext: ZkaContext, instance: String): String

  private[optimus] def versioningKeyAttributeName: String

  private[optimus] def getServiceDiscoveryLeaderElectionBaseNode(mode: String, leName: String): String

  private[optimus] def getSilverKingServicesNode(mode: String, leName: String): String

  private[optimus] def watcherTimeout: Int

}

object ZkUtils {
  private[runtime] val singleton: ZkUtilsProvider = {
    val loaded = java.util.ServiceLoader.load(classOf[ZkUtilsProvider]).asScala.toList
    loaded match {
      case p :: Nil => p
      case Nil      => throw new IllegalArgumentException(s"No ${classOf[ZkUtilsProvider].getName} provided")
      case _        => throw new IllegalArgumentException(s"Multiple ${classOf[ZkUtilsProvider].getName} provided")
    }
  }

  implicit def asInstance(self: ZkUtils.type): ZkUtilsProvider = singleton
}
