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

import java.net.URI
import msjava.slf4jutils.scalalog._
import msjava.zkapi.internal.ZkaContext
import optimus.breadcrumbs.ChainedID
import optimus.config.OptimusConfigurationException
import optimus.config.RuntimeConfiguration
import optimus.dsi.partitioning.PartitionMap
import optimus.utils.PropertyUtils
import optimus.platform.RuntimeEnvironmentKnownNames._
import optimus.platform.dal.DSIURIScheme
import optimus.platform.dal.RuntimeProperties
import optimus.platform.dal.config.DalAppId
import optimus.platform.dal.config.DalEnv
import optimus.platform.dal.config.DalLocation
import optimus.platform.dal.config.DalUri
import optimus.platform.dal.config.DalZoneId
import optimus.platform.dal.servicediscovery.AutomaticBrokerResolution

object ClientRuntimeConfigurationFactoryBase {
  private val log: Logger = getLogger(this)
  private[optimus] val disableZookeeperKerberos =
    PropertyUtils.get("optimus.platform.runtime.client.config.zk.disableKerberos", false)
}

trait ClientRuntimeConfigurationFactoryBase extends WithZkOpsMetrics {
  import ClientRuntimeConfigurationFactoryBase._

  /**
   * This method performs some extra checks on the input URI such as broker:// or replica:// input is not allowed for
   * non-dev envs. Also this method resolves broker://dev?context=[unique|shared|named] URI by doing one more
   * indirection via ZK configuration.
   */
  def getClientRuntimeConfigurationFromDalLocation(
      location: DalLocation,
      appId: DalAppId,
      zoneId: DalZoneId): RuntimeConfiguration = location match {
    case d: DalEnv => getClientRuntimeConfigurationFromEnv(d, appId, zoneId)
    // Here we do an indirection for non-default context URIs such as broker://dev?context=unique by
    // loading configuration for "dev" from ZK and then putting back context related query params
    // back in the resolved uri. This will help tackle the issue where we deploy "write-only" brokers
    // and due to that broker://<dalenv>?context=[unique|named|shared] uris would no longer work.
    case uri @ DalUri(u) if uri.isNonDefaultContext =>
      log.info(s"Resolving non-default context URI: $u")
      val dalEnv = DalEnv(u.getAuthority)
      val conf = getClientRuntimeConfigurationFromEnv(dalEnv, appId, zoneId)
      val resolvedUri = new URI(
        conf
          .getString(RuntimeProperties.DsiUriProperty)
          .getOrElse(
            throw new OptimusConfigurationException("DSI URI is not defined: " + RuntimeProperties.DsiUriProperty)))
      val resolvedUriWithContext: String = {
        val queryMap = DSIURIScheme.getQueryMap(resolvedUri)
        if (queryMap.isEmpty) s"$resolvedUri?${u.getQuery}"
        else s"$resolvedUri&${u.getQuery}"
      }
      log.info(s"After resolution new URI is: $resolvedUriWithContext")
      conf
        .withOverride(RuntimeProperties.DsiUriProperty, resolvedUriWithContext)
        .withOverride(PartitionMap.PartitionMapProperty, PartitionMap.empty)
    case DalUri(u) =>
      val conf = getClientRuntimeConfigurationFromEnv(DalEnv.mock, appId, zoneId)
      conf
        .withOverride(RuntimeProperties.DsiUriProperty, u.toString)
        .withOverride(PartitionMap.PartitionMapProperty, PartitionMap.empty)
  }

  /**
   * Builds a runtime configuration using zookeeper with application specific overrides.
   *
   * @param env
   *   The environment to connect to e.g. "dev"
   * @param appId
   *   The application identifier used for monitoring and trouble shooting.
   * @param inputZone
   *   The quality of service zone used for automatic broker resolution and workload management. You may specify
   *   RuntimeProperties.DsiZonePropertyDefaultValue for development purposes only which runs on best efforts on free
   *   capacity.
   */
  protected def getClientRuntimeConfigurationFromEnv(
      env: DalEnv,
      appId: DalAppId,
      inputZone: DalZoneId): RuntimeConfiguration = {
    log.info("creating runtime factory for env {}, appId {}, inputZone: {}", env, appId, inputZone)
    val (mode, region) = env.components

    val conf = withZkOpsMetrics(ChainedID.root) { timer =>
      if (mode == EnvMock)
        new MockRuntimeConfiguration(mode)
      else if (mode == EnvNone)
        new MockRuntimeConfiguration(mode, appId = DalAppId("NoRuntimeConfiguration"))
      else {
        val (zkCurator, zkEnv) = {
          val (ctx, zkEnv) = timer.timed("getRootContextAndEnv") {
            getZooKeeperRootContextAndEnv(env)
          }
          (ctx.getCurator, zkEnv)
        }

        val resolvedDalEnv =
          AutomaticBrokerResolution.resolveSpecificDalInstance(
            env,
            inputZone,
            RuntimeConfigurationFactory.sysLoc,
            zkCurator,
            zkEnv,
            timer)
        makeZkRuntimeConfiguration(resolvedDalEnv.mode, resolvedDalEnv.instance, zkEnv)
      }
    }

    val partitionMap =
      if (mode == EnvAws || mode == EnvNone || mode == EnvMock) PartitionMap.empty else PartitionMap(env)
    conf
      .withOverride(RuntimeProperties.DsiZoneProperty, inputZone.underlying)
      .withOverride(RuntimeProperties.DsiAppIdProperty, appId.underlying)
      .withOverride(PartitionMap.PartitionMapProperty, partitionMap)
  }

  protected def getZooKeeperRootContextAndEnv(env: DalEnv): (ZkaContext, ZkEnv) =
    ZkUtils.getRootContextAndEnv(env, !disableZookeeperKerberos)

  protected def makeZkRuntimeConfiguration(
      mode: String,
      instance: Option[String],
      zookeeperEnv: ZkEnv): ZkRuntimeConfiguration =
    new ZkRuntimeConfiguration(mode, instance, zookeeperEnv)
}

object RuntimeConfigurationFactory extends ClientRuntimeConfigurationFactoryBase {
  val sysLoc = sys.env.get("SYS_LOC")
}
