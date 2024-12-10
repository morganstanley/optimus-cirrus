/* /*
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

import java.util

import msjava.base.selector._
import msjava.base.selector.ServiceSelector.SelectionState
import msjava.base.slr.internal.ServiceDescriptionSetConsumer
import msjava.base.slr.internal.ServiceLocationSupport
import msjava.base.sr.ServiceDescription
import msjava.zkapi.internal.ZkaContext
import optimus.platform.dsi.bitemporal.Context
import optimus.platform.dsi.bitemporal.NoVersioningServersException
import optimus.platform.dsi.versioning.PathHashT
import optimus.platform.dsi.versioning.VersioningKey

import scala.util.Random
import optimus.scalacompat.collection._

object OptimusVersioningZkClient extends ZkClientFactory[OptimusVersioningZkClient] {
  override protected type CacheKeyType = (String, ZkaContext)

  override def createClient(k: (String, ZkaContext)): OptimusVersioningZkClient = {
    val (instance, rootContext) = k
    new OptimusVersioningZkClient(instance, rootContext)
  }

  def getClient(instance: String): OptimusVersioningZkClient = {
    val k = (instance, ZkUtils.getRootContextForUriAuthority(instance))
    getClient(k)
  }

  private[optimus] val initialServiceSelectionTimeout: Int =
    Integer.getInteger("optimus.dsi.ZKConfig.ServiceSelectionTimeout", 10 * 1000)
}

class OptimusVersioningZkClient private (val instance: String, private[this] val rootContext: ZkaContext)
    extends ZkClient {
  import scala.jdk.CollectionConverters._

  @volatile private var servers = Map.empty[String, List[ServiceDescription]]

  val serviceUri = ZkUtils.createVersioningServerZkUri(rootContext, instance)

  val serviceConsumer = new ServiceDescriptionSetConsumer {
    override def consume(data: util.Collection[ServiceDescription]): Unit = {
      servers = data.asScala.groupBy(_.get(ZkUtils.versioningKeyAttributeName).toString).mapValuesNow(_.toList)
    }

    def getServers = servers
  }

  val serviceSelector = new ServiceSelector {
    // TODO (OPTIMUS-57173): rewrite versioning server storage/selection
    // once MSJava adds the above enhancement, we can improve the selection state logic and
    // finally tap into the flexible service selector framework that offers service selection based on failure
    // adjustment, hard/soft affinity, weight multiplication, weighted random etc. filters
    override def createSelectionState(): SelectionState = {
      new SelectionState {
        override def onNewSetOfServiceData(input: util.Collection[ServiceDescription]) = input
      }
    }
    override def selectNext(context: Any, state: SelectionState): SelectionContext = {
      val key = context.asInstanceOf[VersioningKey]
      val filteredServers = servers.getOrElse(key.id, throw new NoVersioningServersException(instance, key))
      val selectedServer = randomElementFrom(filteredServers)
      new SimpleSelectionContext(selectedServer)
    }
  }

  val serviceLocator = new ServiceLocationSupport
  serviceLocator.subscribeServiceDataEvents(serviceConsumer)
  serviceLocator.setServiceURI(serviceUri)
  serviceLocator.setServiceSelector(serviceSelector)
  serviceLocator.start

  override def close(): Unit = {
    serviceLocator.unsubscribeServiceDataEvents(serviceConsumer)
    serviceLocator.close
    servers = Map.empty
  }

  def getVersioningServers(): Map[String, List[String]] = {
    serviceLocator.forceRefresh
    serviceConsumer.getServers.mapValuesNow(_.map(_.getEndpointUrl))
  }

  // TODO (OPTIMUS-14722): support server-side versioning for non-default DAL contexts
  def getVersioningServer(versioningKey: VersioningKey): String = {
    serviceLocator
      .selectNext(versioningKey, OptimusVersioningZkClient.initialServiceSelectionTimeout)
      .getServiceDescription
      .getEndpointUrl
  }

  private[this] val random = new Random
  private[this] def randomElementFrom[T](list: List[T]): T = list(random.nextInt(list.size))
}
 */