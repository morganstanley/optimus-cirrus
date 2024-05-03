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
package optimus.core.utils

import msjava.zkapi.ZkaConfig
import msjava.zkapi.ZkaFactory
import msjava.zkapi.internal.ZkaData
import optimus.platform.util.Log
import org.apache.curator.framework.CuratorFramework

// to be decided how we will split this when we have kafka crumbs and sprofiling kafka
trait ZkClient extends Log {

  /** path to zookeeper node that we want to parse zk data from */
  val path: String

  /** parse the data in zk node */
  def parseZkData(zkCurator: CuratorFramework): Option[ZkaData] = {
    try {
      val zkNodeData = zkCurator.getData.forPath(path)
      val kafkaZkData = ZkaData.fromBytes(zkNodeData)
      log.info(s"Parsed and got zk data from $path: $path")
      Some(kafkaZkData)
    } catch {
      case e: Exception =>
        log.error(s"Could not get data from $path", e)
        None
    }
  }
}

object ZkClient extends Log {
  private def buildBaseZkUri(env: String, region: String = "na"): String = s"zps://$env.$region/optimus"

  def initializeClient(env: String): Option[CuratorFramework] = {
    val uri = buildBaseZkUri(env)
    val zkCuratorClient = buildZkClient(uri)

    try {
      zkCuratorClient
        .map(client => {
          client.start()
          client.getZookeeperClient.blockUntilConnectedOrTimedOut
          client
        })
    } catch {
      case e: Exception =>
        log.error(s"Could not connect to zookeeper", e)
        None
    }
  }

  private def buildZkClient(zkUri: String): Option[CuratorFramework] = {
    try {
      // retries most likely needed
      val zkaConfig = ZkaConfig.fromURI(zkUri)
      val curatorZookeeperClient = ZkaFactory.newCurator(zkaConfig)
      Some(curatorZookeeperClient)
    } catch {
      case e: Throwable =>
        log.error(s"Failed to create zk curator for zkPath: $zkUri; exception: $e")
        None
    }
  }
}
