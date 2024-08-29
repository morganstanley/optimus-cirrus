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

import java.net.URI
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

import com.ms.silverking.cloud.dht.client.DHTSession
import com.ms.silverking.cloud.dht.client.impl.DHTSessionImpl
import optimus.dal.silverking.client.PrcNamespaceTemplate

/*
 * Ways to look up SilverKing plants from ZooKeeper configuration
 * All plants have tags which can be parsed into a SilverKingPlantConfig. However, there are different plants stored
 * in different places in config: for example, payload SK plants live in brokerconfig whilst PRC SK plants live in
 * dalServices
 */
private[optimus] sealed trait SilverKingLookup extends Any {
  def plantName: String
  def connectionString: String
}

/*
 * A SilverKing plant lookup based on information in dalServices. In order to identify such a plant, we need to know
 * the DAL mode, the instance name, and the plantName. This will ultimately be resolved to a SilverKingPlantConfig
 * by looking at /optimus/dalServices/{env.mode}/silverking_nole/{instance} for a tag called {plantName}. For more on
 * this, see PrcServiceConfig.
 */
private[optimus] final case class DalServicesSilverKingLookup(env: DalEnv, instance: String, plantName: String)
    extends SilverKingLookup {
  override def connectionString: String = s"dalServices:$env/$instance/$plantName"
}
object DalServicesSilverKingLookup {
  val UriScheme = "skClient"

  /*
   * returns DalServicesSilverKingLookup based on the uri provided
   * The lookup happens from a client config node
   * @param uri - URI should be in the format: skClient://mode/instance/clusterName
   */
  def fromUri(uri: URI): DalServicesSilverKingLookup = {
    val mode = uri.getAuthority
    // filter for empty path segments because "/foo/bar".split("/") == Array("", "foo", "bar")
    uri.getPath.split("/").filterNot(_.isEmpty) match {
      case Array(instance, clusterName) => DalServicesSilverKingLookup(DalEnv(mode), instance, clusterName)
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid SK URI $uri is not in the format 'skClient://mode/instance/clusterName'")
    }
  }

  def fromString(env: DalEnv, str: String): Option[DalServicesSilverKingLookup] = str.split("/") match {
    case Array(instance, plantName) => Some(apply(env, instance, plantName))
    case _                          => None
  }
}

/*
 * A SilverKing plant lookup based on information in brokerconfig. In order to identify such a plant, we need to know
 * just the plant name; we will use the ZkRuntimeConfiguration of a specific environment's brokerconfig ZK configuration
 * to look up a tag at optimus.dsimappings.silverking.plants.{plantName}. For more on this, take a look at
 * SilverKingPlantConfig.fromBrokerConfig.
 */
private[optimus] final case class BrokerConfigSilverKingLookup(plantName: String) extends AnyVal with SilverKingLookup {
  override def connectionString: String = s"brokerConfig:$plantName"
}

/*
 * For test purposes, a SilverKing lookup which is a wrapper around an existing DHTSession (i.e. no lookup will
 * actually be necessary to create the SK connection later). As session can get closed so it supports adding a
 * new session which will be the existing session going forward.
 */
private[optimus] final case class ExistingDhtSessionSilverKingLookup(
    private val dhtSession: DHTSession,
    plantName: String,
    namespaceTemplate: PrcNamespaceTemplate)
    extends SilverKingLookup {
  override def connectionString = s"existingSession:$plantName"

  private val existingSession = new AtomicReference[DHTSession](dhtSession)

  private val sessionCounter = new AtomicInteger(1)

  def getExistingSession = existingSession.get()

  def setExistingSession(newSession: DHTSession): Unit = {
    require(getExistingSession.asInstanceOf[DHTSessionImpl].isClosed)
    require(!newSession.asInstanceOf[DHTSessionImpl].isClosed)
    existingSession.set(newSession)
    sessionCounter.incrementAndGet()
  }

  private[optimus] def getSessionCount = sessionCounter.get()

  private[optimus] def isSessionClosed = getExistingSession.asInstanceOf[DHTSessionImpl].isClosed
}
