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
package optimus.dal.ktcp

import optimus.config.RuntimeConfiguration
import optimus.platform.dal.ClientBrokerContext
import optimus.platform.dal.DSIURIScheme
import optimus.platform.dal.RuntimeProperties
import optimus.platform.dal.config.DalAsyncConfig

import java.net.URI

class SktcpDSIFactoryImpl(config: RuntimeConfiguration, asyncConfigOpt: Option[DalAsyncConfig])
    extends KtcpDSIFactoryImpl(config, asyncConfigOpt) {
  override def uriScheme: String = DSIURIScheme.SKTCP
  override protected def secureTransport: Boolean =
    !config.getBoolean(RuntimeProperties.DsiKnownInsecureOutlet).getOrElse(false)
  override protected def augment(
      context: ClientBrokerContext,
      authority: String,
      queryMap: => Map[String, String]): ClientBrokerContext =
    queryMap.get("sni") match {
      case Some(sni) => context.copy(brokerVirtualHostname = Some(sni))
      case None =>
        if (!queryMap.contains("nosni"))
          context.copy(brokerVirtualHostname = Some(authority))
        else context
    }
  override protected def replicaFallback(replica: URI, queryMap: => Map[String, String]): URI =
    queryMap
      .get("replica")
      .map { hostPort =>
        new URI(s"$uriScheme://$hostPort")
      }
      .orElse(queryMap.get("insec_replica").map { hostPort =>
        new URI(s"ktcp://$hostPort")
      })
      .getOrElse(replica)
  override protected def writeFallback(write: Option[URI], queryMap: => Map[String, String]): Option[URI] =
    write.orElse {
      queryMap.get("insec_writer").map(hostPort => new URI("ktcp://" + hostPort))
    }
  override protected def pubsubFallback(pubsub: Option[URI], queryMap: => Map[String, String]): Option[URI] =
    pubsub.orElse(queryMap.get("insec_pubsub").map(hostPort => new URI("ktcp://" + hostPort)))
  override protected def parameterizedSecureTransport(secureTransport: Boolean, uri: _root_.java.net.URI): Boolean =
    if (secureTransport) uri.getScheme == uriScheme else false
}
