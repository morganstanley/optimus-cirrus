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
package optimus.dal.ssl

import java.util.concurrent.atomic.AtomicBoolean
import optimus.graph.DiagnosticSettings
import optimus.platform.internal.SimpleStateHolder
import org.springframework.context.annotation.Condition
import org.springframework.context.annotation.ConditionContext
import org.springframework.core.`type`.AnnotatedTypeMetadata

final class DalSSLConfigParams {
  import DalSSLConfig._
  val enabledReplicationSSL = new AtomicBoolean(enabledReplicationSSLDefault)
  val enabledReplicationProxySSL = new AtomicBoolean(enabledReplicationProxySSLDefault)
}

object DalSSLConfig extends SimpleStateHolder(() => new DalSSLConfigParams) {

  private val EnableClientSSLParameter = "optimus.platform.dal.client.ssl.enabled"
  private val EnableReplicationSSLParameter = "optimus.platform.dal.replication.ssl.enabled"
  private val EnableReplicationProxySSLParameter = "optimus.platform.dal.replication.proxy.ssl.enabled"
  private val ForceKerberosedWriteBrokerParameter = "optimus.platform.dal.write.broker.kerberos"
  private val EnableZoneBasedClientSSL = "optimus.platform.dal.client.ssl.dedicated_port.dalclient"

  val EnableClientSSLDedicatedPortParameter: String = DalSSLConfigConstants.EnableClientSSLDedicatedPortParameter

  val forceKerberosedWriteBroker: Boolean =
    DiagnosticSettings.getBoolProperty(ForceKerberosedWriteBrokerParameter, false)
  val enabledClientSSL: Boolean = DiagnosticSettings.getBoolProperty(EnableClientSSLParameter, false)

  val enabledClientSSLWithDedicatedPort: Boolean =
    DiagnosticSettings.getBoolProperty(EnableClientSSLDedicatedPortParameter, false)

  val enabledZoneBasedClientSSL: Boolean = {
    if (DiagnosticSettings.getStringProperty(EnableZoneBasedClientSSL) == null) !enabledClientSSL
    else {
      val zoneClientSSLFlag = DiagnosticSettings.getBoolProperty(EnableZoneBasedClientSSL, true)
      if (zoneClientSSLFlag && enabledClientSSL)
        throw new IllegalStateException("Conflicting state between Zone Client protocol and Cloud Client protocol")
      zoneClientSSLFlag
    }
  }

  val enabledReplicationSSLDefault: Boolean = DiagnosticSettings.getBoolProperty(EnableReplicationSSLParameter, false)
  def enabledReplicationSSL: Boolean = getState.enabledReplicationSSL.get()
  def setReplicationSSL(v: Boolean): Unit = getState.enabledReplicationSSL.set(v)
  def resetReplicationSSL(): Unit = getState.enabledReplicationSSL.set(enabledReplicationSSLDefault)

  val enabledReplicationProxySSLDefault: Boolean =
    DiagnosticSettings.getBoolProperty(EnableReplicationProxySSLParameter, false)
  def enabledReplicationProxySSL: Boolean = getState.enabledReplicationProxySSL.get()
  def setReplicationProxySSL(v: Boolean): Unit = getState.enabledReplicationProxySSL.set(v)
  def resetReplicationProxySSL(): Unit = getState.enabledReplicationProxySSL.set(enabledReplicationProxySSLDefault)
}
