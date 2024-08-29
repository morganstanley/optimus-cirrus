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
package optimus.dht.common.internal;

import java.net.InetSocketAddress;

import optimus.dht.common.api.RemoteInstanceIdentity;

public class ProvidedRemoteInstanceIdentity implements RemoteInstanceIdentity {

  private final String uniqueId;
  private final String codeVersion;
  private final String hostFQDN;
  private final long pid;
  private final String cloudName;
  private final InetSocketAddress remoteAddress;

  public ProvidedRemoteInstanceIdentity(
      String uniqueId,
      String codeVersion,
      String hostFQDN,
      long pid,
      String cloudName,
      InetSocketAddress remoteAddress) {
    this.uniqueId = uniqueId;
    this.codeVersion = codeVersion;
    this.hostFQDN = hostFQDN;
    this.pid = pid;
    this.cloudName = cloudName;
    this.remoteAddress = remoteAddress;
  }

  @Override
  public String uniqueId() {
    return uniqueId;
  }

  @Override
  public String codeVersion() {
    return codeVersion;
  }

  @Override
  public String hostFQDN() {
    return hostFQDN;
  }

  @Override
  public long pid() {
    return pid;
  }

  @Override
  public String cloudName() {
    return cloudName;
  }

  @Override
  public InetSocketAddress remoteAddress() {
    return remoteAddress;
  }

  @Override
  public String toString() {
    return "[uniqueId="
        + uniqueId
        + ", codeVersion="
        + codeVersion
        + ", hostFQDN="
        + hostFQDN
        + ", pid="
        + pid
        + ", cloudName="
        + cloudName
        + ", remoteAddress="
        + remoteAddress
        + "]";
  }
}
