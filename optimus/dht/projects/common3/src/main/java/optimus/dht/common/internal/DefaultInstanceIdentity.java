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

import msjava.tools.util.MSProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import optimus.dht.common.api.InstanceIdentity;
import optimus.dht.common.util.DHTUtil;
import optimus.dht.common.util.DHTVersion;

public class DefaultInstanceIdentity implements InstanceIdentity {

  private static final Logger logger = LoggerFactory.getLogger(DefaultInstanceIdentity.class);

  private final String uniqueId;
  private final String hostFQDN;
  private final long pid;
  private final String cloudName;

  public DefaultInstanceIdentity() {
    this(DHTUtil.randomAlphanumeric(12));
  }

  public DefaultInstanceIdentity(String uniqueId) {
    this.uniqueId = uniqueId;
    this.hostFQDN = DHTUtil.getFQDN();
    this.pid = MSProcess.getPID();
    this.cloudName = DHTUtil.getCloudName();
  }

  @Override
  public String uniqueId() {
    return uniqueId;
  }

  @Override
  public String codeVersion() {
    return DHTVersion.getVersion();
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
  public String toString() {
    return this.getClass().getSimpleName()
        + "[uniqueId="
        + uniqueId
        + ", hostFQDN="
        + hostFQDN
        + ", pid="
        + pid
        + ", cloudName="
        + cloudName
        + "]";
  }
}
