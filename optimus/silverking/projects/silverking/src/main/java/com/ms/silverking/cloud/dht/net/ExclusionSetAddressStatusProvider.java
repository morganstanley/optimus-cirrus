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
package com.ms.silverking.cloud.dht.net;

import java.net.InetSocketAddress;

import com.ms.silverking.cloud.meta.ExclusionSet;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.net.async.AddressStatusProvider;

public class ExclusionSetAddressStatusProvider implements AddressStatusProvider {
  private final String addressStatusProviderThreadName;
  private final IPAliasMap aliasMap;
  private volatile ExclusionSet exclusionSet;

  public ExclusionSetAddressStatusProvider(String addressStatusProviderThreadName, IPAliasMap aliasMap) {
    this.addressStatusProviderThreadName = addressStatusProviderThreadName;
    this.aliasMap = aliasMap;
    exclusionSet = ExclusionSet.emptyExclusionSet(0);
  }

  public void setExclusionSet(ExclusionSet exclusionSet) {
    this.exclusionSet = exclusionSet;
  }

  @Override
  public boolean isHealthy(InetSocketAddress addr) {
    IPAndPort peer;

    peer = aliasMap.interfaceToDaemon(addr);
    return !exclusionSet.contains(peer.getIPAsString());
  }

  @Override
  public boolean isAddressStatusProviderThread(String context) {
    return addressStatusProviderThreadName.equals(context != null ? context : Thread.currentThread().getName());
  }

  @Override
  public boolean isAddressStatusProviderThread() {
    return isAddressStatusProviderThread(Thread.currentThread().getName());
  }
}
