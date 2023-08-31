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
package com.ms.silverking.cloud.dht.client;

import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.SessionOptions;
import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.net.AddrAndPort;
import com.ms.silverking.text.FieldsRequirement;
import com.ms.silverking.text.ObjectDefParser2;

/** Client configuration settings for a single DHT. */
public class ClientDHTConfiguration implements ClientDHTConfigurationProvider {
  private final String name;
  private final int port;
  private final ZooKeeperConfig zkConfig;

  private static final int portInZKOnly = Integer.MIN_VALUE;

  public static final String nameVar = "GC_SK_NAME";
  public static final String portVar = "GC_SK_PORT";
  public static final String zkLocVar = "GC_SK_ZK_LOC";
  public static final String skfsRootVar = "GC_SKFS_ROOT";

  private static final Set<String> optionalFields;

  public static final ClientDHTConfiguration emptyTemplate =
      new ClientDHTConfiguration("__sample_name__", 1, "localhost:0");
  private static final Class[] constructorFieldClasses =
      new Class[] {String.class, int.class, String.class};
  private static final String[] constructorFieldNames = new String[] {"name", "port", "zkLocs"};

  static {
    ImmutableSet.Builder<String> builder;

    builder = ImmutableSet.builder();
    optionalFields = builder.build();

    ObjectDefParser2.addParser(
        emptyTemplate,
        FieldsRequirement.REQUIRE_ALL_NONOPTIONAL_FIELDS,
        optionalFields,
        constructorFieldClasses,
        constructorFieldNames);
  }

  public ClientDHTConfiguration(String dhtName, int dhtPort, ZooKeeperConfig zkConfig) {
    Preconditions.checkNotNull(dhtName, "dhtName must be non-null");
    Preconditions.checkArgument(
        dhtPort > 0 || dhtPort == portInZKOnly, "dhtPort must be > 0. Found: ", dhtPort);
    Preconditions.checkNotNull(zkConfig, "zkConfig must be non-null");
    this.name = dhtName;
    this.port = dhtPort;
    this.zkConfig = zkConfig;
  }

  @OmitGeneration
  public ClientDHTConfiguration(String dhtName, ZooKeeperConfig zkConfig) {
    this(dhtName, portInZKOnly, zkConfig);
  }

  public ClientDHTConfiguration(String dhtName, String zkConfig) {
    this(dhtName, portInZKOnly, zkConfig);
  }

  public ClientDHTConfiguration(String dhtName, int dhtPort, String zkLocs) {
    this(dhtName, dhtPort, new ZooKeeperConfig(zkLocs));
  }

  @OmitGeneration
  public static ClientDHTConfiguration create(Map<String, String> envMap) {
    return new ClientDHTConfiguration(
        envMap.get(nameVar),
        envMap.get(portVar) == null ? portInZKOnly : Integer.parseInt(envMap.get(portVar)),
        new ZooKeeperConfig(envMap.get(zkLocVar)));
  }

  public Map<String, String> getEnvMap() {
    ImmutableMap.Builder<String, String> ret;
    ret = ImmutableMap.builder();
    ret.put(portVar, "" + getPort());
    ret.put(nameVar, getName());
    ret.put(zkLocVar, getZKConfig().getConnectString());
    return ret.build();
  }

  public String getName() {
    return name;
  }

  public int getPort() {
    return port;
  }

  public ZooKeeperConfig getZKConfig() {
    return zkConfig;
  }

  @Override
  public ClientDHTConfiguration getClientDHTConfiguration() {
    return this;
  }

  public String toString() {
    StringBuilder sb;

    sb = new StringBuilder();
    sb.append(name);
    sb.append(':');
    sb.append(zkConfig);
    return sb.toString();
  }

  public boolean hasPort() {
    return port != portInZKOnly;
  }

  @Override
  public int hashCode() {
    return name.hashCode() ^ port ^ zkConfig.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    ClientDHTConfiguration otherCDC;

    otherCDC = (ClientDHTConfiguration) other;
    if (!name.equals(otherCDC.name)) {
      return false;
    } else if (port != otherCDC.port) {
      return false;
    } else {
      return zkConfig.equals(otherCDC.zkConfig);
    }
  }

  public static ClientDHTConfiguration parse(String def) {
    ClientDHTConfiguration instance;

    instance = ObjectDefParser2.parse(ClientDHTConfiguration.class, def);
    return instance;
  }
}
