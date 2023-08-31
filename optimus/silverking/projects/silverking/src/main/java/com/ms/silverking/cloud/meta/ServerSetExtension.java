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
package com.ms.silverking.cloud.meta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.topology.Node;
import com.ms.silverking.net.IPAndPort;

/** Common functionality used by ExclusionSet to wrap ServerSet. */
public abstract class ServerSetExtension implements VersionedDefinition {
  public static final long INVALID_ZXID = -1;
  public static final String singleLineDelimiter = ",";

  protected final ServerSet serverSet;
  protected long mzxid;

  ServerSetExtension(ServerSet serverSet) {
    this.serverSet = serverSet;
  }

  public long getMzxid() {
    return mzxid;
  }

  public int size() {
    return serverSet.size();
  }

  public Set<String> getServers() {
    return serverSet.getServers();
  }

  @Override
  public long getVersion() {
    return serverSet.getVersion();
  }

  public boolean contains(String serverID) {
    return serverSet.contains(serverID);
  }

  @Override
  public boolean equals(Object o) {
    return serverSet.equals(((ServerSetExtension) o).serverSet);
  }

  @Override
  public String toString() {
    return serverSet.toString();
  }

  public ServerSetExtension addByIPAndPort(Set<IPAndPort> newWorrisomeEntities) {
    Set<String> s;

    s = new HashSet<>();
    for (IPAndPort e : newWorrisomeEntities) {
      s.add(e.getIPAsString());
    }
    return add(s);
  }

  public abstract ServerSetExtension add(Set<String> newExcludedEntities);

  public ServerSetExtension removeByIPAndPort(Set<IPAndPort> newWorrisomeEntities) {
    Set<String> s;

    s = new HashSet<>();
    for (IPAndPort e : newWorrisomeEntities) {
      s.add(e.getIPAsString());
    }
    return remove(s);
  }

  public abstract ServerSetExtension remove(Set<String> newExcludedEntities);

  public Set<IPAndPort> asIPAndPortSet(int port) {
    Set<IPAndPort> s;

    s = new HashSet<>();
    for (String server : serverSet.getServers()) {
      s.add(new IPAndPort(server, port));
    }
    return ImmutableSet.copyOf(s);
  }

  public List<Node> filter(List<Node> raw) {
    List<Node> filtered;

    filtered = new ArrayList<>(raw.size());
    for (Node node : raw) {
      if (!getServers().contains(node.getIDString())) {
        filtered.add(node);
      }
    }
    return filtered;
  }

  public List<IPAndPort> filterByIP(Collection<IPAndPort> raw) {
    List<IPAndPort> filtered;

    filtered = new ArrayList<>(raw.size());
    for (IPAndPort node : raw) {
      boolean worrisome;

      worrisome = false;
      for (String server : getServers()) {
        if (node.getIPAsString().equals(server)) {
          worrisome = true;
          break;
        }
      }
      if (!worrisome) {
        filtered.add(node);
      }
    }
    return filtered;
  }
}
