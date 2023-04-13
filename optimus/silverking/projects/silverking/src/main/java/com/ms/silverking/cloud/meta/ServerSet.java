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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.topology.Node;
import com.ms.silverking.io.StreamParser;
import com.ms.silverking.text.StringUtil;

/**
 * Common functionality used by ExclusionSet and PassiveNodeSet
 */
class ServerSet implements VersionedDefinition {
  private final Set<String> servers;
  private final long version;

  public ServerSet(Set<String> servers, long version) {
    this.servers = createCleanCopy(servers);
    this.version = version;
  }

  private static Set<String> createCleanCopy(Set<String> servers) {
    ImmutableSet.Builder<String> _servers;

    _servers = ImmutableSet.builder();
    for (String server : servers) {
      String cleanDef;

      cleanDef = cleanServerDef(server);
      if (cleanDef.length() > 0) {
        _servers.add(server);
      }
    }
    return _servers.build();
  }

  private static String cleanServerDef(String server) {
    return server.toLowerCase().trim();
  }

  public int size() {
    return servers.size();
  }

  public Set<String> getServers() {
    return servers;
  }

  @Override
  public long getVersion() {
    return version;
  }

  public ServerSet add(Set<String> newServers) {
    ImmutableSet.Builder<String> builder;

    builder = new ImmutableSet.Builder<>();
    ServerSet ss = new ServerSet(builder.addAll(servers).addAll(newServers).build(), getVersion());
    return ss;
    //return new ServerSet(builder.addAll(servers).addAll(newServers).build(), getVersion());
  }

  public ServerSet remove(Set<String> removedServers) {
    Set<String> _s;

    _s = new HashSet<>(servers);
    _s.removeAll(removedServers);
    return new ServerSet(ImmutableSet.copyOf(_s), getVersion());
  }

  public boolean contains(String id) {
    return servers.contains(id);
  }

  public List<Node> filter(List<Node> raw) {
    List<Node> filtered;

    filtered = new ArrayList<>(raw.size());
    for (Node node : raw) {
      if (!servers.contains(node.getIDString())) {
        filtered.add(node);
      }
    }
    return filtered;
  }

  public static ServerSet parse(File file) throws IOException {
    return parse(new FileInputStream(file), VersionedDefinition.NO_VERSION);
  }

  public static ServerSet parse(File file, long version) throws IOException {
    return parse(new FileInputStream(file), version);
  }

  public static ServerSet parse(InputStream in, long version) throws IOException {
    return new ServerSet(ImmutableSet.copyOf(filterExtraText(StreamParser.parseLines(in))), version);
  }

  private static List<String> filterExtraText(List<String> defs) {
    List<String> filtered;

    filtered = new ArrayList<>(defs.size());
    for (String def : defs) {
      String[] d;

      d = def.split("\\s+");
      filtered.add(d[0]);
    }
    return filtered;
  }

  @Override
  public boolean equals(Object o) {
    return servers.equals(((ServerSet) o).servers);
  }

  @Override
  public String toString() {
    return StringUtil.toString(servers, '\n');
  }
}
