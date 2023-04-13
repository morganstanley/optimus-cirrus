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
package com.ms.silverking.cloud.storagepolicy;

import java.util.List;

import com.ms.silverking.cloud.topology.Node;

public class RegionServers {
  private final List<Node> primary;
  private final List<Node> secondary;

  public RegionServers(List<Node> primary, List<Node> secondary) {
    this.primary = primary;
    this.secondary = secondary;
  }

  private void addList(StringBuilder sb, String name, List<Node> nodes) {
    sb.append(name);
    sb.append('\n');
    for (Node node : nodes) {
      sb.append(node);
      sb.append(' ');
    }
    sb.append('\n');
  }

  @Override
  public String toString() {
    StringBuilder sb;

    sb = new StringBuilder();
    addList(sb, "Primary", primary);
    addList(sb, "Secondary", secondary);
    return sb.toString();
  }
}
