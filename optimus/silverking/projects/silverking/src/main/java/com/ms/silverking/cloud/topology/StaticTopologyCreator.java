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
package com.ms.silverking.cloud.topology;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.ImmutableList;

public class StaticTopologyCreator {
  public static final String parentID = "static_rack";

  public static Topology createTopology(String id, Collection<String> servers) {
    GenericNode root;
    List<Node> children;

    children = new ArrayList<>(servers.size());
    for (String serverID : servers) {
      children.add(new GenericNode(NodeClass.server, serverID));
    }
    root = new GenericNode(NodeClass.rack, parentID, children);
    return Topology.fromRoot(id, root);
  }

  public static void main(String[] args) {
    System.out.println(createTopology(null, ImmutableList.copyOf(args)));
  }
}
