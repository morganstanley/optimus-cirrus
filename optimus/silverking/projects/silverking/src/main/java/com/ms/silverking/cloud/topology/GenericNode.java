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
import java.util.List;

public class GenericNode extends Node {
  private final String id;

  public GenericNode(NodeClass nodeClass, String id, List<Node> children) {
    super(nodeClass, id.hashCode(), children);
    this.id = id;
  }

  public GenericNode(NodeClass nodeClass, String id) {
    this(nodeClass, id, new ArrayList<Node>(0));
  }

  public static GenericNode create(String def, List<Node> children) throws TopologyParseException {
    String[] tokens;
    NodeClass nodeClass;
    String id;

    tokens = def.split("\\:");
    if (tokens.length != 2) {
      throw new TopologyParseException("Invalid def: " + def);
    }
    nodeClass = NodeClass.forName(tokens[0]);
    id = tokens[1];
    return new GenericNode(nodeClass, id, children);
  }

  @Override
  public String getIDString() {
    return id;
  }
}
