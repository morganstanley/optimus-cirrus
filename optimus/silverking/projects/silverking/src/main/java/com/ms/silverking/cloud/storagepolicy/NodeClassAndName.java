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

import com.ms.silverking.cloud.topology.NodeClass;

public class NodeClassAndName {
  private final NodeClass nodeClass;
  private final String name;

  public NodeClassAndName(NodeClass nodeClass, String name) {
    this.nodeClass = nodeClass;
    this.name = name;
  }

  public static NodeClassAndName parse(String s) throws PolicyParseException {
    String[] tokens;

    tokens = s.split(":");
    if (tokens.length > 2) {
      throw new PolicyParseException("Bad NodeClassAndName: " + s);
    } else {
      NodeClass nodeClass;
      String name;

      nodeClass = NodeClass.forName(tokens[0]);
      if (nodeClass == null) {
        throw new PolicyParseException("Bad NodeClass: " + tokens[0]);
      }
      if (tokens.length > 1) {
        name = tokens[1];
      } else {
        name = null;
      }
      return new NodeClassAndName(nodeClass, name);
    }
  }

  public NodeClass getNodeClass() {
    return nodeClass;
  }

  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return nodeClass + ":" + name;
  }
}
