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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeClassAndStoragePolicyName extends NodeClassAndName {
  public NodeClassAndStoragePolicyName(NodeClass nodeClass, String storagePolicyName) {
    super(nodeClass, storagePolicyName);
  }

  private static Logger log = LoggerFactory.getLogger(NodeClassAndStoragePolicyName.class);

  public static NodeClassAndStoragePolicyName parse(String s) throws PolicyParseException {
    NodeClassAndName n;

    n = NodeClassAndName.parse(s);
    if (n.getName() != null) {
      if (n.getNodeClass() == NodeClass.server) {
        log.error("parsing: {}" , s);
        throw new PolicyParseException("name not allowed for class " + n.getNodeClass());
      }
    } else {
      if (n.getNodeClass() != NodeClass.server) {
        log.error("parsing: {}" , s);
        throw new PolicyParseException("name required for class " + n.getNodeClass());
      }
    }
    return new NodeClassAndStoragePolicyName(n.getNodeClass(), n.getName());
  }

  public NodeClass getNodeClass() {
    return super.getNodeClass();
  }

  public String getStoragePolicyName() {
    return super.getName();
  }

  @Override
  public String toString() {
    if (getStoragePolicyName() != null) {
      return getNodeClass() + ":" + getStoragePolicyName();
    } else {
      return getNodeClass().toString();
    }
  }
}
