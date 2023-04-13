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
import com.ms.silverking.cloud.toporing.RingTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consider deprecating
 * <p>
 * Associates a StoragePolicy definition with
 * primary and secondary servers.
 */
public class ResolvedStoragePolicy {
  private final StoragePolicy storagePolicy;
  private final List<Node> primaryServers;
  private final List<Node> secondaryServers;

  private static Logger log = LoggerFactory.getLogger(ResolvedStoragePolicy.class);

  public ResolvedStoragePolicy(StoragePolicy storagePolicy, List<Node> primaryServers, List<Node> secondaryServers) {
    this.storagePolicy = storagePolicy;
    this.primaryServers = primaryServers;
    this.secondaryServers = secondaryServers;
  }

  public StoragePolicy getStoragePolicy() {
    return storagePolicy;
  }

  public List<Node> getPrimaryServers() {
    return primaryServers;
  }

  public List<Node> getSecondaryServers() {
    return secondaryServers;
  }

  // creation

  public static ResolvedStoragePolicy resolve(StoragePolicy storagePolicy, RingTree ringTree) {
        /*
        Node    root;
        
        root = topoRing.getTopology().getRoot();
        walk(topoRing, root);
        */
    return null;
  }

  private static void walk(RingTree ringTree, Node node) {
    log.info("{} {}",node , ringTree.getMap(node.getIDString()));
    for (Node child : node.getChildren()) {
      walk(ringTree, child);
    }
  }
/*    
    public static ResolvedStoragePolicy resolve(StoragePolicy storagePolicy, Node node) {
        assert node != null;
        System.out.println(storagePolicy +" "+ node);
        if (!node.getNodeClass().equals(storagePolicy.getNodeClass())) {
            System.err.println(node.getNodeClass() +" != "+ storagePolicy.getNodeClass());
            throw new RuntimeException("Node class mismatch in policy resolution");
        } else {
            List<Node>  primaryServers;
            List<Node>  secondaryServers;
            
            primaryServers = new ArrayList<>();
            secondaryServers = new ArrayList<>();
            resolve(storagePolicy, node, primaryServers, secondaryServers);
            return new ResolvedStoragePolicy(storagePolicy, primaryServers, secondaryServers);
        }
    }
    
    private static void resolve(StoragePolicy storagePolicy, Node node,
                            List<Node> primaryServers, List<Node> secondaryServers) {
        if (!node.getNodeClass().equals(storagePolicy.getNodeClass())) {
            throw new RuntimeException("Node class mismatch in policy resolution");
        } else {
            List<Node>  children;
            Policy      primaryPolicy;
            Policy      secondaryPolicy;
            
            // what does it mean to be secondarily stored in the hierarchy

            primaryPolicy = storagePolicy.getPrimaryPolicy();
            secondaryPolicy = storagePolicy.getSecondaryPolicy();
            children = node.getChildren();
            if (children.size() < primaryPolicy.getNumber()) {
                // FUTURE - possibly throw checked exception to allow
                // higher-level code to wait when this happens
                throw new RuntimeException("children.size() < primaryPolicy.getNumber()");
            }
            if (children.size() > 0) {
                NodeClass   childrenClass;
                
                childrenClass = children.get(0).getNodeClass();
                if (!childrenClass.equals(NodeClass.server)) {
                    for (int i = 0; i < primaryPolicy.getNumber(); i++) {
                        resolve(primaryPolicy.getStoragePolicy(), children.get(i), primaryServers, secondaryServers);
                        // FUTURE - how about secondary?
                    }
                } else {
                    int i;
                    
                    i = 0;
                    while (i < primaryPolicy.getNumber()) {
                        primaryServers.add(children.get(i));
                        i++;
                    }
                    while (i < secondaryPolicy.getNumber()) {
                        secondaryServers.add(children.get(i));
                        i++;
                    }
                }
            } 
        }
    }
    */
}
