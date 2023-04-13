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
package com.ms.silverking.cloud.dht.meta;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ms.silverking.cloud.dht.client.impl.SimpleNamespaceCreator;
import com.ms.silverking.cloud.management.MetaToolModuleBase;
import com.ms.silverking.cloud.management.MetaToolOptions;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;

public class NamespaceLinksZK extends MetaToolModuleBase<Map<String, String>, MetaPaths> {
  public NamespaceLinksZK(MetaClient mc) throws KeeperException {
    super(mc, mc.getMetaPaths().getInstanceNSLinkPath());
  }

  @Override
  public Map<String, String> readFromFile(File file, long version) throws IOException {
    throw new RuntimeException("readFromFile not implemented");
  }

  @Override
  public Map<String, String> readFromZK(long version, MetaToolOptions options) throws KeeperException {
    return readFromZK();
  }

  @Override
  public void writeToFile(File file, Map<String, String> instance) throws IOException {
    throw new RuntimeException("writeToFile not implemented");
  }

  @Override
  public String writeToZK(Map<String, String> nsLinks, MetaToolOptions options) throws IOException, KeeperException {
    throw new RuntimeException("writeToZK not implemented");
  }

  public Map<String, String> readFromZK() throws KeeperException {
    String basePath;
    List<String> children;
    Map<String, String> nsLinkMap;

    basePath = mc.getMetaPaths().getInstanceNSLinkPath();
    children = zk.getChildren(basePath);
    nsLinkMap = new HashMap<>();
    for (String child : children) {
      String parent;

      parent = zk.getString(child);
      nsLinkMap.put(child, parent);
    }
    return nsLinkMap;
  }

  public void writeToZK(String child, String parent) throws IOException, KeeperException {
    String basePath;
    long childContext;
    long parentContext;

    basePath = mc.getMetaPaths().getInstanceNSLinkPath();
    zk.createString(basePath + "/" + child, parent);
    childContext = new SimpleNamespaceCreator().createNamespace(child).contextAsLong();
    parentContext = new SimpleNamespaceCreator().createNamespace(parent).contextAsLong();
    zk.createString(basePath + "/" + Long.toHexString(childContext), Long.toHexString(parentContext));
  }

  public void clearAllZK() throws IOException, KeeperException {
    String basePath;

    basePath = mc.getMetaPaths().getInstanceNSLinkPath();
    for (String child : zk.getChildren(basePath)) {
      zk.delete(basePath + "/" + child);
    }
  }
}
