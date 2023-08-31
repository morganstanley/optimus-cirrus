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
import java.io.IOException;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.management.MetaToolModuleBase;
import com.ms.silverking.cloud.management.MetaToolOptions;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.io.IOUtil;
import org.apache.zookeeper.CreateMode;

public class ServerSetExtensionZK<M extends MetaPaths> extends MetaToolModuleBase<ServerSet, M> {
  private static final char delimiterChar = '\n';
  private static final String delimiterString = "" + delimiterChar;

  public ServerSetExtensionZK(MetaClientBase<M> mc, String base) throws KeeperException {
    super(mc, base);
  }

  @Override
  public ServerSet readFromFile(File file, long version) throws IOException {
    return ServerSet.parse(file, version);
  }

  @Override
  public ServerSet readFromZK(long version, MetaToolOptions options) throws KeeperException {
    String vBase;
    // List<String>    nodes;
    String[] nodes;

    vBase = getVBase(version);
    // nodes = zk.getChildren(vBase);
    nodes = zk.getString(vBase).split(delimiterString);
    return new ServerSet(ImmutableSet.copyOf(nodes), version);
  }

  @Override
  public void writeToFile(File file, ServerSet serverSet) throws IOException {
    IOUtil.writeAsLines(file, serverSet.getServers());
  }

  @Override
  public String writeToZK(ServerSet serverSet, MetaToolOptions options)
      throws IOException, KeeperException {
    String vBase;
    String zkVal;

    zkVal = CollectionUtil.toString(serverSet.getServers(), "", "", delimiterChar, "");
    vBase = zk.createString(base + "/", zkVal, CreateMode.PERSISTENT_SEQUENTIAL);
    /*
    for (String entity : serverSet.getServers()) {
        //System.out.println(vBase +"/"+ entity);
        zk.createString(vBase +"/"+ entity, entity);
    }
    */
    return null;
  }

  public ServerSet readLatestFromZK() throws KeeperException {
    return readLatestFromZK(null);
  }

  public ServerSet readLatestFromZK(MetaToolOptions options) throws KeeperException {
    long version;

    version = zk.getLatestVersion(base);
    return readFromZK(version, options);
  }
}
