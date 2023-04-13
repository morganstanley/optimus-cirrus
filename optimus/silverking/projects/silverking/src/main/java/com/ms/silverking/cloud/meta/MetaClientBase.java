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

import java.io.IOException;

import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaClientBase<T extends MetaPathsBase> extends MetaClientCore {
  protected final T metaPaths;

  public MetaClientBase(T metaPaths, ZooKeeperConfig zkConfig) throws IOException, KeeperException {
    super(zkConfig);
    this.metaPaths = metaPaths;
  }

  private static Logger log = LoggerFactory.getLogger(MetaClientBase.class);

  public T getMetaPaths() {
    return metaPaths;
  }

  public void ensureMetaPathsExist() throws KeeperException {
    try {
      getZooKeeper().createAllNodes(metaPaths.getPathList());
    } catch (RuntimeException re) {
      log.info("Failed to create meta paths {}", metaPaths.getPathList());
      throw re;
    }
  }

  public void ensurePathExists(String path, boolean createIfMissing) throws KeeperException {
    if (!getZooKeeper().exists(path)) {
      if (createIfMissing) {
        try {
          getZooKeeper().create(path);
        } catch (KeeperException ke) {
          if (!getZooKeeper().exists(path)) {
            throw new RuntimeException("Path doesn't exist and creation failed: " + path);
          }
        }
      } else {
        throw new RuntimeException("Path doesn't exist: " + path);
      }
    }
  }
}
