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

import com.ms.silverking.cloud.meta.NodeCreationListener;
import com.ms.silverking.cloud.meta.NodeCreationWatcher;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient;
import com.ms.silverking.numeric.NumConversion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LinkCreationWatcher implements NodeCreationListener {
  private final long child;
  private final LinkCreationListener listener;
  private final String path;
  private final SilverKingZooKeeperClient zk;

  private static Logger log = LoggerFactory.getLogger(LinkCreationWatcher.class);

  public LinkCreationWatcher(
      SilverKingZooKeeperClient zk, String basePath, long child, LinkCreationListener listener) {
    this.zk = zk;
    path = basePath + "/" + Long.toHexString(child);
    this.child = child;
    this.listener = listener;

    new NodeCreationWatcher(zk, path, this);
  }

  @Override
  public void nodeCreated(String path) {
    if (path.lastIndexOf('/') > 0) {
      long parent;

      try {
        parent = NumConversion.parseHexStringAsUnsignedLong(zk.getString(path));
      } catch (Exception e) {
        log.error("Unable to read link parent", e);
        return;
      }
      listener.linkCreated(child, parent);
    } else {
      log.info("Bogus LinkCreationWathcer.nodeCreated() path: {}", path);
    }
  }
}
