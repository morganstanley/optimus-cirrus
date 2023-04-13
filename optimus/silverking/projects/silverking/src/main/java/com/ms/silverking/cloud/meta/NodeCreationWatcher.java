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

import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient;
import com.ms.silverking.thread.ThreadUtil;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeCreationWatcher implements Watcher {
  private final SilverKingZooKeeperClient zk;
  private final String parent;
  private final String path;
  private final NodeCreationListener listener;

  private static final int errorSleepMinMillis = 100;
  private static final int errorSleepMaxMillis = 30 * 1000;

  private static Logger log = LoggerFactory.getLogger(NodeCreationWatcher.class);

  private static final boolean debug = false;

  public NodeCreationWatcher(SilverKingZooKeeperClient zk, String path, NodeCreationListener listener) {
    this.zk = zk;
    this.path = path;
    this.listener = listener;
    parent = SilverKingZooKeeperClient.parentPath(path);
    if (!checkForExistence()) {
      setWatch();
    }
  }

  private void setWatch() {
    boolean set;

    set = false;
    while (!set) {
      try {
        if (debug) {
          log.info("Setting watch on: {}" , parent);
        }
        zk.getChildren(parent, this);
        set = true;
      } catch (Exception e) {
        log.error("",e);
      }
      if (!set) {
        log.info("Sleeping for retry...");
        ThreadUtil.randomSleep(errorSleepMinMillis, errorSleepMaxMillis);
        log.info("Retrying...");
      }
    }
  }

  @Override
  public void process(WatchedEvent event) {
    boolean connected;
    boolean exists;

    if (debug) {
      log.info("{}",event);
    }
    switch (event.getState()) {
    case SaslAuthenticated:
    case SyncConnected:
      connected = true;
      break;
    default:
      connected = false;
    }
    if (connected) {
      switch (event.getType()) {
      case NodeChildrenChanged:
        exists = checkForExistence();
        if (!exists) {
          setWatch();
        }
        break;
      default:
        break;
      }
    } else {
      setWatch();
    }
  }

  private boolean checkForExistence() {
    try {
      if (zk.exists(path)) {
        listener.nodeCreated(path);
        return true;
      } else {
        return false;
      }
    } catch (Exception e) {
      log.error("",e);
      return false;
    }
  }
}
