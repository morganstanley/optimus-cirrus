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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Watches a single versioned ZooKeeper path for new versions. */
public class VersionWatcher extends WatcherBase {
  private final VersionListener listener;
  private long lastNotifiedVersion;

  private static Logger log = LoggerFactory.getLogger(VersionWatcher.class);

  private static final boolean verbose = false;

  public VersionWatcher(
      MetaClientCore metaClientCore,
      String basePath,
      VersionListener listener,
      long intervalMillis,
      long maxInitialSleep) {
    super(metaClientCore, basePath, intervalMillis, maxInitialSleep);
    this.listener = listener;
    lastNotifiedVersion = Long.MIN_VALUE;
  }

  public VersionWatcher(
      MetaClientCore metaClientCore,
      String basePath,
      VersionListener listener,
      long intervalMillis) {
    this(metaClientCore, basePath, listener, intervalMillis, intervalMillis);
  }

  protected void _doCheck() throws KeeperException {
    log.debug("checkVersions");
    try {
      SilverKingZooKeeperClient _zk;
      List<String> children;
      List<Long> currentVersions;
      long mostRecentVersion;

      if (verbose) {
        log.info("VersionCheck start: {}", basePath);
      }
      _zk = metaClientCore.getZooKeeper();
      children = _zk.getChildren(basePath, this);
      currentVersions = new ArrayList<>(children.size());
      for (String child : children) {
        currentVersions.add(Long.parseLong(child));
      }
      Collections.sort(currentVersions);
      if (currentVersions.size() > 0) {
        mostRecentVersion = currentVersions.get(currentVersions.size() - 1);
      } else {
        mostRecentVersion = Long.MIN_VALUE;
      }
      if (active && mostRecentVersion > lastNotifiedVersion) {
        lastNotifiedVersion = mostRecentVersion;
        listener.newVersion(basePath, mostRecentVersion);
      }
      if (verbose) {
        log.info("VersionCheck complete: {}", basePath);
      }
    } catch (KeeperException ke) {
      log.info("*** ZooKeeper state: {}", metaClientCore.getZooKeeper().getState());
      throw ke;
    }
  }

  private void checkVersions() {
    try {
      doCheck();
    } catch (KeeperException re) {
      throw new RuntimeException(re);
    }
  }

  public void connected(WatchedEvent event) {
    log.debug("connected");
    checkVersions();
  }

  public void nodeCreated(WatchedEvent event) {
    log.debug("nodeCreated");
    checkVersions();
  }

  public void nodeDeleted(WatchedEvent event) {
    log.info("nodeDeleted {}", event.getPath());
    checkVersions();
  }

  public void nodeChildrenChanged(WatchedEvent event) {
    log.debug("nodeChildrenChanged");
    checkVersions();
  }
}
