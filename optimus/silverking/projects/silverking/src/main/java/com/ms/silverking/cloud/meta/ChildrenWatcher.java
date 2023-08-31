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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Watchers all children under a znode for changes. */
public class ChildrenWatcher extends WatcherBase {
  private final ChildrenListener listener;
  private volatile Map<String, byte[]> childStates;

  private static Logger log = LoggerFactory.getLogger(ChildrenWatcher.class);

  public ChildrenWatcher(
      Timer timer,
      MetaClientCore metaClientCore,
      String basePath,
      ChildrenListener listener,
      long intervalMillis,
      long maxInitialSleep) {
    super(metaClientCore, timer, basePath, intervalMillis, maxInitialSleep);
    this.listener = listener;
  }

  public ChildrenWatcher(
      MetaClientCore metaClientCore,
      String basePath,
      ChildrenListener listener,
      long intervalMillis,
      long maxInitialSleep) {
    this(null, metaClientCore, basePath, listener, intervalMillis, maxInitialSleep);
  }

  public ChildrenWatcher(
      Timer timer,
      MetaClientCore metaClientCore,
      String basePath,
      ChildrenListener listener,
      long intervalMillis) {
    this(timer, metaClientCore, basePath, listener, intervalMillis, intervalMillis);
  }

  public ChildrenWatcher(
      MetaClientCore metaClientCore,
      String basePath,
      ChildrenListener listener,
      long intervalMillis) {
    this(metaClientCore, basePath, listener, intervalMillis, intervalMillis);
  }

  private Map<String, byte[]> readChildStates() throws KeeperException {
    SilverKingZooKeeperClient _zk;
    List<String> children;

    _zk = metaClientCore.getZooKeeper();
    children = _zk.getChildren(basePath, this);
    return _zk.getByteArrays(basePath, ImmutableSet.copyOf(children), this, this);
  }

  protected void _doCheck() throws KeeperException {
    Map<String, byte[]> latestChildStates;

    latestChildStates = readChildStates();
    if (!mapsAreEqual(latestChildStates, childStates)) {
      childStates = latestChildStates;
      listener.childrenChanged(basePath, latestChildStates);
    }
  }

  private void checkChildStates() {
    try {
      doCheck();
    } catch (KeeperException ke) {
      throw new RuntimeException(ke);
    }
  }

  /**
   * Can't use Guava Maps.difference() since we have byte arrays here.
   *
   * @param latestChildStates
   * @param childStates2
   * @return
   */
  private boolean mapsAreEqual(Map<String, byte[]> m1, Map<String, byte[]> m2) {
    Set<String> m1KeySet;

    m1KeySet = m1.keySet();
    if (m2 != null && m1KeySet.equals(m2.keySet())) {
      for (String key : m1KeySet) {
        if (!Arrays.equals(m1.get(key), m2.get(key))) {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }
  }

  public void connected(WatchedEvent event) {
    log.debug("connected");
    checkChildStates();
  }

  public void nodeCreated(WatchedEvent event) {
    log.debug("nodeCreated");
    checkChildStates();
  }

  public void nodeDeleted(WatchedEvent event) {
    log.info("nodeDeleted {}", event.getPath());
    checkChildStates();
  }

  public void nodeDataChanged(WatchedEvent event) {
    log.debug("nodeDataChanged ");
    checkChildStates();
  }

  public void nodeChildrenChanged(WatchedEvent event) {
    log.debug("nodeChildrenChanged");
    checkChildStates();
  }
}
