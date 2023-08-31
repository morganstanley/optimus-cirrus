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
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Watches a single versioned ZooKeeper path for new versions. */
public class ValueWatcher extends WatcherBase {
  private final ValueListener listener;
  private long lastNotifiedZXID;

  private static final boolean verbose = false;

  private static Logger log = LoggerFactory.getLogger(ValueWatcher.class);

  public ValueWatcher(
      MetaClientCore metaClientCore,
      String basePath,
      ValueListener listener,
      long intervalMillis,
      long maxInitialSleep) {
    super(metaClientCore, basePath, intervalMillis, maxInitialSleep);
    this.listener = listener;
    lastNotifiedZXID = Long.MIN_VALUE;
  }

  public ValueWatcher(
      MetaClientCore metaClientCore, String basePath, ValueListener listener, long intervalMillis) {
    this(metaClientCore, basePath, listener, intervalMillis, intervalMillis);
  }

  protected void _doCheck() throws KeeperException {
    try {
      SilverKingZooKeeperClient _zk;
      byte[] value;
      Stat stat;

      if (verbose) {
        log.info("ValueCheck start: {}", basePath);
      }
      _zk = metaClientCore.getZooKeeper();
      stat = new Stat();
      value = _zk.getByteArray(basePath, this, stat);

      if (stat.getMzxid() > lastNotifiedZXID) {
        listener.newValue(basePath, value, stat);
        lastNotifiedZXID = stat.getMzxid();
      }
      if (verbose) {
        log.info("ValueCheck complete: {}", basePath);
      }
    } catch (KeeperException ke) {
      log.info("*** ZooKeeper state: {}", metaClientCore.getZooKeeper().getState());
      throw ke;
    }
  }

  private void checkValue() {
    try {
      doCheck();
    } catch (KeeperException ke) {
      throw new RuntimeException(ke);
    }
  }

  public void nodeDataChanged(WatchedEvent event) {
    log.debug("nodeDataChanged");
    checkValue();
  }

  public void connected(WatchedEvent event) {
    log.debug("connected");
    checkValue();
  }

  public void nodeCreated(WatchedEvent event) {
    log.debug("nodeCreated");
    checkValue();
  }

  public void nodeDeleted(WatchedEvent event) {
    log.info("Unexpected nodeDeleted {}", event.getPath());
  }

  public void nodeChildrenChanged(WatchedEvent event) {
    log.debug("nodeChildrenChanged");
    // checkValue();
  }
}
