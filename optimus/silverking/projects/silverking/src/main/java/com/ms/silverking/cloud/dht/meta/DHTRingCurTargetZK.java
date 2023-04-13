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

import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.numeric.NumConversion;
import com.ms.silverking.thread.ThreadUtil;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DHTRingCurTargetZK {
  private final MetaClient mc;


  public enum NodeType {Current, Target, Manager}

  private static final int retries = 4;
  private static final int retrySleepMS = 2 * 1000;
  static final int minValueLength = NumConversion.BYTES_PER_LONG * 2;

  private static Logger log = LoggerFactory.getLogger(DHTRingCurTargetZK.class);

  private static final boolean debug = true;

  public DHTRingCurTargetZK(MetaClient mc, DHTConfiguration dhtConfig) throws KeeperException {
    this(mc);
  }

  public DHTRingCurTargetZK(MetaClient mc) throws KeeperException {
    this.mc = mc;
    ensureBasePathExists();
    ensureDataNodesExist();
  }

  private void ensureBasePathExists() throws KeeperException {
    mc.ensureMetaPathsExist();
  }

  private void ensureDataNodesExist() throws KeeperException {
    mc.getZooKeeper().ensureCreated(MetaPaths.getInstanceCurRingAndVersionPairPath(mc.getDHTName()));
    mc.getZooKeeper().ensureCreated(MetaPaths.getInstanceTargetRingAndVersionPairPath(mc.getDHTName()));
  }

  public void setRingAndVersionPair(NodeType nodeType, String ringName, Pair<Long, Long> version)
      throws KeeperException {
    switch (nodeType) {
    case Current:
      setCurRingAndVersionPair(ringName, version);
      break;
    case Target:
      setTargetRingAndVersionPair(ringName, version);
      break;
    case Manager:
      setManagerRingAndVersionPair(ringName, version);
      break;
    default:
      throw new RuntimeException("Panic");
    }
  }

  public Triple<String, Long, Long> getRingAndVersionPair(NodeType nodeType) throws KeeperException {
    switch (nodeType) {
    case Current:
      return getCurRingAndVersionPair();
    case Target:
      return getTargetRingAndVersionPair();
    case Manager:
      return getManagerRingAndVersionPair();
    default:
      throw new RuntimeException("Panic");
    }
  }

  public void setCurRingAndVersionPair(String ringName, Pair<Long, Long> version) throws KeeperException {
    setCurRingAndVersionPair(ringName, version.getV1(), version.getV2());
  }

  public void setCurRingAndVersionPair(String ringName, long ringConfigVersion, long configInstanceVersion)
      throws KeeperException {
    setRingAndVersionPair(ringName, ringConfigVersion, configInstanceVersion,
        MetaPaths.getInstanceCurRingAndVersionPairPath(mc.getDHTName()));
  }

  public Triple<String, Long, Long> getCurRingAndVersionPair(Stat stat) throws KeeperException {
    return getRingAndVersion(MetaPaths.getInstanceCurRingAndVersionPairPath(mc.getDHTName()), stat);
  }

  public Triple<String, Long, Long> getCurRingAndVersionPair() throws KeeperException {
    return getRingAndVersion(MetaPaths.getInstanceCurRingAndVersionPairPath(mc.getDHTName()));
  }

  public void setTargetRingAndVersionPair(String ringName, Pair<Long, Long> version) throws KeeperException {
    setTargetRingAndVersionPair(ringName, version.getV1(), version.getV2());
  }

  public void setTargetRingAndVersionPair(String ringName, long ringConfigVersion, long configInstanceVersion)
      throws KeeperException {
    setRingAndVersionPair(ringName, ringConfigVersion, configInstanceVersion,
        MetaPaths.getInstanceTargetRingAndVersionPairPath(mc.getDHTName()));
  }

  public Triple<String, Long, Long> getTargetRingAndVersionPair() throws KeeperException {
    return getRingAndVersion(MetaPaths.getInstanceTargetRingAndVersionPairPath(mc.getDHTName()));
  }

  public void setManagerRingAndVersionPair(String ringName, Pair<Long, Long> version) throws KeeperException {
    setManagerRingAndVersionPair(ringName, version.getV1(), version.getV2());
  }

  public void setManagerRingAndVersionPair(String ringName, long ringConfigVersion, long configInstanceVersion)
      throws KeeperException {
    setRingAndVersionPair(ringName, ringConfigVersion, configInstanceVersion,
        MetaPaths.getInstanceManagerRingAndVersionPairPath(mc.getDHTName()));
  }

  public Triple<String, Long, Long> getManagerRingAndVersionPair() throws KeeperException {
    return getRingAndVersion(MetaPaths.getInstanceManagerRingAndVersionPairPath(mc.getDHTName()));
  }

  private void setRingAndVersionPair(String ringName, long ringConfigVersion, long configInstanceVersion, String path)
      throws KeeperException {
    int attemptIndex;
    boolean complete;

    attemptIndex = 0;
    complete = false;
    while (!complete) {
      try {
        Stat stat;

        if (!mc.getZooKeeper().exists(path)) {
          mc.getZooKeeper().create(path);
        }
        stat = mc.getZooKeeper().set(path, nameAndVersionToBytes(ringName, ringConfigVersion, configInstanceVersion));
        if (debug) {
          log.info("{}",path);
          log.info("{}",stat);
        }
        complete = true;
      } catch (KeeperException ke) {
        log.error("Exception in DHTRingCurTargetZK.setRingAndVersion(). Attempt: {}" , attemptIndex,ke);
        if (attemptIndex >= retries) {
          throw ke;
        } else {
          ++attemptIndex;
          ThreadUtil.randomSleep(retrySleepMS, retrySleepMS << attemptIndex);
        }
      }
    }
  }

  private Triple<String, Long, Long> getRingAndVersion(String path) throws KeeperException {
    return getRingAndVersion(path, null);
  }

  private Triple<String, Long, Long> getRingAndVersion(String path, Stat stat) throws KeeperException {
    if (!mc.getZooKeeper().exists(path)) {
      return null;
    } else {
      byte[] b;

      b = mc.getZooKeeper().getByteArray(path, null, stat);
      if (b.length < minValueLength) {
        return null;
      } else {
        return bytesToNameAndVersion(b);
      }
    }
  }

  public static byte[] nameAndVersionToBytes(String ringName, long ringConfigVersion, long configInstanceVersion) {
    byte[] nb;
    byte[] b;

    nb = ringName.getBytes();
    b = new byte[nb.length + NumConversion.BYTES_PER_LONG * 2];
    System.arraycopy(nb, 0, b, 0, nb.length);
    NumConversion.longToBytes(ringConfigVersion, b, nb.length);
    NumConversion.longToBytes(configInstanceVersion, b, nb.length + NumConversion.BYTES_PER_LONG);
    return b;
  }

  public static Triple<String, Long, Long> bytesToNameAndVersion(byte[] b) {
    if (b.length < minValueLength) {
      throw new RuntimeException("b.length too small: " + b.length);
    } else {
      byte[] nb;
      long v2;
      long v3;

      nb = new byte[b.length - NumConversion.BYTES_PER_LONG * 2];
      System.arraycopy(b, 0, nb, 0, nb.length);
      v2 = NumConversion.bytesToLong(b, nb.length);
      v3 = NumConversion.bytesToLong(b, nb.length + NumConversion.BYTES_PER_LONG);
      return Triple.of(new String(nb), v2, v3);
    }
  }

  public boolean curAndTargetRingsMatch() throws KeeperException {
    return getCurRingAndVersionPair().equals(getTargetRingAndVersionPair());
  }
}
