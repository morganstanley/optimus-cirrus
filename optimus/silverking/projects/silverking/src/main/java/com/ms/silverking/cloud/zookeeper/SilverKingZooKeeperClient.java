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
package com.ms.silverking.cloud.zookeeper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.numeric.NumConversion;
import com.ms.silverking.thread.ThreadUtil;
import optimus.utils.zookeeper.NoSASLZookeeperFactory;
import optimus.utils.zookeeper.PlainSASLZookeeperFactory;
import org.apache.curator.RetryPolicy;
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZookeeperFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Extension of the raw ZooKeeper class used to simplify some interaction. */
public class SilverKingZooKeeperClient {
  private static final ConcurrentMap<ZooKeeperConfig, CuratorWithBasePath> curatorPool =
      new ConcurrentHashMap<>();
  private static final Object curatorPoolLock = new Object();
  private static final RetryPolicy defaultRetryPolicy = new ExponentialBackoffRetry(100, 10, 20000);

  private static Logger log = LoggerFactory.getLogger(SilverKingZooKeeperClient.class);

  private static CuratorWithBasePath createAndSetCurator(
      ZooKeeperConfig zkConfig, int sessionTimeoutMs, ZookeeperFactory zookeeperFactory) {
    synchronized (curatorPoolLock) {
      CuratorFramework curator;
      CuratorWithBasePath curatorWithSubPath;

      curator =
          CuratorFrameworkFactory.builder()
              .zookeeperFactory(zookeeperFactory)
              .ensembleProvider(new ImmutableEnsembleProvider(zkConfig.getConnectString()))
              .retryPolicy(defaultRetryPolicy)
              .dontUseContainerParents()
              .sessionTimeoutMs(sessionTimeoutMs)
              .build();

      if (curator.getState() == CuratorFrameworkState.LATENT) {
        curator.start();
      }
      curatorWithSubPath =
          new CuratorWithBasePath() {
            @Override
            public String getResolvedPath(String path) {
              return path;
            }

            @Override
            public CuratorFramework getCurator() {
              return curator;
            }
          };

      curatorPool.put(zkConfig, curatorWithSubPath);
      return curatorWithSubPath;
    }
  }

  public static void setCurator(ZooKeeperConfig zkConfig, CuratorWithBasePath curatorWithBasePath)
      throws IllegalArgumentException {
    synchronized (curatorPoolLock) {
      String chroot, currConnStr, base;

      chroot = zkConfig.getChroot();
      currConnStr =
          curatorWithBasePath.getCurator().getZookeeperClient().getCurrentConnectionString();
      base = currConnStr.contains("/") ? currConnStr.substring(currConnStr.indexOf('/')) : "";
      if (!chroot.startsWith(base)) {
        throw new IllegalArgumentException(
            "Given curator is using ["
                + currConnStr
                + "] which cannot work with chroot ["
                + chroot
                + "]");
      }
      curatorPool.put(zkConfig, curatorWithBasePath);
      log.info("[ {} ] is bound with curator [ {} ]", zkConfig, curatorWithBasePath);
    }
  }

  private final CuratorWithBasePath curatorWithBasePath;
  // TODO (OPTIMUS-0000): various other classes consume this at present in order to share ZK
  // connections where the ZK Config matches.
  // However it would be better to remove this in future and let Curator take care of the connection
  // state management
  // for us.
  private final ZooKeeperConfig zkConfig;

  private static final int processRunnerThreads = 6;

  private static final int displayMissingIntervalSeconds = 20;
  private static final int reissueIntervalSeconds = 60;
  private static final int timeoutSeconds = 3 * 60 + 10;

  private static final int ANY_VERSION = -1;

  public static final int AUTO_VERSION_FIELD_SIZE = 10;

  public static class ImmutableEnsembleProvider extends FixedEnsembleProvider {
    public ImmutableEnsembleProvider(String connectionString) {
      super(connectionString, false);
    }

    @Override
    public void setConnectionString(String connectionString) {
      log.info("Ignoring setConnectionString {}", connectionString);
    }
  }

  private static boolean isSASLDisabled() {
    return "false".equalsIgnoreCase(System.getProperty(ZKClientConfig.ENABLE_CLIENT_SASL_KEY));
  }

  private static ZookeeperFactory zookeeperFactory(ZooKeeperConfig zkConfig) {
    if (isSASLDisabled() || zkConfig.getProid().isEmpty()) {
      return new NoSASLZookeeperFactory();
    }

    return new PlainSASLZookeeperFactory(zkConfig.getProid());
  }

  public SilverKingZooKeeperClient(ZooKeeperConfig zkConfig, int sessionTimeoutMs) {
    CuratorWithBasePath sharedInstance;

    sharedInstance = curatorPool.get(zkConfig);
    if (sharedInstance == null) {
      this.curatorWithBasePath =
          createAndSetCurator(zkConfig, sessionTimeoutMs, zookeeperFactory(zkConfig));
    } else {
      this.curatorWithBasePath = sharedInstance;
    }
    this.zkConfig = zkConfig;
  }

  public ZooKeeperConfig getZKConfig() {
    return zkConfig;
  }

  public CuratorWithBasePath getCurator() {
    return curatorWithBasePath;
  }

  public CuratorFrameworkState getState() {
    return curatorWithBasePath.getCurator().getState();
  }

  ///////////////
  // operations

  public void setEphemeralInteger(String path, int data) throws KeeperException {
    setEphemeral(path, NumConversion.intToBytes(data));
  }

  public void setEphemeral(String path, byte[] data) throws KeeperException {
    try {
      curatorWithBasePath
          .getCurator()
          .create()
          .orSetData()
          .withMode(CreateMode.EPHEMERAL)
          .forPath(curatorWithBasePath.getResolvedPath(path), data);
    } catch (Exception ex) {
      throw KeeperException.forMethod("setEphemeral", ex);
    }
  }

  public Stat set(String path, byte[] data, int version) throws KeeperException {
    try {
      return curatorWithBasePath
          .getCurator()
          .setData()
          .withVersion(version)
          .forPath(curatorWithBasePath.getResolvedPath(path), data);
    } catch (InterruptedException ie) {
      throw new RuntimeException("panic");
    } catch (Exception ex) {
      throw KeeperException.forMethod("set", ex);
    }
  }

  public Stat set(String path, byte[] data) throws KeeperException {
    try {
      return curatorWithBasePath
          .getCurator()
          .setData()
          .forPath(curatorWithBasePath.getResolvedPath(path), data);
    } catch (Exception ex) {
      throw KeeperException.forMethod("set", ex);
    }
  }

  public Stat setString(String path, String data) throws KeeperException {
    try {
      return set(path, data.getBytes());
    } catch (Exception ex) {
      throw KeeperException.forMethod("setString", ex);
    }
  }

  public String create(String path, byte[] data, CreateMode createMode) throws KeeperException {
    try {
      return curatorWithBasePath
          .getCurator()
          .create()
          .withMode(createMode)
          .forPath(curatorWithBasePath.getResolvedPath(path), data);
    } catch (InterruptedException ie) {
      throw new RuntimeException("panic");
    } catch (Exception ex) {
      throw KeeperException.forMethod("create", ex);
    }
  }

  public String create(String path, byte[] data) throws KeeperException {
    return create(path, data, CreateMode.PERSISTENT);
  }

  public String create(String path) throws KeeperException {
    return create(path, "".getBytes());
  }

  public String ensureCreated(String path) throws KeeperException {
    if (!exists(path)) {
      return create(path, "".getBytes());
    } else {
      return path;
    }
  }

  public void delete(String path) throws KeeperException {
    try {
      curatorWithBasePath.getCurator().delete().forPath(curatorWithBasePath.getResolvedPath(path));
    } catch (InterruptedException ie) {
      throw new RuntimeException("panic");
    } catch (Exception ex) {
      throw KeeperException.forMethod("delete", ex);
    }
  }

  public void deleteVersionedChildren(String parent, int numVersionsToRetain)
      throws KeeperException {
    List<String> children;

    children = new ArrayList<>(getChildren(parent));
    Collections.sort(children); // lexical sorting is sufficient
    for (int i = children.size() - (1 + numVersionsToRetain); i >= 0; i--) {
      String child;

      child = parent + "/" + children.get(i);
      log.debug("Deleting {}", child);
      delete(child);
    }
  }

  public void waitForCompletion(List<? extends CompletableFuture> ops)
      throws InterruptedException, ExecutionException {
    for (CompletableFuture op : ops) {
      op.get();
    }
  }

  public CompletableFuture<Void> deleteAsync(String path) throws KeeperException {
    CompletableFuture<Void> op;

    try {
      op = new CompletableFuture<>();
      curatorWithBasePath
          .getCurator()
          .delete()
          .withVersion(ANY_VERSION)
          .inBackground((CuratorFramework client, CuratorEvent event) -> op.complete(null))
          .forPath(curatorWithBasePath.getResolvedPath(path));
      return op;
    } catch (Exception ex) {
      throw KeeperException.forMethod("deleteAsync", ex);
    }
  }

  public void deleteRecursive(String path) throws KeeperException {
    deleteChildrenRecursive(path);
    delete(path);
  }

  private void deleteChildrenRecursive(String path) throws KeeperException {
    List<String> children;
    List<CompletableFuture<Void>> ops;

    children = getChildren(path);
    for (String child : children) {
      deleteChildrenRecursive(path + "/" + child);
    }
    ops = new ArrayList<>(children.size());
    for (String child : children) {
      ops.add(deleteAsync(path + "/" + child));
    }
    try {
      waitForCompletion(ops);
    } catch (InterruptedException | ExecutionException ex) {
      throw KeeperException.forMethod("deleteChildrenRecursive::waitForCompletion", ex);
    }
  }

  public void createInt(String path, int val) throws KeeperException {
    create(path, NumConversion.intToBytes(val));
  }

  public int getInt(String path) throws KeeperException {
    return getInt(path, null);
  }

  public int getInt(String path, Watcher watcher) throws KeeperException {
    byte[] data;

    try {
      data =
          curatorWithBasePath
              .getCurator()
              .getData()
              .usingWatcher(watcher)
              .forPath(curatorWithBasePath.getResolvedPath(path));
    } catch (InterruptedException ie) {
      throw new RuntimeException("panic");
    } catch (Exception ex) {
      throw KeeperException.forMethod("getInt", ex);
    }

    return NumConversion.bytesToInt(data);
  }

  public double getDouble(String path) throws KeeperException {
    byte[] data;
    try {
      data =
          curatorWithBasePath
              .getCurator()
              .getData()
              .forPath(curatorWithBasePath.getResolvedPath(path));
    } catch (InterruptedException ie) {
      throw new RuntimeException("panic");
    } catch (Exception ex) {
      throw KeeperException.forMethod("getDouble", ex);
    }

    return NumConversion.bytesToDouble(data);
  }

  public String createString(String path, String val, CreateMode createMode)
      throws KeeperException {
    return create(path, val.getBytes(), createMode);
  }

  public void createString(String path, String val) throws KeeperException {
    create(path, val.getBytes());
  }

  public Map<String, String> getStrings(Set<String> paths) throws KeeperException {
    return convertToStringData(getByteArrays(paths));
  }

  public Map<String, Integer> getInts(Set<String> paths, Watcher watcher) throws KeeperException {
    return convertToIntData(getByteArrays(paths, watcher));
  }

  private Map<String, byte[]> syncGetPathsData(Set<String> paths, Watcher watcher)
      throws KeeperException {
    Map<String, byte[]> dataMap = new HashMap<>();
    for (String path : paths) {
      try {
        byte[] data =
            curatorWithBasePath
                .getCurator()
                .getData()
                .usingWatcher(watcher)
                .forPath(curatorWithBasePath.getResolvedPath(path));
        if (data != null) {
          log.info("Sync zk get success: {} ", path);
          dataMap.put(path, data);
        } else {
          log.error("", new Exception("No data has been found in path [" + path + "]"));
        }
      } catch (Exception ex) {
        throw KeeperException.forMethod("syncGetPathsData", ex);
      }
    }
    return dataMap;
  }

  public Map<String, byte[]> getByteArrays(Set<String> paths) throws KeeperException {
    return syncGetPathsData(paths, null);
  }

  public Map<String, byte[]> getByteArrays(Set<String> paths, Watcher watcher)
      throws KeeperException {
    return syncGetPathsData(paths, watcher);
  }

  public Map<String, byte[]> getByteArrays(
      String basePath, Set<String> children, Watcher watcher, CancelableObserver observer)
      throws KeeperException {
    ImmutableSet.Builder<String> childrenPaths;

    childrenPaths = ImmutableSet.builder();
    for (String child : children) {
      childrenPaths.add(basePath + "/" + child);
    }

    return stripBasePathFromKeys(syncGetPathsData(childrenPaths.build(), watcher), basePath);
  }

  private Map<String, byte[]> stripBasePathFromKeys(Map<String, byte[]> rawMap, String basePath) {
    ImmutableMap.Builder<String, byte[]> strippedMap;

    strippedMap = ImmutableMap.builder();
    for (Map.Entry<String, byte[]> entry : rawMap.entrySet()) {
      strippedMap.put(stripBasePath(entry.getKey(), basePath), entry.getValue());
    }
    return strippedMap.build();
  }

  private String stripBasePath(String rawPath, String basePath) {
    if (rawPath.startsWith(basePath + "/")) {
      return rawPath.substring(basePath.length() + 1);
    } else {
      throw new RuntimeException(String.format("%s does not start with %s", rawPath, basePath));
    }
  }

  //////////////////////////////////////////

  private Map<String, String> convertToStringData(Map<String, byte[]> data) {
    Map<String, String> stringData;

    stringData = new HashMap<>();
    for (Map.Entry<String, byte[]> e : data.entrySet()) {
      stringData.put(e.getKey(), new String(e.getValue()));
    }
    return stringData;
  }

  private Map<String, Integer> convertToIntData(Map<String, byte[]> data) {
    Map<String, Integer> intData;

    intData = new HashMap<>();
    for (Map.Entry<String, byte[]> e : data.entrySet()) {
      intData.put(e.getKey(), NumConversion.bytesToInt(e.getValue()));
    }
    return intData;
  }

  public byte[] getByteArray(String path) throws KeeperException {
    return getByteArray(path, null);
  }

  public byte[] getByteArray(String path, Watcher watcher) throws KeeperException {
    return getByteArray(path, watcher, new Stat());
  }

  public byte[] getByteArray(String path, Watcher watcher, Stat stat) throws KeeperException {
    try {
      return curatorWithBasePath
          .getCurator()
          .getData()
          .storingStatIn(stat)
          .usingWatcher(watcher)
          .forPath(curatorWithBasePath.getResolvedPath(path));
    } catch (InterruptedException ie) {
      throw new RuntimeException("panic");
    } catch (Exception ex) {
      throw KeeperException.forMethod("getByteArray", ex);
    }
  }

  public String getString(String path) throws KeeperException {
    byte[] data;

    data = getByteArray(path);
    return new String(data);
  }

  public String getString(String path, Watcher watcher, Stat stat) throws KeeperException {
    byte[] data;

    data = getByteArray(path, watcher, stat);
    return new String(data);
  }

  public int getInteger(String path) throws KeeperException {
    byte[] data;

    data = getByteArray(path);
    return NumConversion.bytesToInt(data);
  }

  public boolean exists(String path) throws KeeperException {
    try {
      return curatorWithBasePath
              .getCurator()
              .checkExists()
              .forPath(curatorWithBasePath.getResolvedPath(path))
          != null;
    } catch (InterruptedException ie) {
      throw new RuntimeException("panic");
    } catch (Exception ex) {
      throw KeeperException.forMethod("exists", ex);
    }
  }

  public List<String> getChildren(String path, Watcher watcher) throws KeeperException {
    try {
      return curatorWithBasePath
          .getCurator()
          .getChildren()
          .usingWatcher(watcher)
          .forPath(curatorWithBasePath.getResolvedPath(path));
    } catch (InterruptedException ie) {
      throw new RuntimeException("panic");
    } catch (Exception ex) {
      throw KeeperException.forMethod("getChildren", ex);
    }
  }

  public List<String> getChildren(String path) throws KeeperException {
    try {
      return curatorWithBasePath
          .getCurator()
          .getChildren()
          .forPath(curatorWithBasePath.getResolvedPath(path));
    } catch (InterruptedException ie) {
      throw new RuntimeException("panic");
    } catch (Exception ex) {
      throw KeeperException.forMethod("getChildren", ex);
    }
  }

  public List<String> getChildrenPriorTo(String path, long zxid) throws KeeperException {
    List<String> allChildren;
    List<String> priorChildren;

    allChildren = getChildren(path);
    priorChildren = new ArrayList<>();
    for (String child : allChildren) {
      if (getStat(path + "/" + child).getMzxid() < zxid) {
        priorChildren.add(child);
      }
    }
    return priorChildren;
  }

  public Stat getStat(String path) throws KeeperException {
    try {
      Stat stat;

      stat = new Stat();
      curatorWithBasePath
          .getCurator()
          .getData()
          .storingStatIn(stat)
          .forPath(curatorWithBasePath.getResolvedPath(path));
      return stat;
    } catch (InterruptedException ie) {
      throw new RuntimeException("panic");
    } catch (Exception ex) {
      throw KeeperException.forMethod("getStat", ex);
    }
  }

  public void close() {
    curatorWithBasePath.getCurator().close();
  }

  public static SilverKingZooKeeperClient getZooKeeperWithRetries(
      ZooKeeperConfig zkConfig, int sessionTimeout, int connectAttempts) {
    SilverKingZooKeeperClient _zk;
    int curAttempt;

    _zk = null;
    curAttempt = 1;
    do {
      try {
        _zk = new SilverKingZooKeeperClient(zkConfig, sessionTimeout);
        _zk.curatorWithBasePath
            .getCurator()
            .blockUntilConnected(sessionTimeout, TimeUnit.MILLISECONDS);
        break;
      } catch (InterruptedException ie) {
        log.error("Interrupted while connecting to ZooKeeper", ie);
      } catch (Exception e) {
        if (curAttempt >= connectAttempts) {
          log.error("Exception while connecting to ZooKeeper, all retries exhausted", e);
          throw e;
        } else {
          log.error(
              "Caught exception while connecting to ZooKeeper, will retry after wait period", e);
        }
      }
      curAttempt++;
      ThreadUtil.randomSleep(0, (1 << curAttempt) * 1000);
    } while (curAttempt < connectAttempts);
    return _zk;
  }

  public void createAllNodes(Collection<String> paths) throws KeeperException {
    for (String path : paths) {
      if (path != null) {
        createAllNodes(path);
      }
    }
  }

  public void createAllParentNodes(String path) throws KeeperException {
    int index;

    if (!path.startsWith("/")) {
      throw new RuntimeException("Bad path:" + path);
    }
    index = path.lastIndexOf('/');
    if (index >= 0) {
      // index < 0 indicates root level; no creation necessary
      createAllNodes(path.substring(0, index));
    }
  }

  public void createAllNodes(String path) throws KeeperException {
    try {
      curatorWithBasePath
          .getCurator()
          .create()
          .orSetData()
          .creatingParentsIfNeeded()
          .forPath(curatorWithBasePath.getResolvedPath(path), "".getBytes());
    } catch (Exception ex) {
      throw KeeperException.forMethod("createAllNodes", ex);
    }
  }

  public static String padVersion(long version) {
    return Strings.padStart(Long.toString(version), AUTO_VERSION_FIELD_SIZE, '0');
  }

  public static String padVersionPath(String path, long version) {
    return path + "/" + padVersion(version);
  }

  public long getCreationTime(String path) throws KeeperException {
    return getStat(path).getCtime();
  }

  private List<Long> getSortedVersions(String path) throws KeeperException {
    List<String> children;
    List<Long> currentVersions;

    children = getChildren(path);
    if (children.size() == 0) {
      return new ArrayList<>(0);
    } else {
      currentVersions = new ArrayList<>(children.size());
      for (String child : children) {
        currentVersions.add(Long.parseLong(child));
      }
      Collections.sort(currentVersions);
      return currentVersions;
    }
  }

  public long getLatestVersion(String path) throws KeeperException {
    List<Long> sorted;

    sorted = getSortedVersions(path);
    return sorted.isEmpty() ? -1 : sorted.get(sorted.size() - 1);
  }

  public long getLeastVersion(String path) throws KeeperException {
    List<Long> sorted;

    sorted = getSortedVersions(path);
    return sorted.isEmpty() ? -1 : sorted.get(0);
  }

  public long getLatestVersionFromPath(String path) {
    int index;

    index = path.lastIndexOf('/');
    return Long.parseLong(path.substring(index + 1));
  }

  public String getLatestVersionPath(String path) throws KeeperException {
    return path + "/" + Strings.padStart(Long.toString(getLatestVersion(path)), 10, '0');
  }

  public String getLeastVersionPath(String path) throws KeeperException {
    return path + "/" + Strings.padStart(Long.toString(getLeastVersion(path)), 10, '0');
  }

  public long getVersionPriorTo(String path, long zxid) throws KeeperException {
    List<String> children;
    List<Long> currentVersions;

    children = getChildrenPriorTo(path, zxid);
    if (children.size() == 0) {
      return -1;
    } else {
      currentVersions = new ArrayList<>(children.size());
      for (String child : children) {
        currentVersions.add(Long.parseLong(child));
      }
      Collections.sort(currentVersions);
      return currentVersions.get(currentVersions.size() - 1);
    }
  }

  public static String parentPath(String childPath) {
    int lastSlash;

    lastSlash = childPath.lastIndexOf('/');
    if (lastSlash < 0) {
      return null;
    } else {
      return childPath.substring(0, lastSlash);
    }
  }

  public static class KeeperException extends Exception {
    private KeeperException(String message, Exception cause) {
      super(message, cause);
    }

    public static KeeperException forMethod(String method, Exception cause) {
      return new KeeperException("Exception during ZK op " + method, cause);
    }
  }
}
