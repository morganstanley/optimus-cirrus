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
package com.ms.silverking.cloud.management;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import com.ms.silverking.cloud.meta.MetaClientBase;
import com.ms.silverking.cloud.meta.MetaPathsBase;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MetaToolModuleBase<T, M extends MetaPathsBase> implements MetaToolModule<T> {
  private static final int defaultMode = 1;
  private static final String defaultModeName = "mode";
  protected final MetaClientBase<M> mc;
  protected final SilverKingZooKeeperClient zk;
  protected final M paths;
  protected final String base;
  protected final Logger log = LoggerFactory.getLogger(MetaToolModuleBase.class);
  protected String base2;
  protected int mode;

  public MetaToolModuleBase(MetaClientBase<M> mc, String base) throws KeeperException {
    this.mc = mc;
    this.zk = mc.getZooKeeper();
    this.mode = defaultMode;
    paths = mc.getMetaPaths();
    mc.ensureMetaPathsExist();
    this.base = base;
    if (base == null) {
      log.info("base is null");
    }
  }

  public MetaToolModuleBase(MetaClientBase<M> mc, String base, String base2)
      throws KeeperException {
    this.mc = mc;
    this.zk = mc.getZooKeeper();
    this.mode =
        zk.exists(MetaPathsBase.modeZKPath)
            ? (int) getMode(this.zk).get(defaultModeName)
            : defaultMode;
    log.info("mode : {}", this.mode);
    paths = mc.getMetaPaths();
    mc.ensureMetaPathsExist();
    this.base = base;
    this.base2 = base2;
    if (base == null) {
      log.info("base is null");
    }
    if (base2 == null) {
      log.info("base2 is null");
    }
  }

  public String getBase() {
    return base;
  }

  public String getBase2() {
    return base2;
  }

  protected String getVersionPath(String name, long version) {
    return SilverKingZooKeeperClient.padVersionPath(base + "/" + name, version);
  }

  protected String getVersionPath2(String name, long version) {
    return SilverKingZooKeeperClient.padVersionPath(base2 + "/" + name, version);
  }

  protected String getVersionPath(long version) {
    return SilverKingZooKeeperClient.padVersionPath(base, version);
  }

  protected String getVersionPath2(long version) {
    return SilverKingZooKeeperClient.padVersionPath(base2, version);
  }

  protected String getVBase(String name, long version) throws KeeperException {
    String vBase;
    if (mode == 1) {
      vBase = getVersionPath(name, version);
      mc.ensurePathExists(vBase, false);
    } else {
      if (zk.exists(getVersionPath2(name, version))) {
        vBase = getVersionPath2(name, version);
      } else {
        log.error("Failed to find path in serverconfig : {}", getVersionPath2(name, version));
        vBase = getVersionPath(name, version);
      }
    }
    mc.ensurePathExists(vBase, false);
    return vBase;
  }

  protected String getVBase(long version) throws KeeperException {
    String vBase;
    if (mode == 1) {
      vBase = getVersionPath(version);
    } else {
      if (zk.exists(getVersionPath2(version))) {
        vBase = getVersionPath2(version);
      } else {
        log.error("Failed to find path in serverconfig : {}", getVersionPath2(version));
        vBase = getVersionPath(version);
      }
    }
    mc.ensurePathExists(vBase, false);
    return vBase;
  }

  @Override
  public void deleteFromZK(long version)
      throws KeeperException, InterruptedException, ExecutionException {
    if (mode == 1) {
      zk.deleteRecursive(getVersionPath(version));
    } else {
      KeeperException cloudKe = null;
      KeeperException serverConfigKe = null;
      String cloudPath = getVersionPath(version);
      String serverConfigPath = getVersionPath2(version);
      try {
        zk.deleteRecursive(cloudPath);
      } catch (KeeperException ke) {
        log.error("Failed to delete from cloud path : {}", cloudPath, ke);
        cloudKe = ke;
      }

      try {
        zk.deleteRecursive(serverConfigPath);
      } catch (KeeperException ke) {
        log.error("Failed to delete from serverconfig path : {}", serverConfigKe, ke);
        serverConfigKe = ke;
      }

      if (Objects.nonNull(cloudKe) && Objects.nonNull(serverConfigKe)) {
        throw cloudKe;
      }

      if (Objects.nonNull(cloudKe)) {
        throw cloudKe;
      }

      if (Objects.nonNull(serverConfigKe)) {
        throw serverConfigKe;
      }
    }
  }

  @Override
  public long getLatestVersion() throws KeeperException {
    if (mode == 1) {
      return zk.getLatestVersion(base);
    } else {
      if (zk.getChildren(base2).size() > 0) {
        return zk.getLatestVersion(base2);
      } else {
        log.error("Failed to get latest version from serverconfig path : {}", base2);
        return zk.getLatestVersion(base);
      }
    }
  }

  public void setMode(int mode) {
    this.mode = mode;
  }

  public Map getMode(SilverKingZooKeeperClient zk) throws KeeperException {
    String modeString = zk.getString(MetaPathsBase.modeZKPath);
    Map<String, Integer> modes = new HashMap<>();
    if (modeString.matches("\\bmode\\b=\\d$")) {
      String[] modeArray = modeString.split("=");
      modes.put(modeArray[0], Integer.parseInt(modeArray[1]));
    } else {
      log.warn("wrong config data in {}", MetaPathsBase.modeZKPath);
      modes.put(defaultModeName, defaultMode);
    }
    return modes;
  }
}
