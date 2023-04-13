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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import com.ms.silverking.cloud.management.MetaToolModuleBase;
import com.ms.silverking.cloud.management.MetaToolOptions;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.io.FileUtil;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClassVarsZK extends MetaToolModuleBase<ClassVars, MetaPaths> {

  private static Logger log = LoggerFactory.getLogger(ClassVarsZK.class);

  public ClassVarsZK(MetaClient mc) throws KeeperException {
    super(mc, mc.getMetaPaths().getClassVarsBasePath(), mc.getMetaPaths().getClassVarsServerBasePath());
  }

  @Override
  public ClassVars readFromFile(File file, long version) throws IOException {
    return ClassVars.parse(file, version);
  }

  public ClassVars getClassVars(String classVarsName) throws KeeperException {
    MetaToolOptions mto;

    mto = new MetaToolOptions();
    mto.name = classVarsName;
    return readFromZK(-1, mto);
  }

  @Override
  public ClassVars readFromZK(long version, MetaToolOptions options) throws KeeperException {
    String base;
    String base2;
    String curBase;
    String vBase;

    base = getBase() + "/" + options.name;
    base2 = getBase2() + "/" + options.name;
    if (mode == 1) {
      curBase = base;
    } else {
      if (zk.exists(base2)) {
        curBase = base2;
      } else {
        curBase = base;
      }
    }

    if (version < 0) {
      version = mc.getZooKeeper().getLatestVersion(curBase);
    }
    vBase = getVBase(options.name, version);
    if (mode == 2 && vBase.contains("cloud")) {
      log.error("Failed to read from serverconfig path : {}", base2);
      log.info("Falling back to cloud path : {}", base);
    }
    log.info("read source path : {}", vBase);
    return ClassVars.parse(zk.getString(vBase), version);
  }

  @Override
  public void writeToFile(File file, ClassVars instance) throws IOException {

    FileUtil.writeToFile(file, instance.toString());

  }

  @Override
  public String writeToZK(ClassVars classVars, MetaToolOptions options) throws IOException, KeeperException {
    return writeToZK(classVars, options.name);
  }

  public String writeToZK(ClassVars classVars, String name) throws KeeperException {

    if (mode == 1) {
      return writeToCloudZK(classVars, name);
    } else {
      String cloudPath = null;
      String serverConfig = null;
      KeeperException cloudKe = null;
      KeeperException serverConfigKe = null;
      try {
        cloudPath = writeToCloudZK(classVars, name);
      } catch (KeeperException ke) {
        log.error("Failed to write to cloud path : {}/{}  {}", getBase() , name, ke);
        cloudKe = ke;
      }
      try {
        serverConfig = writeToServerConfigZK(classVars, name);
      } catch (KeeperException ke) {
        log.error("Failed to write to serverconfig path {}/{} {} ", getBase2() , name, ke);
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

      return cloudPath;
    }
  }

  private String writeToServerConfigZK(ClassVars classVars, String name) throws KeeperException {
    String path;
    String classVarsName2 = base2 + "/" + name;

    if (zk.exists(classVarsName2)) {
      List<String> versions = zk.getChildren(classVarsName2);
      for (String ver : versions) {
        zk.delete(classVarsName2 + "/" + ver);
      }
      zk.delete(classVarsName2);
    }
    log.info("writing to path : {}", classVarsName2);
    zk.createString(classVarsName2, classVars.toString(), CreateMode.PERSISTENT);
    path = zk.createString(classVarsName2 + "/", classVars.toString(), CreateMode.PERSISTENT_SEQUENTIAL);
    log.info("write path: {}", path);
    return path;
  }

  private String writeToCloudZK(ClassVars classVars, String name) throws KeeperException {
    String path;
    String classVarsName = base + "/" + name;

    if (zk.exists(classVarsName)) {
      List<String> versions = zk.getChildren(classVarsName);
      for (String ver : versions) {
        zk.delete(classVarsName + "/" + ver);
      }
      zk.delete(classVarsName);
    }
    log.info("writing to path : {}", classVarsName);
    zk.createString(classVarsName, classVars.toString(), CreateMode.PERSISTENT);
    path = zk.createString(classVarsName + "/", classVars.toString(), CreateMode.PERSISTENT_SEQUENTIAL);

    return path;
  }

  public void deleteFromZK(long version, String name) throws KeeperException, InterruptedException, ExecutionException {
    if (mode == 1) {
      zk.deleteRecursive(getVersionPath(name, version));
    } else {
      KeeperException cloudKe = null;
      KeeperException serverConfigKe = null;

      String cloudPath = getVersionPath(name, version);
      String serverConfigPath = getVersionPath2(name, version);
      try {
        zk.deleteRecursive(cloudPath);
      } catch (KeeperException ke) {
        log.error("Failed to delete from cloud path : {}", cloudPath, ke);
        cloudKe = ke;
      }

      try {
        zk.deleteRecursive(serverConfigPath);
      } catch (KeeperException ke) {
        log.error("Failed to delete from serverconfig path : {}", serverConfigPath, ke);
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

  public long getLatestVersion(String name) throws KeeperException {
    if (mode == 1) {
      return zk.getLatestVersion(base + "/" + name);
    } else {
      if (zk.exists(base2 + "/" + name)) {
        return zk.getLatestVersion(base2 + "/" + name);
      } else {
        log.error("Failed to get latest version from serverconfig path : {}/{}", base2 , name);
        return zk.getLatestVersion(base + "/" + name);
      }
    }
  }

}
