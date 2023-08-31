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
package com.ms.silverking.cloud.toporing.meta;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import com.ms.silverking.cloud.management.MetaToolModuleBase;
import com.ms.silverking.cloud.management.MetaToolOptions;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WeightsZK extends MetaToolModuleBase<WeightSpecifications, MetaPaths> {
  // Below must agree with WeightSpecifications.parse
  // FUTURE - make code common
  private static final char fieldDelimiterChar = '\t';
  private static final String fieldDelimiterString = "" + fieldDelimiterChar;
  private static final char entryDelimiterChar = '\n';
  private static final String entryDelimiterString = "" + entryDelimiterChar;

  private static Logger log = LoggerFactory.getLogger(WeightsZK.class);

  public WeightsZK(MetaClient mc) throws KeeperException {
    super(mc, mc.getMetaPaths().getWeightsPath(), mc.getMetaPaths().getWeightsServerPath());
  }

  @Override
  public WeightSpecifications readFromFile(File file, long version) throws IOException {
    return (WeightSpecifications) new WeightSpecifications(version).parse(file);
  }

  @Override
  public WeightSpecifications readFromZK(long version, MetaToolOptions options)
      throws KeeperException {
    String vBase;
    ByteArrayInputStream inStream;
    vBase = getVBase(version);
    inStream = new ByteArrayInputStream(zk.getString(vBase).getBytes());
    try {
      if (mode == 2 && vBase.contains("cloud")) {
        log.error("Failed to read from serverconfig path : {}", base2);
        log.info("Falling back to cloud path : {}", base);
      }
      log.info("read source path : {}", vBase);
      return (WeightSpecifications) new WeightSpecifications(version).parse(inStream);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    /*
    String  vBase;
    List<String>    nodes;
    Map<String, Double> nodeWeights;

    nodeWeights = new HashMap<>();
    vBase = getVBase(version);
    nodes = zk.getChildren(vBase);
    for (String node : nodes) {
        double  weight;

        weight = zk.getDouble(vBase +"/"+ node);
        nodeWeights.put(node, weight);
    }
    return new WeightSpecifications(version, nodeWeights);
    */
  }

  @Override
  public void writeToFile(File file, WeightSpecifications instance) throws IOException {
    throw new RuntimeException("writeToFile not implemented for WeightSpecifications");
  }

  @Override
  public String writeToZK(WeightSpecifications weightSpecs, MetaToolOptions options)
      throws IOException, KeeperException {
    if (mode == 1) {
      return writeToCloudZK(weightSpecs, options);
    } else {
      KeeperException cloudKe = null;
      KeeperException serverConfigKe = null;
      try {
        writeToCloudZK(weightSpecs, options);
      } catch (KeeperException ke) {
        log.error("Failed to write to cloud path : {} ", getBase(), ke);
        cloudKe = ke;
      }
      try {
        writeToServerConfigZK(weightSpecs, options);
      } catch (KeeperException ke) {
        log.error("Failed to write to serverconfig path : {}", getBase2(), ke);
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
      return null;
    }
  }

  private String writeToCloudZK(WeightSpecifications weightSpecs, MetaToolOptions options)
      throws KeeperException {
    String vBaseCloud;
    StringBuilder sb;

    sb = new StringBuilder();
    for (Map.Entry<String, Double> nodeWeight : weightSpecs.getNodeWeights()) {
      sb.append(
          nodeWeight.getKey() + fieldDelimiterChar + nodeWeight.getValue() + entryDelimiterChar);
    }
    // trim trailing entryDelimiter
    if (sb.length() > 0) {
      sb.deleteCharAt(sb.length() - 1);
    }
    log.info("writing to base path : {}", base);
    vBaseCloud = zk.createString(base + "/", sb.toString(), CreateMode.PERSISTENT_SEQUENTIAL);
    return null;
  }

  private String writeToServerConfigZK(WeightSpecifications weightSpecs, MetaToolOptions options)
      throws KeeperException {
    String vBaseServerConfig;
    StringBuilder sb;

    sb = new StringBuilder();
    for (Map.Entry<String, Double> nodeWeight : weightSpecs.getNodeWeights()) {
      sb.append(
          nodeWeight.getKey() + fieldDelimiterChar + nodeWeight.getValue() + entryDelimiterChar);
    }
    // trim trailing entryDelimiter
    if (sb.length() > 0) {
      sb.deleteCharAt(sb.length() - 1);
    }
    log.info("writing to base path : {}", base2);
    vBaseServerConfig =
        zk.createString(base2 + "/", sb.toString(), CreateMode.PERSISTENT_SEQUENTIAL);
    return null;
  }
}
