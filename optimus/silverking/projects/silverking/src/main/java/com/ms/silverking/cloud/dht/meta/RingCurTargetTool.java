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

import java.io.IOException;
import java.io.PrintStream;

import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

public class RingCurTargetTool {
  private final PrintStream out;

  enum Mode {
    Read,
    Write
  };

  public RingCurTargetTool() {
    out = System.out;
  }

  public void go(SKGridConfiguration gc, RingCurTargetToolOptions options)
      throws IOException, KeeperException, CmdLineException {
    DHTRingCurTargetZK zk;
    MetaClient mc;
    MetaPaths mp;
    long latestConfigVersion;
    DHTConfiguration dhtConfig;

    mc = new MetaClient(gc);
    mp = mc.getMetaPaths();
    latestConfigVersion = mc.getZooKeeper().getLatestVersion(mp.getInstanceConfigPath());
    dhtConfig = new DHTConfigurationZK(mc).readFromZK(latestConfigVersion, null);
    zk = new DHTRingCurTargetZK(mc, dhtConfig);
    switch (options.mode) {
      case Write:
        if (options.versionPair == null) {
          throw new CmdLineException("Write requires a version");
        }
        doWrite(
            zk,
            options.nodeType,
            getRingAndVersionPair(dhtConfig.getRingName(), options.versionPair));
        break;
      case Read:
        doRead(zk, options.nodeType);
        break;
      default:
        throw new RuntimeException("Panic");
    }
  }

  private Triple<String, Long, Long> getRingAndVersionPair(String ringName, String versionPair) {
    String[] versionDefs;

    versionDefs = versionPair.split(",");
    return new Triple<>(ringName, Long.parseLong(versionDefs[0]), Long.parseLong(versionDefs[1]));
  }

  private void doWrite(
      DHTRingCurTargetZK zk,
      DHTRingCurTargetZK.NodeType nodeType,
      Triple<String, Long, Long> ringAndVersionPair)
      throws KeeperException {
    Triple<String, Long, Long> _ringAndVersionPair;
    boolean verifiedOK;

    zk.setRingAndVersionPair(nodeType, ringAndVersionPair.getHead(), ringAndVersionPair.getTail());
    out.printf("Setting Ring and VersionPair:\t%s\n", ringAndVersionPair);
    out.printf("Verifying...\n");
    _ringAndVersionPair = zk.getRingAndVersionPair(nodeType);
    verifiedOK = ringAndVersionPair.equals(_ringAndVersionPair);
    out.printf("Verification %s\n", verifiedOK ? "Successful" : "Failed");
    if (!verifiedOK) {
      throw new RuntimeException("Verification failed");
    }
  }

  private void doRead(DHTRingCurTargetZK zk, DHTRingCurTargetZK.NodeType nodeType)
      throws KeeperException {
    Triple<String, Long, Long> ringAndVersionPair;

    ringAndVersionPair = zk.getRingAndVersionPair(nodeType);
    out.printf("Ring and VersionPair:\t%s\n", ringAndVersionPair);
  }

  public static void main(String[] args) {
    try {
      CmdLineParser parser;
      RingCurTargetToolOptions options;
      RingCurTargetTool ringCurTargetTool;
      SKGridConfiguration gc;

      options = new RingCurTargetToolOptions();
      parser = new CmdLineParser(options);
      try {
        parser.parseArgument(args);
        gc = SKGridConfiguration.parseFile(options.gridConfig);
        new RingCurTargetTool().go(gc, options);
      } catch (CmdLineException cle) {
        System.err.println(cle.getMessage());
        parser.printUsage(System.err);
        System.exit(-1);
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }
}
