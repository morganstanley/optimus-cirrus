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

import java.io.IOException;
import java.util.Arrays;

import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NamedRingConfigurationUtil {
  public enum Op {
    GetRingName
  };

  private static Logger log = LoggerFactory.getLogger(NamedRingConfigurationUtil.class);

  public static NamedRingConfiguration fromGridConfiguration(SKGridConfiguration gc)
      throws IOException, KeeperException {
    MetaClient _mc;
    long version;
    String ringName;
    ZooKeeperConfig zkConfig;
    com.ms.silverking.cloud.dht.meta.MetaClient dhtMC;

    dhtMC = new com.ms.silverking.cloud.dht.meta.MetaClient(gc);
    ringName = dhtMC.getDHTConfiguration().getRingName();
    zkConfig = dhtMC.getZooKeeper().getZKConfig();

    _mc =
        new MetaClient(
            new NamedRingConfiguration(ringName, RingConfiguration.emptyTemplate), zkConfig);

    // TODO (OPTIMUS-0000): version never changes
    version = _mc.getZooKeeper().getLatestVersion(MetaPaths.getRingConfigPath(ringName));
    return new NamedRingConfiguration(
        ringName, new RingConfigurationZK(_mc).readFromZK(version, null));
  }

  private static void doOp(Op op, String[] args) throws IOException, KeeperException {
    switch (op) {
      case GetRingName:
        getRingName(args[0]);
        break;
      default:
        throw new RuntimeException("panic");
    }
  }

  private static void getRingName(String gcName) throws IOException, KeeperException {
    SKGridConfiguration gc;
    NamedRingConfiguration ringConfig;

    gc = SKGridConfiguration.parseFile(gcName);
    ringConfig = fromGridConfiguration(gc);
    log.info(ringConfig.getRingName());
  }

  private static void displayUsage() {
    log.info("<Op> <args...>");
    System.exit(-1);
  }

  public static void main(String[] args) {
    try {
      if (args.length < 1) {
        displayUsage();
      } else {
        Op op;

        op = Op.valueOf(args[0]);
        if (op == null) {
          displayUsage();
        } else {
          doOp(op, Arrays.copyOfRange(args, 1, args.length));
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
