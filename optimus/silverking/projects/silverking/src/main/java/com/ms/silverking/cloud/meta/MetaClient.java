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

import java.io.File;
import java.io.IOException;

import com.ms.silverking.cloud.topology.Topology;
import com.ms.silverking.cloud.topology.TopologyParser;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaClient extends MetaClientBase<MetaPaths> {
  private final CloudConfiguration cloudConfig;

  public MetaClient(CloudConfiguration cloudConfig, ZooKeeperConfig zkConfig)
      throws IOException, KeeperException {
    super(new MetaPaths(cloudConfig), zkConfig);
    this.cloudConfig = cloudConfig;
  }

  private static Logger log = LoggerFactory.getLogger(MetaClient.class);

  public CloudConfiguration getCloudConfiguration() {
    return cloudConfig;
  }

  public void test() throws Exception {
    Topology topology;

    topology = TopologyParser.parse(new File("c:/tmp/topo2.txt"));
    log.info("{}", topology);
  }

  // for unit testing only
  public static void main(String[] args) {
    try {
      if (args.length != 2) {
        System.out.println("args: <cloudConfiguration> <ensemble>");
      } else {
        MetaClient mc;
        ZooKeeperConfig zc;
        CloudConfiguration cloudConfig;

        cloudConfig = CloudConfiguration.parse(args[0]);
        zc = new ZooKeeperConfig(args[1]);
        mc = new MetaClient(cloudConfig, zc);
        mc.test();
      }
    } catch (Exception e) {
      log.error("", e);
    }
  }
}
