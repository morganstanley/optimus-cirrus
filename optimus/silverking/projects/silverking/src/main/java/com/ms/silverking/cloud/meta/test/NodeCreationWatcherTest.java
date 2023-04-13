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
package com.ms.silverking.cloud.meta.test;

import com.ms.silverking.cloud.meta.NodeCreationListener;
import com.ms.silverking.cloud.meta.NodeCreationWatcher;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient;

public class NodeCreationWatcherTest implements NodeCreationListener {
  private final SilverKingZooKeeperClient zk;

  public NodeCreationWatcherTest(SilverKingZooKeeperClient zk) {
    this.zk = zk;
  }

  @Override
  public void nodeCreated(String path) {
    System.out.println("nodeCreated: " + path);
  }

  public void addWatch(String path) {
    System.out.println("watching: " + path);
    new NodeCreationWatcher(zk, path, this);
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      if (args.length < 2) {
        System.out.println("<zkConfig> <path...>");
      } else {
        ZooKeeperConfig zkConfig;
        NodeCreationWatcherTest wTest;

        zkConfig = new ZooKeeperConfig(args[0]);
        wTest = new NodeCreationWatcherTest(new SilverKingZooKeeperClient(zkConfig, 2 * 60 * 1000));
        for (int i = 1; i < args.length; i++) {
          wTest.addWatch(args[i]);
        }
        Thread.sleep(60 * 60 * 1000);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
