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
package optimus.utils.zookeeper

import org.apache.curator.utils.ZookeeperFactory
import org.apache.zookeeper.Watcher
import org.apache.zookeeper.ZooKeeper
import org.apache.zookeeper.client.ZKClientConfig

class NoSASLZKClientConfig extends ZKClientConfig {
  setProperty(ZKClientConfig.ENABLE_CLIENT_SASL_KEY, "false")
}

class NoSASLZookeeperFactory extends ZookeeperFactory {
  override def newZooKeeper(
      connectString: String,
      sessionTimeout: Int,
      watcher: Watcher,
      canBeReadOnly: Boolean): ZooKeeper = {
    new ZooKeeper(connectString, sessionTimeout, watcher, canBeReadOnly, new NoSASLZKClientConfig())
  }
}

class PlainSASLZKClientConfig(proid: String) extends ZKClientConfig {
  setProperty(ZKClientConfig.ENABLE_CLIENT_SASL_KEY, "true")
  setProperty(ZKClientConfig.ZK_SASL_CLIENT_USERNAME, proid)
}

class PlainSASLZookeeperFactory(proid: String) extends ZookeeperFactory {
  override def newZooKeeper(
      connectString: String,
      sessionTimeout: Int,
      watcher: Watcher,
      canBeReadOnly: Boolean): ZooKeeper = {
    new ZooKeeper(connectString, sessionTimeout, watcher, canBeReadOnly, new PlainSASLZKClientConfig(proid))
  }
}
