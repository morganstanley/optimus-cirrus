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
package optimus.breadcrumbs.kafka
import org.apache.zookeeper.client.ZKClientConfig

// Constructor for kafka.zk.KafkaZkClient in Kafka 2.8.2 supplies name and zkClientConfig below by default if not specified
// Constructor in Kafka 3.5.1 requires both to be supplied
object KafkaCompatability {
  // used for kafka.utils.Logging.logIdent in kafka.zookeeper.ZooKeeperClient
  val name = "[ZooKeeperClient] "

  // def not val because it's mutable - supply a new one each time
  def zkClientConfig = new ZKClientConfig
}
