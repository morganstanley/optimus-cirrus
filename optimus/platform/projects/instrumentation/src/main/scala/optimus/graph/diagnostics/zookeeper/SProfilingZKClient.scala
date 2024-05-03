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
package optimus.graph.diagnostics.zookeeper
import optimus.core.utils.ZkClient
import optimus.platform.util.Log

// testing with dev for now; will be deleted when we end up calling the initialization function
object ZkClientInitializer extends Log {
  def main(args: Array[String]): Unit = {
    val client = ZkClient.initializeClient("dev")
    log.info(s"Created ZK client: $client")
  }
}

object SProfilingZKClient extends ZkClient with Log {
  override val path = "/kafka/sprofiling"
}
