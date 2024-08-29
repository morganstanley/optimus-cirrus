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
package optimus.dal

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import org.slf4j.LoggerFactory

object ZookeeperLogSuppression {

  // SilverKing produces a lot of logging from ZK, particularly from ClientCnxn
  // In the future, we'll replace a lot of this with Curators which will reduce the logging
  // until then we need to suppress it
  // We also try to suppress these lines in the broker and other scripts, so this file exists here so
  // we can do so if we desire to.
  // n.b. that ZK has a weird logging setup and doesn't get suppressed by logback or log4j
  // this method can be called in the runtime to ensure the log lines are suppressed
  def suppress(): Unit = {
    val zkLog: Logger = LoggerFactory.getLogger("org.apache.zookeeper.ClientCnxn").asInstanceOf[Logger]
    if (zkLog.isDebugEnabled) zkLog.setLevel(Level.INFO)
  }

}
