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
package optimus.platform.util

import optimus.exceptions.RTException
import optimus.platform.NodeTry
import optimus.platform.node
import optimus.platform.scenarioIndependent
import msjava.slf4jutils.scalalog.getLogger

object NodeTryUtils {
  val safeLog = getLogger(this)
}

trait NodeTryUtils {

  implicit class NodeTryOps[A](nodeTry: NodeTry[A]) {
    @node @scenarioIndependent def toOptionRT: Option[A] =
      nodeTry.map(Option.apply).getOrRecover { case ex @ RTException =>
        NodeTryUtils.safeLog.error("Error: ", ex)
        None
      }
  }

}
