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
package optimus.platform.dal

import optimus.platform.dal.config.HostPort
import optimus.platform.dal.config.TreadmillApp
import optimus.platform.dal.config.TreadmillInstanceId

sealed trait ClientMachineIdentifier extends Serializable
// The UnremarkableClientMachine can be set on the client side where it's hard
// to get the host:port. The server already gets that, so when we create the
// EstablishedClientSession on the server, we create an instance of
// UnremarkableClientMachineWithHostPort.
object UnremarkableClientMachine extends ClientMachineIdentifier {
  override def toString(): String = "N/A"
}
final case class UnremarkableClientMachineWithHostPort(val hostport: HostPort) extends ClientMachineIdentifier {
  override def toString(): String = hostport.hostport
}
final case class TreadmillNode(val treadmillApp: TreadmillApp, val treadmillInstanceId: TreadmillInstanceId)
    extends ClientMachineIdentifier {
  override def toString(): String =
    s"treadmillApp: ${treadmillApp.underlying} treadmillInstanceId: ${treadmillInstanceId.underlying}"
}
