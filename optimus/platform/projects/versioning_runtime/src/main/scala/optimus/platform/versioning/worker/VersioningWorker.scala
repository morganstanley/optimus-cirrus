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
package optimus.platform.versioning.worker

import java.rmi.Remote
import java.rmi.RemoteException
import optimus.dsi.session.ClientSessionInfo
import optimus.platform.dsi.bitemporal.proto.Dsi.TemporalContextProto
import optimus.platform.pickling.PickledProperties
import optimus.platform.versioning.RftShape

import scala.util.Try

final case class VersioningRequest(
    properties: PickledProperties,
    fromShape: RftShape,
    toShape: RftShape,
    loadContext: TemporalContextProto,
    sessionInfo: ClientSessionInfo,
    id: String)

final case class VersioningResponse(result: Try[PickledProperties], id: String)

object VersioningWorkerRegistryLocatorId {
  val Id = "versioningworker"
}

trait VersioningWorker extends Remote {

  @throws(classOf[RemoteException])
  def ipcPerformTransformation(req: VersioningRequest): VersioningResponse

  @throws(classOf[RemoteException])
  def shutdown(): Unit
}
