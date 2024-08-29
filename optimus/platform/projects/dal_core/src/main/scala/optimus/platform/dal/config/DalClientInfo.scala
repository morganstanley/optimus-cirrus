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
package optimus.platform.dal.config

import optimus.platform.dsi.bitemporal.ResolvedClientAppIdentifier

private[optimus] trait DalClientInfoT {
  def realId: String
  def effectiveId: String
  def clientHostPort: HostPort
  def applicationIdentifier: ResolvedClientAppIdentifier
  def clientPid: Option[Int]
  def clientPath: String
}

private[optimus] final case class DalClientInfo(
    realId: String,
    effectiveId: String,
    clientHostPort: HostPort,
    applicationIdentifier: ResolvedClientAppIdentifier,
    clientPid: Option[Int],
    clientPath: String
) extends DalClientInfoT {
  override def toString: String = {
    val pidStr = clientPid.map(p => s" pid=$p").getOrElse("")
    val aidStr = s"${applicationIdentifier.zoneId}/${applicationIdentifier.appId}"
    s"DalClientInfo($realId/$effectiveId@$clientHostPort $aidStr$pidStr)"
  }
}

private[optimus] object DalClientInfo {
  val Admin: DalClientInfo =
    DalClientInfo("<admin>", "<admin>", HostPort.NoHostPort, ResolvedClientAppIdentifier.Unknown, None, "")
}
