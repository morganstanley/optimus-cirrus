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
package optimus.platform.dsi.bitemporal

import optimus.breadcrumbs.ChainedID
import optimus.platform.dal.ClientMachineIdentifier
import optimus.platform.dal.config.HostPort

trait LoggableDsiResultsInfo {
  def commandCounts: CommandCounts
  def applicationIdentifier: ResolvedClientAppIdentifier
  def clientMachineIdentifier: Option[ClientMachineIdentifier]
  def hasStreamingCommand: Boolean
  def accReadCommandCount: Int
  def pid: Option[Int]
  def realId: String
  def effectiveId: String
  def effectiveZone: String
  def hostport: HostPort
  def chainedId: ChainedID
  def commandToTagsMapping: Map[Command, Seq[String]]
  def partitionForCommands: Option[String]
}

final case class CommandCounts(
    other: Int,
    assert: Int,
    put: Int,
    putSlot: Int,
    invalidate: Int,
    revert: Int,
    getInfo: Int) {
  val write = put + putSlot + invalidate + revert
  val total = getInfo + assert + write + other
}

object CommandCounts {
  def apply(raw: (Int, Int, Int, Int, Int, Int, Int)): CommandCounts =
    new CommandCounts(raw._1, raw._2, raw._3, raw._4, raw._5, raw._6, raw._7)
}
