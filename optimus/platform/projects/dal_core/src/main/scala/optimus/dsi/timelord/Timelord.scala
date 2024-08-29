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
package optimus.dsi.timelord

import optimus.dsi.base.Subject
import optimus.platform.dal.config.DalAppId

final case class TimelordSystem(
    systemName: String,
    entitlements: Seq[TimelordEntitlement]) // this will end up being an configuration entity in the dal

sealed trait TimelordType {
  def external: Boolean
}

object Timelord {
  case object Backfilling extends TimelordType { val external = true }
  final case class Mastering(systemName: String) extends TimelordType { val external = true }
  case object DAL extends TimelordType { val external = false }
  case object Admin extends TimelordType { val external = true }
}

final case class TimelordConnection(appId: DalAppId, prodId: String)
final case class TimelordEntitlement(
    connection: TimelordConnection,
    useCase: TimelordType,
    writeEntityTypes: Set[String] = Set.empty)

object TimelordApplicationPermissions {
  type TimelordTypes = Map[TimelordConnection, TimelordType]
}

trait TimelordApplicationPermissions extends Subject[TimelordApplicationPermissions] {
  import TimelordApplicationPermissions._

  /**
   * returns a map which describes which timelord connection performs which use case
   */
  def timelordTypes: TimelordTypes

  def timelordSystems: Set[TimelordSystem]

}

private[optimus] object EmptyTimelordApplicationPermissions extends TimelordApplicationPermissions {
  override val timelordTypes: Map[TimelordConnection, TimelordType] = Map.empty[TimelordConnection, TimelordType]
  override val timelordSystems: Set[TimelordSystem] = Set.empty[TimelordSystem]
}
