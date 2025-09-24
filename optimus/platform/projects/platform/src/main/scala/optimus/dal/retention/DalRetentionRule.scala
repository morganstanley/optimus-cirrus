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
package optimus.dal.retention

import optimus.dal.retention.DalRetentionRule.ReferencePoint
import optimus.platform._
import optimus.platform.storable.Entity

final case class DalRetentionRule[T <: Entity](
    resource: Query[T],
    retentionPeriodInDays: Long,
    referencePoint: DalRetentionRule.ReferenceTime = ReferencePoint.LastUpdate
)

object DalRetentionRule {
  sealed trait ReferenceTime
  // ReferenceTime is used to determine the point in time from which the retention period is calculated. We can add Creation and LastAccess ReferenceTime in the future if needed.
  object ReferencePoint {
    case object LastUpdate extends ReferenceTime
  }
}

/*
Client needs to implement this trait in order to provide retention rules for a specific Entity type.
This needs to be implemented in the companion object of the concrete Entity type.
 */
trait DalRetentionRules[T <: Entity] {

  def dalRetentionRules: Set[DalRetentionRule[T]]
}
