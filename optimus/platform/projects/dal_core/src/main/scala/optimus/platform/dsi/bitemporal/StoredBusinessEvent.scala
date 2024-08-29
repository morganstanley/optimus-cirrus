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

import optimus.platform.storable._
import java.time.Instant
import optimus.platform.bitemporal.Segment
import optimus.dsi.base.RefHolder
import optimus.platform.storable.VersionedReference

final case class BusinessEventGrouping(
    id: TypedBusinessEventReference,
    cmid: Option[CmReference],
    className: String,
    types: Seq[String],
    maxTimeSliceCount: Int,
    lockToken: Long)
    extends HasDSIId[BusinessEventReference]
    with OptimisticallyVersioned {

  /**
   * @return
   *   a new version of the grouping with the updated space
   */
  def next(maxTimeSliceCount: Int, tt: Instant) =
    new BusinessEventGrouping(id, cmid, className, types, maxTimeSliceCount, lockToken + 1)

  // transitional code to promote old grouping schema to new (including types). remove after schema migration complete.
  def next(maxTimeSliceCount: Int, newTypes: Seq[String], tt: Instant) =
    //    new BusinessEventGrouping(id, className, newTypes, maxTimeSliceCount, DateTimeSerialization.fromInstant(tt))
    new BusinessEventGrouping(id, cmid, className, newTypes, maxTimeSliceCount, lockToken + 1)

  override def equals(o: Any): Boolean = o match {
    case evt: BusinessEventGrouping => lockToken == evt.lockToken && permanentRef == evt.permanentRef
    case _                          => false
  }

  override def hashCode: Int = id.hashCode * 31 + lockToken.hashCode

  final def permanentRef = id

  override def toString() = s"Event Grp(ref=${id}, cn=${className}, lt=${lockToken}, tsCnt=${maxTimeSliceCount})"
}

final case class BusinessEventTimeSlice(
    id: BusinessEventReference,
    appEventId: AppEventReference,
    timeSliceNumber: Int,
    validTime: Instant,
    segment: Segment[VersionedReference],
    isCancel: Boolean) {
  def businessEventVersionedRef = segment.data
  def versionId = timeSliceNumber

}

final case class BusinessEventKey(key: SerializedKey, id: RefHolder, beref: BusinessEventReference)
    extends HasDSIId[RefHolder]
