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

import java.time.Instant
import optimus.dsi.base.SlottedVersionedReference
import optimus.platform.storable.SerializedEntity.TypeRef
import optimus.platform.storable.StorableReference
import optimus.platform.storable._

object TombstoneBlobEntry {
  def apply(
      vref: VersionedReference,
      slot: Int,
      ref: StorableReference,
      tt: Instant,
      isEvent: Boolean,
      typeId: Option[Int]): TombstoneBlobEntry = {
    new TombstoneBlobEntry(new SlottedVersionedReference(vref, slot), ref, tt, isEvent, typeId)
  }
  def apply(
      vref: VersionedReference,
      ref: StorableReference,
      tt: Instant,
      isEvent: Boolean,
      typeId: Option[Int]): TombstoneBlobEntry = {
    // TODO (OPTIMUS-14649): Investigate how obliterate works with slots
    apply(vref, 0, ref, tt, isEvent, typeId)
  }
}

final case class TombstoneBlobEntry private (
    slottedVref: SlottedVersionedReference,
    ref: StorableReference,
    tt: Instant,
    isEvent: Boolean,
    typeId: Option[Int]) {
  def vref = slottedVref.vref
  def slot = slottedVref.slot
}
