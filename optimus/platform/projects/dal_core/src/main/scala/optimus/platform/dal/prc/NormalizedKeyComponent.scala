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
package optimus.platform.dal.prc

import optimus.dsi.base.RefHolder
import optimus.platform.dsi.bitemporal.Command
import optimus.platform.dsi.bitemporal.EntityClassAppIdUserIdQuery
import optimus.platform.dsi.bitemporal.EntityClassQuery
import optimus.platform.dsi.bitemporal.EntityCmReferenceQuery
import optimus.platform.dsi.bitemporal.EventClassQuery
import optimus.platform.dsi.bitemporal.EventCmReferenceQuery
import optimus.platform.dsi.bitemporal.EventReferenceQuery
import optimus.platform.dsi.bitemporal.EventSerializedKeyQuery
import optimus.platform.dsi.bitemporal.LinkageQuery
import optimus.platform.dsi.bitemporal.Query
import optimus.platform.dsi.bitemporal.ReferenceQuery
import optimus.platform.dsi.bitemporal.SerializedKeyQuery
import optimus.platform.storable.EntityReference
import optimus.platform.storable.TypedBusinessEventReference

sealed trait NormalizedPrcKeyComponent {
  def isCacheable: Boolean
}

final case class NormalizedNonCacheableCommand(command: Command) extends NormalizedPrcKeyComponent {
  val isCacheable = false;
}

sealed trait NormalizedCacheableQuery extends NormalizedPrcKeyComponent {
  val isCacheable = true;
  def denormalize: Query
}

object NormalizedCacheableQuery {
  def normalize(query: Query): Option[NormalizedCacheableQuery] = query match {
    case ReferenceQuery(ref, _, _)                             => Some(NormalizedEntityReferenceQuery(ref))
    case EventReferenceQuery(ref: TypedBusinessEventReference) => Some(NormalizedEventReferenceQuery(ref))

    case _: EventReferenceQuery | _: EntityClassQuery | _: EventClassQuery | _: SerializedKeyQuery |
        _: EventSerializedKeyQuery | _: ReferenceQuery | _: EntityClassAppIdUserIdQuery | _: EntityCmReferenceQuery |
        _: EventCmReferenceQuery | _: EntityClassAppIdUserIdQuery | _: LinkageQuery =>
      // can't yet use these in PRC
      None
  }

  def apply(query: Query): NormalizedCacheableQuery =
    normalize(query).getOrElse(
      throw new IllegalArgumentException(s"Cannot construct a normalized cacheable query for $query"))
}

final case class NormalizedEntityReferenceQuery private (entityRefBytes: RefHolder) extends NormalizedCacheableQuery {
  def denormalize: Query =
    ReferenceQuery(EntityReference(entityRefBytes.data))
}

object NormalizedEntityReferenceQuery {
  // Some entity reference objects may be typed/nontyped for the same underlying reference, which breaks equality
  // So we need to use the refHolder instead which will only hold the actual entity reference byte array
  def apply(entityRef: EntityReference): NormalizedEntityReferenceQuery =
    apply(new RefHolder(entityRef.data))
}

final case class NormalizedEventReferenceQuery(reference: TypedBusinessEventReference)
    extends NormalizedCacheableQuery {
  def denormalize: Query = EventReferenceQuery(reference)
}
