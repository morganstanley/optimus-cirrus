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
package optimus.dsi.base

import optimus.platform.ImmutableArray
import optimus.platform.storable.StorableReference
import optimus.platform.storable.SerializedKey
import optimus.platform.storable.EntityReference
import optimus.platform.storable.BusinessEventReference
import optimus.platform.storable.AppEventReference
import optimus.platform.storable.VersionedReference

object ObliterateAction {
  sealed trait JournaledOp {
    def keys: Iterable[SerializedKey]
  }
  sealed trait KeyOp extends JournaledOp {
    val id: RefHolder
    val key: SerializedKey
    override val keys: Iterable[SerializedKey] = Seq(key)
  }

  final case class RemoveKey(id: RefHolder, key: SerializedKey) extends KeyOp
  final case class RemoveUniqueIndexGrouping(id: RefHolder, key: SerializedKey) extends KeyOp
  // TODO (OPTIMUS-36520): make 'maybeKey' and 'maybeKeyHash' non-optional.
  final case class RemoveUniqueIndexTimeSlices(
      eRefsToRemove: Seq[EntityReference],
      maybeKey: Option[SerializedKey],
      maybeKeyHash: Option[RefHolder])
      extends JournaledOp {
    override def keys: Iterable[SerializedKey] = maybeKey
  }
}

sealed trait ObliterateAction {
  val refs: Seq[StorableReference] // refs are always typed
  val types: Map[Int, ImmutableArray[Int]] // each unique typeId in refs has its inherited types in this map
  val journaledOps: Seq[ObliterateAction.JournaledOp]
  val keys: Seq[SerializedKey] = journaledOps.flatMap(_.keys)
  val tombstone: Boolean
  val appIdOpt: Option[AppEventReference]
}

final case class ObliterateEntities(
    refs: Seq[EntityReference],
    types: Map[Int, ImmutableArray[Int]],
    journaledOps: Seq[ObliterateAction.JournaledOp],
    tombstone: Boolean,
    appIdOpt: Option[AppEventReference],
    vrefsOpt: Option[Set[(VersionedReference, EntityReference)]] = None)
    extends ObliterateAction
/*
 * Events cannot have unique indexes currently, so the key operations will always be remove operations.
 * Also, note that with the current event implementation, there is no need to have the keys in there ObliterateEventBatch as
 * - keys are part of the whole BusinessEvent document referenced by ref
 * - there are no unique keys on business events
 * However, since that might change in the future, e.g. separate key documents for events, we add them here already
 * (04/08/2014)
 */
final case class ObliterateEvents(
    refs: Seq[BusinessEventReference],
    types: Map[Int, ImmutableArray[Int]],
    journaledOps: Seq[ObliterateAction.RemoveKey],
    appIdOpt: Option[AppEventReference])
    extends ObliterateAction {
  override val tombstone = false
}
