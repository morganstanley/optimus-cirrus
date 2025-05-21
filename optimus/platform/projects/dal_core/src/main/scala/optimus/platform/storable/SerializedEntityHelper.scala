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
package optimus.platform.storable

import optimus.platform.dsi.bitemporal.PutApplicationEvent
import optimus.platform.dsi.bitemporal.WriteBusinessEvent
import optimus.utils.CollectionUtils._

object SerializedEntityHelper {
  def entityPerPutEvent(
      putEvent: PutApplicationEvent
  ): Map[WriteBusinessEvent.Put, SerializedEntity] =
    putEvent.bes.flatMap(_.puts.map(p => p -> p.ent)).toMap

  def allSerializedEntities(
      entities: Seq[SerializedEntity]
  ): Seq[SerializedEntity] = entities ++ entities.flatMap(_.inlinedEntities)

  def collectStoredEntityReferences(
      entities: Seq[SerializedEntity]
  ): Seq[FinalTypedReference] = entities.flatMap(SerializedEntityHelper.collectStoredEntityReferences)

  def collectStoredEntityReferences(
      entity: SerializedEntity
  ): Seq[FinalTypedReference] = {
    val entsFromProperty = entity.properties.values.toSeq.flatMap(collectStoredEntityProperty)
    val entsFromLinkages = entity.linkages
      .getOrElse(Map.empty)
      .flatMap(_._2.map(_.link))
      .collectInstancesOf[FinalTypedReference]

    entsFromProperty ++ entsFromLinkages
  }

  def collectStoredEntityProperty(
      propertyValue: Any
  ): Seq[FinalTypedReference] =
    propertyValue match {
      case v: FinalTypedReference => Seq(v)
      case seq: collection.Seq[_] => seq.toSeq.flatMap(collectStoredEntityProperty)
      case set: Set[_]            => set.flatMap(collectStoredEntityProperty).toSeq
      case m: Map[_, _]           => m.values.flatMap(collectStoredEntityProperty).toSeq
      case _                      => Seq.empty
    }

  def collectEntitiesTyped[E <: Entity](
      entities: Seq[Entity],
      clazz: Class[E]
  ): Seq[E] = collectEntitiesUnTyped(entities, clazz).map(_.asInstanceOf[E])

  def collectEntitiesUnTyped(
      entities: Seq[Entity],
      clazz: Class[_]
  ): Seq[Entity] = entities.collect { case e if clazz.isAssignableFrom(e.getClass) => e }
}
