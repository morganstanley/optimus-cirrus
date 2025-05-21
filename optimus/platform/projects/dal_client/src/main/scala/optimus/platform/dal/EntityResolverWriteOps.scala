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
package optimus.platform.dal

import optimus.platform._
import optimus.platform.dsi.bitemporal.PutApplicationEvent
import optimus.platform.dsi.bitemporal.WriteBusinessEvent
import optimus.platform.storable.{BusinessEventReference, Entity, EntityReference, Key}

trait EntityResolverWriteOps {
  @async def executeAppEvent(
      appEvtCmds: Seq[PutApplicationEvent],
      cmdToEntity: Map[WriteBusinessEvent.Put, Entity],
      mutateEntities: Boolean,
      retryWrite: Boolean,
      refMap: EntityReference => Entity): PersistResult

  @async def invalidateCurrentInstances[E <: Entity: Manifest](): Int
  def invalidateCurrentInstances[E <: Entity: Manifest](refs: Seq[EntityReference]): Int
  def invalidateCurrentInstances(refs: Seq[EntityReference], className: String): Int
  def purgePrivateContext(): Unit

  def obliterateClass[E <: Entity: Manifest](): Unit
  def obliterateEntity(key: Key[_]): Unit
  def obliterateEntities(refs: Seq[EntityReference], classNameOpt: Option[String] = None): Unit
  def obliterateEntityForClass(ref: EntityReference, className: String): Unit
  def obliterateEventClass[B <: BusinessEvent: Manifest](): Unit
  def obliterateEvent(key: Key[_]): Unit
  def obliterateEvent(ref: BusinessEventReference): Unit
  def obliterateEvents(refs: Seq[BusinessEventReference], classNameOpt: Option[String] = None): Unit
  def obliterateEventForClass(ref: BusinessEventReference, className: String): Unit
}
