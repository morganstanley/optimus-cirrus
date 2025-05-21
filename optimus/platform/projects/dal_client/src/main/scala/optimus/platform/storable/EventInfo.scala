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

import optimus.entity.IndexInfo
import optimus.entity.StorableInfo
import optimus.graph.PropertyInfo
import optimus.platform._
import optimus.platform.dal.DALEventInfo
import optimus.platform.dal.LocalDALEventInfo
import optimus.platform.pickling.PickledInputStream
import optimus.platform.pickling.UnsafeFieldInfo
import optimus.platform.pickling.ReflectiveEventPickling
import optimus.platform.BusinessEvent

class EventInfo(
    val runtimeClass: Class[_],
    val properties: Seq[PropertyInfo[_]],
    val parents: Seq[EventInfo],
    val indexes: Seq[IndexInfo[_, _]],
    val upcastDomain: Option[UpcastDomain] = None)
    extends StorableInfo {
  override def toString: String = runtimeClass.getName

  protected[optimus] lazy val unsafeFieldInfo: Seq[UnsafeFieldInfo] =
    ReflectiveEventPickling.prepareMeta(this)

  private[optimus] lazy val validTime_mh = ReflectiveEventPickling.getValidTimeHandle(runtimeClass)

  type BaseType = BusinessEvent
  type PermRefType = BusinessEventReference
  def deserializePermReference(rep: String): PermRefType = BusinessEventReference.fromString(rep)

  lazy val baseTypes: Set[EventInfo] = {
    parents.foldLeft(Set.empty[EventInfo])(_ union _.baseTypes) + this
  }

  val keys: Seq[IndexInfo[_, _]] = indexes filter { _.unique }

  def createUnpickled(
      is: PickledInputStream,
      forceUnpickle: Boolean,
      eventInfo: DALEventInfo,
      eventRef: BusinessEventReference): BaseType = {
    ReflectiveEventPickling.unpickleCreate(this, is, forceUnpickle, eventInfo, eventRef)
  }
  override def createUnpickled(is: PickledInputStream, forceUnpickle: Boolean): BusinessEvent =
    createUnpickled(is, forceUnpickle, LocalDALEventInfo, null)
}
