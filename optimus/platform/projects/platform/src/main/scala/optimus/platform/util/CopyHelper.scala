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
package optimus.platform.util

import optimus.entity.EntityInfoRegistry
import optimus.entity.StorableInfo
import optimus.graph.PropertyInfo
import optimus.platform._
import optimus.platform.dal.EventSerializer
import optimus.platform.pickling.PickledMapWrapper
import optimus.platform.pickling.Pickler
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityReference
import optimus.platform.storable.EventInfo
import optimus.platform.storable.SerializedBusinessEvent
import optimus.platform.storable.SerializedEntity
import optimus.platform.storable.Storable
import optimus.platform.storable.StorablePropertyMapOutputStream
import optimus.platform.storable.UniqueStorageInfo

import java.time.Instant

class PropertyNotModifiedException(msg: String) extends RuntimeException(msg)

object CopyHelper {
  private type Props = Map[String, Any] // that good ol' omnitype again

  private def getNewProperties(info: StorableInfo, oldProps: Props, newProps: Props): Props = {

    // the compiler won't do check if the type is not concrete, so we need to check whether all properties are modified or not
    val pickledProps = newProps.map { case (key, value) =>
      val pinfo: PropertyInfo[_] = info.storedProperties.find(_.name == key).getOrElse {
        throw new PropertyNotModifiedException(s"$key in $info can't be modified (not storable val)")
      }
      val stream = new StorablePropertyMapOutputStream()
      pinfo.pickler.asInstanceOf[Pickler[Any]].pickle(value: Any, stream)
      key -> stream.value
    }

    oldProps ++ pickledProps
  }

  // this is called from CopyMethodMacros
  def copyHelp[E <: Storable](e: E)(props: Map[String, Any]): E = {
    val cinst = e match {
      case ent: Entity =>
        ent.$info.createUnpickled(
          new PickledMapWrapper(getNewProperties(e.$info, e.toMap, props)),
          true,
          UniqueStorageInfo,
          null)
      case evt: BusinessEvent =>
        val (validTime, newProps) = props.get("validTime") match {
          case Some(x: Instant) => (x, getNewProperties(e.$info, e.toMap, props - "validTime"))
          case _                => (evt.validTime, getNewProperties(e.$info, e.toMap, props))
        }
        val ctx = TemporalContext(validTime, null)
        evt.$info.createUnpickled(new PickledMapWrapper(newProps, ctx), true)
      case _ => throw new IllegalArgumentException(s"Not an entity or event: $e")
    }
    assert(cinst.dal$isTemporary)
    cinst.asInstanceOf[E]
  }

  def toUniqueInstanceEntity(
      serializedEntity: SerializedEntity,
      eref2Entity: Map[EntityReference, Entity] = Map.empty): Entity = {
    val className = serializedEntity.className
    val uinst =
      EntityInfoRegistry
        .getClassInfo(className)
        .createUnpickled(
          new PickledMapWrapper(serializedEntity.properties, inlineEntitiesByRef = eref2Entity),
          forceUnpickle = true,
          UniqueStorageInfo,
          entityRef = null
        )
    assert(uinst.dal$isTemporary)
    uinst
  }

  def toUniqueInstanceEvent(serializedBusinessEvent: SerializedBusinessEvent): BusinessEvent = {
    val eventInfo = EntityInfoRegistry.getInfo(serializedBusinessEvent.className).asInstanceOf[EventInfo]
    val ctx = TemporalContext(serializedBusinessEvent.validTime, null)
    val uinst =
      eventInfo.createUnpickled(new PickledMapWrapper(serializedBusinessEvent.properties, ctx), forceUnpickle = true)
    assert(uinst.dal$isTemporary)
    uinst
  }
}
