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
package optimus.platform.pickling

import optimus.core.utils.RuntimeMirror
import optimus.entity.EntityInfo
import optimus.graph.PropertyInfo
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityReference
import optimus.platform.storable.StorageInfo

import java.lang.invoke.MethodHandle
import java.lang.reflect.Method

final case class UnsafeFieldInfo(
    pinfo: PropertyInfo[_],
    storageKind: UnsafeFieldInfo.StorageKind,
    initMethod: Option[Method],
    fieldReader: MethodHandle,
    fieldSetter: MethodHandle
)

object UnsafeFieldInfo {

  /** An enumeration of the ways in which a storable entity property can be stored on the entity instance. */
  type StorageKind = StorageKind.Value
  object StorageKind extends Enumeration {

    /**
     * The node itself is stored in a field of type `PropertyNode`. This occurs when the value is either an entity node
     * or a constructor argument.
     */
    val Node: Value = Value

    /**
     * The property is stored as its type, but it has node methods ($impl, $newNode, &c.) as it is tweakable. In this
     * case, the corresponding node is synthesized by `PluginSupport.observedValueNode`.
     */
    val Tweakable: Value = Value

    /**
     * The property is stored as its type, and is not tweakable. In this case, the value can be read and written as-is
     * to the field.
     */
    val Val: Value = Value

  }
}

object ReflectiveEntityPickling {
  val instance: ReflectiveEntityPickling = {
    // look impl up by reflection since it's in dal_client which depends on core
    val module = RuntimeMirror.moduleByName("optimus.platform.pickling.ReflectiveEntityPicklingImpl", getClass)
    module.asInstanceOf[ReflectiveEntityPickling]
  }
}

trait ReflectiveEntityPickling {

  def pickle(inst: Entity, out: PickledOutputStream): Unit

  /**
   * Create an Entity instance wholesale from an unpickling stream.
   *
   * @param info
   *   Entity descriptor.
   * @param is
   *   The input stream from which to read data.
   * @param forceUnpickle
   *   Whether to eagerly or lazily unpickle things
   * @param storageInfo
   * @param ref
   *   Reference into which to put the created Entity
   * @return
   */
  def unpickleCreate(
      info: EntityInfo,
      is: PickledInputStream,
      forceUnpickle: Boolean,
      storageInfo: StorageInfo,
      ref: EntityReference): Entity

  def prepareMeta(info: EntityInfo): collection.Seq[UnsafeFieldInfo]
}
