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
package optimus.platform.relational.internal

import optimus.platform.AdvancedUtils
import optimus.platform.TemporalContext
import optimus.platform.pickling.{PickledInputStream, Unpickler}
import optimus.platform.storable.{Entity, EntityReference, SerializedEntity, StorableReference}

import scala.collection.mutable

class PriqlPickledMapWrapper(
    var properties: Map[String, Any],
    val temporalContext: TemporalContext = null,
    val reference: StorableReference = null,
    var inlineMap: Map[EntityReference, SerializedEntity] = Map.empty,
    val instanceMap: mutable.Map[EntityReference, Entity] = mutable.Map())
    extends PickledInputStream
    with Serializable {

  @inline private[this] final def setIfFound(key: String, set: Any => Unit): Boolean = {
    val a = properties.get(key)
    if (a.isDefined) {
      set(a.get)
      true
    } else {
      false
    }
  }

  final def seek[T](k: String, unpickler: Unpickler[T]): Boolean = {
    // Seek should only called from entity constructors on properties that are known to be nonblocking (barring bugs).
    // However, the unpickler code path is necessarily async (unless we duplicate the logic everywhere).
    // In order to support assertAsync tests that load from the DAL, suppress syncStack failure during entity unpickling.
    // Note that we turn it off here instead of in EntitySerialization because we DO want to catch entity constructors
    // that call into async nodes, e.g.:
    //   @entity class Foo { val badProperty = blockingCall }
    setIfFound(
      k,
      { v =>
        value = AdvancedUtils.suppressSyncStackDetection { unpickler.unpickle(v, this) }
      })
  }

  def seekRaw(k: String): Boolean = {
    setIfFound(
      k,
      { v =>
        value = v
      })
  }

  def seekChar(k: String): Boolean = {
    setIfFound(
      k,
      { v =>
        charValue = v.asInstanceOf[Char]; value = v
      })
  }

  def seekDouble(k: String): Boolean = {
    setIfFound(
      k,
      { v =>
        doubleValue = v.asInstanceOf[Double]; value = v
      })
  }

  def seekFloat(k: String): Boolean = {
    setIfFound(
      k,
      { v =>
        floatValue = v.asInstanceOf[Float]; value = v
      })
  }

  def seekInt(k: String): Boolean = {
    setIfFound(
      k,
      { v =>
        intValue = v.asInstanceOf[Int]; value = v
      })
  }

  def seekLong(k: String): Boolean = {
    setIfFound(
      k,
      { v =>
        longValue = v.asInstanceOf[Long]; value = v
      })
  }

  var charValue: Char = _
  var doubleValue: Double = _
  var floatValue: Float = _
  var intValue: Int = _
  var longValue: Long = _
  var value: Any = _
}
