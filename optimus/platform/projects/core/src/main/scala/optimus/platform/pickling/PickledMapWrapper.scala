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

import optimus.platform.TemporalContext
import optimus.platform.storable.InlineEntityHolder
import optimus.platform.storable.StorableReference

// This class is Serializable only to enable inlined entities to be serialized.
final class PickledMapWrapper(
    val properties: Map[String, Any],
    val temporalContext: TemporalContext = null,
    val reference: StorableReference = null,
    override val inlineEntitiesByRef: InlineEntityHolder = InlineEntityHolder.empty)
    extends PickledInputStream
    with Serializable {

  def newMutStream: PickledInputStreamMut = new PickledInputStreamMut(properties, this)
}

class PickledInputStreamMut(properties: Map[String, Any], context: PickledInputStream) {
  @inline private[this] def setIfFound(key: String, set: Any => Unit): Boolean = {
    val a = properties.get(key)
    if (a.isDefined) {
      set(a.get)
      true
    } else {
      false
    }
  }

  def seek[T](k: String, unpickler: Unpickler[T]): Boolean = {
    setIfFound(k, { v => value = unpickler.unpickle(v, context) })
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
