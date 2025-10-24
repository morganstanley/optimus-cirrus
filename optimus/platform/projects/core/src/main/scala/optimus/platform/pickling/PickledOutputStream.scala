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

import optimus.graph.PropertyInfo
import optimus.platform.storable.Entity

trait WriteContext {
  def writeFieldName(k: String): Unit
  def writePropertyInfo(info: PropertyInfo[_]): Unit = writeFieldName(info.name)

  def writeBoolean(data: Boolean): Unit
  def writeChar(data: Char): Unit
  def writeDouble(data: Double): Unit
  def writeFloat(data: Float): Unit
  def writeInt(data: Int): Unit
  def writeLong(data: Long): Unit
  def writeRawObject(data: AnyRef): Unit
}

trait PickledOutputStream extends WriteContext {
  def writeStartObject(): Unit

  /**
   * isUnordered should be true if the array being written represents an unordered collection (e.g. HashSet, HashMap),
   * and false if it's an ordered collection (Seq, SortedSet etc.). The implementation may choose to sort unordered
   * collections to produce stable output, but must not change the order of collections which were already ordered.
   */
  def writeStartArray(isUnordered: Boolean = false): Unit
  def writeEndObject(): Unit
  def writeEndArray(): Unit

  def writeRawObject(data: AnyRef): Unit

  def writeEntity(entity: Entity): Unit

  def write[T](data: T, pickler: Pickler[T]): Unit

  // Should we force hydration of lazy-loaded properties?
  // When this returns true, the pickling logic may call writeRawObject with the pickled representation of the property.
  // Otherwise, it must force hydration.
  def forceHydration: Boolean = false
}

abstract class AbstractPickledOutputStream extends PickledOutputStream {
  def writeFieldName(k: String): Unit = {}
  def writeStartObject(): Unit = {}
  def writeStartArray(isUnordered: Boolean): Unit = {}
  def writeEndObject(): Unit = {}
  def writeEndArray(): Unit = {}

  def write[T](data: T, pickler: Pickler[T]): Unit = {
    pickler.pickle(data, this)
  }

  def writeBoolean(data: Boolean): Unit = {}
  def writeChar(data: Char): Unit = {}
  def writeDouble(data: Double): Unit = {}
  def writeFloat(data: Float): Unit = {}
  def writeInt(data: Int): Unit = {}
  def writeLong(data: Long): Unit = {}
  def writeRawObject(data: AnyRef): Unit = {}
}
