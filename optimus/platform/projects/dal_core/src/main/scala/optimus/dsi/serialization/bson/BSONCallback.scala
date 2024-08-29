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
package optimus.dsi.serialization.bson

// TODO (OPTIMUS-57177): license might need reformatting in order to publish this code as open-source
/**
 * A simplified callback describing the structure of a BSON document limited to the constructs used by optimus.
 *
 * See the <a href="http://bsonspec.org/spec.html">BSON Spec</a>.
 */
trait BSONCallback {

  /**
   * Signals the start of a BSON document, which usually maps onto some Java object.
   */
  def objectStart(): Unit = {}

  /**
   * Signals the start of a BSON document, which usually maps onto some Java object.
   *
   * @param name
   *   the field name of the document.
   */
  def objectStart(name: String): Unit = {}

  /**
   * @param array
   *   true if this object is an array
   */
  def objectStart(array: Boolean): Unit = {}

  /**
   * Called at the end of the document/array, and returns this object.
   *
   * @return
   *   the Object that has been read from this section of the document.
   */
  def objectDone(): AnyRef = ???

  /**
   * Signals the start of a BSON array.
   */
  def arrayStart(): Unit = {}

  /**
   * Signals the start of a BSON array, with its field name.
   *
   * @param name
   *   the name of this array field
   */
  def arrayStart(name: String): Unit = {}

  /**
   * Called the end of the array, and returns the completed array.
   *
   * @return
   *   an Object representing the array that has been read from this section of the document.
   */
  def arrayDone(): AnyRef = ???

  /**
   * Called when reading a BSON field that exists but has a null value.
   *
   * @param name
   *   the name of the field
   */
  def gotNull(name: String): Unit = {}

  /**
   * Called when reading a field with a Boolean value.
   *
   * @param name
   *   the name of the field
   * @param value
   *   the field's value
   */
  def gotBoolean(name: String, value: Boolean): Unit = {}

  /**
   * Called when reading a field with a Double value.
   *
   * @param name
   *   the name of the field
   * @param value
   *   the field's value
   */
  def gotDouble(name: String, value: Double): Unit = {}

  /**
   * Called when reading a field with an Int32 value.
   *
   * @param name
   *   the name of the field
   * @param value
   *   the field's value
   */
  def gotInt(name: String, value: Int): Unit = {}

  /**
   * Called when reading a field with an Int64 value.
   *
   * @param name
   *   the name of the field
   * @param value
   *   the field's value
   */
  def gotLong(name: String, value: Long): Unit = {}

  /**
   * Called when reading a field with a String value.
   *
   * @param name
   *   the name of the field
   * @param value
   *   the field's value
   */
  def gotString(name: String, value: String): Unit = {}

  /**
   * Called when reading a field with an ObjectID value.
   *
   * @param name
   *   the name of the field
   * @param id
   *   the object ID
   */
  def gotObjectId(name: String, id: CallbackObjectId): Unit = {}

  /**
   * Called when reading a field with a binary value. Note that binary values have a subtype, which may determine how
   * the value is processed.
   *
   * @param name
   *   the name of the field
   * @param type
   *   one of the binary subtypes
   * @param data
   *   the field's value
   */
  def gotBinary(name: String, typeByte: Byte, data: Array[Byte]): Unit = {}
}
