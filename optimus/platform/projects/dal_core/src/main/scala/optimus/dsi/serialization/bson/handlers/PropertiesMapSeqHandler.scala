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
package optimus.dsi.serialization.bson.handlers

import java.util.Arrays

import optimus.dsi.serialization.bson.BSONCallback
import optimus.dsi.serialization.bson.BinaryMarkers
import optimus.dsi.serialization.bson.BSONCallback

/**
 * Used to handle the array sections in the BSON representation of a limited set of types.
 */
class PropertiesMapSeqHandler(
    initParentName: String,
    initParent: PropertyHolder,
    initHandlerStack: PropertiesMapHandlerStack)
    extends BSONCallback
    with PropertyHolder {
  var parentName = initParentName
  var parent = initParent
  var elemParser: ElemParser = null
  var handlerStack = initHandlerStack

  def reinit(
      reinitParentName: String,
      reinitParent: PropertyHolder,
      reinitHandlerStack: PropertiesMapHandlerStack): Unit = {
    handlerStack = reinitHandlerStack
    elemParser = null
    parent = reinitParent
    parentName = reinitParentName
  }

  /**
   * Some special types are encoded in BSON using sequences of byte arrays. The first byte array in the sequence is a
   * marker to indicate what is to follow. Here we set the element handler based on this marker so that subsequent
   * elements are parsed appropriately
   */
  override def gotBinary(name: String, typeByte: Byte, data: Array[Byte]): Unit = {
    if (elemParser != null) {
      elemParser.parse(data)
    } else if (Arrays.equals(data, BinaryMarkers.ByteArray)) {
      elemParser = handlerStack.byteArrayParser
    } else if (Arrays.equals(data, BinaryMarkers.LocalDate)) {
      elemParser = handlerStack.localDateElemParser
    } else if (Arrays.equals(data, BinaryMarkers.LocalTime)) {
      elemParser = handlerStack.localTimeElemParser
    } else if (Arrays.equals(data, BinaryMarkers.OffsetTime)) {
      elemParser = handlerStack.offsetTimeElemParser
    } else if (Arrays.equals(data, BinaryMarkers.ZonedDateTime)) {
      elemParser = handlerStack.zonedDateTimeElemParser
    } else if (Arrays.equals(data, BinaryMarkers.ZoneId)) {
      elemParser = handlerStack.zoneIdElemParser
    } else if (Arrays.equals(data, BinaryMarkers.Period)) {
      elemParser = handlerStack.periodElemParser
    } else {
      elemParser = handlerStack.buffElemParser
      elemParser.parse(data)
    }
  }

  def generalHandleElem(v: Any): Unit = {
    if (elemParser == null) {
      elemParser = new BuffElemParser()
    }
    elemParser.parse(v)
  }

  override def gotInt(name: String, v: Int): Unit = generalHandleElem(v)
  override def gotLong(name: String, v: Long): Unit = generalHandleElem(v)
  override def gotString(name: String, v: String): Unit = generalHandleElem(v)
  override def gotBoolean(name: String, v: Boolean): Unit = generalHandleElem(v)
  override def gotDouble(name: String, v: Double): Unit = generalHandleElem(v)

  override def arrayStart(name: String): Unit = {
    handlerStack.push(handlerStack.newSeqHandler(name, this))
  }

  override def set(name: String, v: Any): Unit = generalHandleElem(v)

  override def objectStart(name: String): Unit = {
    handlerStack.push(handlerStack.newPropertiesHandler(name, this))
  }

  override def arrayDone(): AnyRef = {
    val deser = if (elemParser == null) Seq.empty else elemParser.deserialize()
    parent.set(parentName, deser)
    handlerStack.pop()
  }
}
