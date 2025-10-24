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

import msjava.slf4jutils.scalalog._
import optimus.dsi.serialization.bson.handlers.PropertiesMapHandlerStack
import optimus.dsi.serialization.bson.handlers.PropertyHolder
import optimus.platform.pickling.PickledProperties

private[bson] class HandlerStackCallback extends BSONCallback with PropertyHolder {
  private val RootKey = "RootKey"

  implicit val handlerStack: PropertiesMapHandlerStack = new PropertiesMapHandlerStack
  private val rootProps = handlerStack.newPropertiesHandler(RootKey, this)
  private val logger = getLogger[HandlerStackCallback]
  private val debug = false
  private var result: PickledProperties = null

  handlerStack.push(rootProps)

  override def set(name: String, value: Any): Unit = {
    if (debug) {
      logger.debug(s"set $name with $value")
    }
    if (RootKey == name) {
      result = value.asInstanceOf[PickledProperties]
    }
  }

  override def parentName: String = RootKey

  def properties: PickledProperties = result

  private var indent = ""

  private def in(): Unit = {
    indent = indent + "    "
  }

  private def out(): Unit = {
    indent = indent.take(indent.length - 4)
  }

  override def objectStart(): Unit = {
    if (debug) {
      logger.debug(s"${indent}objectStart()"); in()
    }
    handlerStack.head.objectStart()
  }

  override def objectStart(array: Boolean): Unit = {
    if (debug) {
      logger.debug(s"${indent}objectStart(array: $array)"); in()
    }
    handlerStack.head.objectStart(array)
  }

  override def objectStart(name: String): Unit = {
    if (debug) {
      logger.debug(s"${indent}objectStart(name: $name)"); in()
    }
    handlerStack.head.objectStart(name)
  }

  override def gotBinary(name: String, typeByte: Byte, data: Array[Byte]): Unit = {
    if (debug) logger.debug(s"${indent}gotBinary(name: $name, typeByte: $typeByte, data: $data)")
    handlerStack.head.gotBinary(name, typeByte, data)
  }

  override def gotString(name: String, v: String): Unit = {
    if (debug) logger.debug(s"${indent}gotString(name: $name, v: $v)")
    handlerStack.head.gotString(name, v)
  }

  override def objectDone(): AnyRef = {
    if (debug) {
      out(); logger.debug(s"${indent}objectDone()")
    }
    handlerStack.head.objectDone()
  }

  override def arrayStart(name: String): Unit = {
    if (debug) {
      logger.debug(s"${indent}arrayStart(name: $name)"); in()
    }
    handlerStack.head.arrayStart(name)
  }

  override def arrayStart(): Unit = {
    if (debug) {
      logger.debug(s"${indent}arrayStart()"); in()
    }
    handlerStack.head.arrayStart()
  }

  override def arrayDone(): AnyRef = {
    if (debug) {
      out(); logger.debug(s"${indent}arrayDone()")
    }
    handlerStack.head.arrayDone()
  }

  override def gotObjectId(name: String, id: CallbackObjectId): Unit = {
    if (debug) logger.debug(s"${indent}gotObjectId(name: $name, id: $id)")
    handlerStack.head.gotObjectId(name, id)
  }

  override def gotDouble(name: String, v: Double): Unit = {
    if (debug) logger.debug(s"${indent}gotDouble(name: $name, v: $v)")
    handlerStack.head.gotDouble(name, v)
  }

  override def gotInt(name: String, v: Int): Unit = {
    if (debug) logger.debug(s"${indent}gotInt(name: $name, v: $v)")
    handlerStack.head.gotInt(name, v)
  }

  override def gotNull(name: String): Unit = {
    if (debug) logger.debug(s"${indent}gotNull(name: $name)")
    handlerStack.head.gotNull(name)
  }

  override def gotLong(name: String, v: Long): Unit = {
    if (debug) logger.debug(s"${indent}gotLong(name: $name, v: $v)")
    handlerStack.head.gotLong(name, v)
  }

  override def gotBoolean(name: String, v: Boolean): Unit = {
    if (debug) logger.debug(s"${indent}gotBoolean(name: $name, v: $v)")
    handlerStack.head.gotBoolean(name, v)
  }
}
