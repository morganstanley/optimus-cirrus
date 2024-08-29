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

import optimus.dsi.serialization.bson.BSONCallback

import scala.collection.immutable.HashMap

/**
 * Used to handle a map of String to Any for nested object representation.
 */
trait PropertyHolder {
  def set(name: String, value: Any): Unit

  def parentName: String

  override def toString: String = s"${getClass.getSimpleName}($parentName)"
}

class PropertiesHandler(
    val initParentName: String,
    initParent: PropertyHolder,
    initHandlerStack: PropertiesMapHandlerStack)
    extends BSONCallback
    with PropertyHolder {
  private var builder = HashMap.newBuilder[String, Any]
  var parentName = initParentName
  var parent = initParent
  private var handlerStack = initHandlerStack

  def getProps = builder.result()

  def reinit(
      reinitParentName: String,
      reinitParent: PropertyHolder,
      reinitHandlerStack: PropertiesMapHandlerStack): Unit = {
    handlerStack = reinitHandlerStack
    parentName = reinitParentName
    parent = reinitParent
    builder.clear()
  }

  override def gotString(name: String, v: String): Unit = set(name, v)

  override def gotInt(name: String, v: Int): Unit = set(name, v)

  override def gotDouble(name: String, v: Double): Unit = set(name, v)

  override def gotLong(name: String, v: Long): Unit = set(name, v)

  override def gotBoolean(name: String, v: Boolean): Unit = set(name, v)

  override def gotBinary(name: String, typeByte: Byte, v: Array[Byte]): Unit = set(name, v)

  override def set(name: String, value: Any): Unit = {
    builder += Tuple2(name, value)
  }

  override def objectStart(name: String): Unit = {
    handlerStack.push(handlerStack.newPropertiesHandler(name, this))
  }

  override def objectDone(): AnyRef = {
    val map = builder.result()
    parent.set(parentName, map)
    handlerStack.pop()
  }

  override def arrayStart(name: String): Unit = {
    handlerStack.push(handlerStack.newSeqHandler(name, this))
  }
}
