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

import optimus.dsi.serialization.bson.entError

import java.time._
import optimus.platform.pickling.ImmutableByteArray
import optimus.platform.storable.StorableReference

/**
 * Implementations of ElemParser are used to parse the elements in the BSON document representing SerializedEntity
 */
trait ElemParser {
  def parse(elem: Any): Unit
  def deserialize(): Any
}

class BuffElemParser extends ElemParser {
  private var buff = new Array[Any](2)
  private var pos = 0

  def reinit(): Unit = {
    pos = 0
  }

  override def parse(elem: Any): Unit = {
    if (pos >= buff.length) {
      val newBuff = new Array[Any](buff.length * 3 / 2 + 1)
      Array.copy(buff, 0, newBuff, 0, buff.length)
      buff = newBuff
    }
    val wrapElem = elem match {
      case b: Array[Byte] => ImmutableByteArray(b)
      case _              => elem
    }
    buff(pos) = wrapElem
    pos += 1
  }

  override def deserialize(): Any = {
    val retArray = new Array[Any](pos)
    Array.copy(buff, 0, retArray, 0, pos)
    pos = 0
    val seq: Seq[Any] = retArray
    seq
  }
}

class StoredSerializableElemParser[T <: StorableReference](handlerFn: (Array[Byte], Option[Int]) => T)
    extends ElemParser {
  private var ref: Array[Byte] = null
  private var typeId: Option[Int] = None

  override def parse(elem: Any): Unit = elem match {
    case x: Array[Byte] if ref eq null =>
      ref = x
    case x: Int if !typeId.isDefined =>
      typeId = Some(x)
    case _ => entError(s"bad type following marker $elem")
  }

  override def deserialize(): Any = {
    val res = handlerFn(ref, typeId)
    ref = null
    typeId = None
    res
  }
}

class UnaryElemParser[T](handlerFn: T => Any) extends ElemParser {
  private var singleElem: Any = null
  override def parse(elem: Any): Unit = singleElem = elem
  override def deserialize(): Any = handlerFn(singleElem.asInstanceOf[T])
}

class LongStringElemParser(handlerFn: (Long, String) => Any) extends ElemParser {
  private var longElem: Long = _
  private var stringElem: String = _

  override def parse(elem: Any): Unit = elem match {
    case x: Long   => longElem = x
    case x: String => stringElem = x
    case _         => entError(s"bad type following marker $elem")
  }

  override def deserialize(): Any = handlerFn(longElem, stringElem)
}

class PeriodElemParser extends ElemParser {
  private var pos: Int = 0
  private var nanos: Long = _
  private var vals = new Array[Int](6)

  override def deserialize(): Any = {
    val p = Period.of(vals(0), vals(1), vals(2))
    pos = 0
    p
  }

  override def parse(elem: Any): Unit = {
    if (pos == 6)
      nanos = elem match {
        case i: Int => i.toLong
        case _      => elem.asInstanceOf[Long]
      }
    else
      vals(pos) = elem.asInstanceOf[Int]
    pos += 1
  }
}
