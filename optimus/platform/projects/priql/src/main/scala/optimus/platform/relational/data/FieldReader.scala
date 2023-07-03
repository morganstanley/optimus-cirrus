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
package optimus.platform.relational.data

import optimus.platform.relational.tree._
import optimus.platform.pickling.PickledInputStream

trait FieldReader {
  def readValue[T: TypeInfo](ordinal: Int): T

  def readOptionValue[T: TypeInfo](ordinal: Int): Option[T]

  def readBoolean(ordinal: Int): Boolean

  def readOptionBoolean(ordinal: Int): Option[Boolean]

  def readByte(ordinal: Int): Byte

  def readOptionByte(ordinal: Int): Option[Byte]

  def readChar(ordinal: Int): Char

  def readOptionChar(ordinal: Int): Option[Char]

  def readShort(ordinal: Int): Short

  def readOptionShort(ordinal: Int): Option[Short]

  def readInt(ordinal: Int): Int

  def readOptionInt(ordinal: Int): Option[Int]

  def readLong(ordinal: Int): Long

  def readOptionLong(ordinal: Int): Option[Long]

  def readFloat(ordinal: Int): Float

  def readOptionFloat(ordinal: Int): Option[Float]

  def readDouble(ordinal: Int): Double

  def readOptionDouble(ordinal: Int): Option[Double]

  def readString(ordinal: Int): String

  def readOptionString(ordinal: Int): Option[String]

  def isNone(ordinal: Int): Boolean

  def pickledInputStream: PickledInputStream
}

object FieldReader {
  import TypeInfo._

  val TYPE = TypeInfo(classOf[FieldReader])

  private val OPTION_CLASS = classOf[Option[_]]
  private val OPTION_BOOLEAN = TypeInfo(OPTION_CLASS, BOOLEAN)
  private val OPTION_BYTE = TypeInfo(OPTION_CLASS, BYTE)
  private val OPTION_CHAR = TypeInfo(OPTION_CLASS, CHAR)
  private val OPTION_SHORT = TypeInfo(OPTION_CLASS, SHORT)
  private val OPTION_INT = TypeInfo(OPTION_CLASS, INT)
  private val OPTION_LONG = TypeInfo(OPTION_CLASS, LONG)
  private val OPTION_FLOAT = TypeInfo(OPTION_CLASS, FLOAT)
  private val OPTION_DOUBLE = TypeInfo(OPTION_CLASS, DOUBLE)
  private val OPTION_STRING = TypeInfo(OPTION_CLASS, STRING)

  private lazy val isNoneMethod = new RuntimeMethodDescriptor(FieldReader.TYPE, "isNone", BOOLEAN)

  private lazy val readMethods = Map[TypeInfo[_], MethodDescriptor](
    BOOLEAN -> new RuntimeMethodDescriptor(FieldReader.TYPE, "readBoolean", BOOLEAN),
    BYTE -> new RuntimeMethodDescriptor(FieldReader.TYPE, "readByte", BYTE),
    CHAR -> new RuntimeMethodDescriptor(FieldReader.TYPE, "readChar", CHAR),
    SHORT -> new RuntimeMethodDescriptor(FieldReader.TYPE, "readShort", SHORT),
    INT -> new RuntimeMethodDescriptor(FieldReader.TYPE, "readInt", INT),
    LONG -> new RuntimeMethodDescriptor(FieldReader.TYPE, "readLong", LONG),
    FLOAT -> new RuntimeMethodDescriptor(FieldReader.TYPE, "readFloat", FLOAT),
    DOUBLE -> new RuntimeMethodDescriptor(FieldReader.TYPE, "readDouble", DOUBLE),
    STRING -> new RuntimeMethodDescriptor(FieldReader.TYPE, "readString", STRING)
  )

  private lazy val readOptionMethods = Map[TypeInfo[_], MethodDescriptor](
    BOOLEAN -> new RuntimeMethodDescriptor(FieldReader.TYPE, "readOptionBoolean", OPTION_BOOLEAN),
    BYTE -> new RuntimeMethodDescriptor(FieldReader.TYPE, "readOptionByte", OPTION_BYTE),
    CHAR -> new RuntimeMethodDescriptor(FieldReader.TYPE, "readOptionChar", OPTION_CHAR),
    SHORT -> new RuntimeMethodDescriptor(FieldReader.TYPE, "readOptionShort", OPTION_SHORT),
    INT -> new RuntimeMethodDescriptor(FieldReader.TYPE, "readOptionInt", OPTION_INT),
    LONG -> new RuntimeMethodDescriptor(FieldReader.TYPE, "readOptionLong", OPTION_LONG),
    FLOAT -> new RuntimeMethodDescriptor(FieldReader.TYPE, "readOptionFloat", OPTION_FLOAT),
    DOUBLE -> new RuntimeMethodDescriptor(FieldReader.TYPE, "readOptionDouble", OPTION_DOUBLE),
    STRING -> new RuntimeMethodDescriptor(FieldReader.TYPE, "readOptionString", OPTION_STRING)
  )

  def getReaderFunction(fieldType: TypeInfo[_], ordinal: Int, reader: RelationElement): RelationElement = {
    if (fieldType.clazz == classOf[Option[_]]) {
      readOptionMethods
        .get(fieldType.typeParams.head)
        .map(method => ElementFactory.call(reader, method, List(ElementFactory.constant(ordinal, INT))))
        .getOrElse {
          val method =
            new RuntimeMethodDescriptor(FieldReader.TYPE, "readOptionValue", fieldType, List(fieldType.typeParams.head))
          val paramList1 = new ExpressionListElement(List(ElementFactory.constant(ordinal, INT)))
          val paramList2 = new ExpressionListElement(List(ElementFactory.constant(fieldType.typeParams.head)))
          ElementFactory.call(reader, method, List(paramList1, paramList2))
        }
    } else {
      readMethods
        .get(fieldType)
        .map(method => ElementFactory.call(reader, method, List(ElementFactory.constant(ordinal, INT))))
        .getOrElse {
          val method = new RuntimeMethodDescriptor(FieldReader.TYPE, "readValue", fieldType, List(fieldType))
          val paramList1 = new ExpressionListElement(List(ElementFactory.constant(ordinal, INT)))
          val paramList2 = new ExpressionListElement(List(ElementFactory.constant(fieldType)))
          ElementFactory.call(reader, method, List(paramList1, paramList2))
        }
    }
  }

  def getCheckNoneFunction(ordinal: Int, reader: RelationElement): RelationElement = {
    ElementFactory.call(reader, isNoneMethod, List(ElementFactory.constant(ordinal, INT)))
  }
}
