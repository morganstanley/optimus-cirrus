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
package optimus.platform.versioning

import optimus.platform.pickling.PickledProperties
import optimus.platform.storable.SerializedEntity
import optimus.scalacompat.collection._

import scala.collection.immutable.SortedMap
import scala.collection.compat._

/*
 * Encodes the version of an entity as its type and stored property names
 */
sealed abstract class AbstractShape[FT <: FieldTypeT](
    val className: SerializedEntity.TypeRef,
    val fields: SortedMap[String, FT],
    val isCanonical: Boolean)
    extends Serializable {
  override final def toString(): String =
    s"${getClass.getSimpleName}(className=$className, fields=$fields, isCanonical=$isCanonical)"

  override def equals(other: Any): Boolean = other match {
    case s: AbstractShape[_] =>
      if (s eq this)
        true
      else
        (s ne null) && className == s.className && fields == s.fields
    case _ => false
  }

  private lazy val _hashCode = className.hashCode + 41 * fields.hashCode

  override def hashCode(): Int = {
    _hashCode
  }
}

class Shape(className: SerializedEntity.TypeRef, fields: SortedMap[String, FieldType], isCanonical: Boolean)
    extends AbstractShape[FieldType](className, fields, isCanonical)

object Shape {
  def fromProperties(className: SerializedEntity.TypeRef, fields: PickledProperties): Shape = {
    val fieldTypes: SortedMap[String, FieldType] = fields.iterator
      .map { case (name, value) =>
        (name, FieldType.fromPickledValue(value))
      }
      .convertTo(SortedMap)
    new Shape(className, fieldTypes, false)
  }
}

class RftShape(
    className: SerializedEntity.TypeRef,
    fields: SortedMap[String, RegisteredFieldTypeT],
    isCanonical: Boolean)
    extends AbstractShape[RegisteredFieldTypeT](className, fields, isCanonical)
    with Serializable {
  def generateClientSideShape(): Shape = {
    val fieldTypes = fields map { case (name, value) =>
      (name, value.toFieldType())
    }
    new Shape(className, fieldTypes, isCanonical)
  }
}

object RftShape {
  def apply(
      className: SerializedEntity.TypeRef,
      fields: SortedMap[String, RegisteredFieldTypeT],
      isCanonical: Boolean) =
    new RftShape(className, fields, isCanonical)
}
