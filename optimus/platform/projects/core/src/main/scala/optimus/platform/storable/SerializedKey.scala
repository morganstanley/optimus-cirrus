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
package optimus.platform.storable
import com.fasterxml.jackson.annotation.JsonCreator

class SerializedKey(
    val typeName: String,
    val properties: SortedPropertyValues,
    val unique: Boolean,
    val indexed: Boolean,
    val refFilter: Boolean = false,
    // TODO (OPTIMUS-43113): we will remove this once we have a strategic solution
    val serializedSizeOpt: Option[Int] = None /*this is not used in 'equals' & 'hashCode'*/ )
    extends Serializable {
  if (refFilter) require(indexed && !unique, "refFilter is only supported for non unique indexed")
  require(typeName ne null, "type name should not be null")

  /**
   * Since we serialize properties as Map we need to constructor to deserialize
   * Note: names here must match the names of the val declarations above!
   */
  @JsonCreator
  def this(
      typeName: String,
      properties: Map[String, Any],
      unique: Boolean,
      indexed: Boolean,
      refFilter: Boolean,
      serializedSizeOpt: Option[Int]) =
    this(typeName, SortedPropertyValues(properties), unique, indexed, refFilter, serializedSizeOpt)

  def isKey: Boolean = unique && !indexed

  override def equals(o: Any): Boolean = o match {
    case sk: SerializedKey => typeName == sk.typeName && properties == sk.properties
    case _                 => false
  }

  /**
   * Compares the meta types of keys. The meta type is the type name and property names. Property values are excluded.
   */
  def compareMetaType(that: SerializedKey): Int = {
    typeName.compareTo(that.typeName) match {
      case 0 => properties.compareNames(that.properties)
      case r => r
    }
  }

  override lazy val hashCode: Int = typeName.hashCode * 31 + properties.hashCode

  def copy(newProps: SortedPropertyValues): SerializedKey =
    new SerializedKey(typeName, newProps, unique, indexed, refFilter)

  private def refFilterStr = if (indexed && !unique) s" , refFilter: $refFilter" else ""
  override def toString =
    s"SerializedKey($typeName, [${properties.mkString(", ")}], unique: $unique, indexed: $indexed$refFilterStr)"
}

object SerializedKey {
  def apply(
      typeName: String,
      props: collection.Seq[(String, Any)],
      unique: Boolean = true,
      indexed: Boolean = false,
      refFilter: Boolean = false,
      serializedSizeOpt: Option[Int] = None): SerializedKey =
    new SerializedKey(typeName, SortedPropertyValues(props), unique, indexed, refFilter, serializedSizeOpt)

  implicit val ordering: Ordering[SerializedKey] = (l: SerializedKey, r: SerializedKey) => l.compareMetaType(r)
}
