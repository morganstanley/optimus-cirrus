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
import scala.collection.mutable

//
// We unfortunately have become dependent on the ordering behaviour of Scala 2.12 collections
// And specific Map types that were used in the DAL pickling implementations.
// We are sensitive to field order in any types (e.g. @embeddable) that are used as part of
// @key or @indexed properties. While SortedPropertyValues deals with ordering of the properties
// by fieldname, the values themselves also have to remain constant in terms of field order.
// For example, For the class session.SlotMetadata. The @key is represented as
//
// @embeddable final case class Key(className: SerializedEntity.TypeRef, slotNumber: Int)
//
// When SlotMetadata is pickled and eventually written to the DAL, a hash of the json generated
// from the pickled form is used as the mongodb primary key and this is sensitive to the
// order of the fields in the pickled form. In the case of this class, the order needs to be
// className,_tag, slotNumber.
//
// This order is a side effect of the code path and we must preserve this same order within any
// pickling format we may want to use.
// This order is the effect of using the below newBuilder specifically.
//
object PicklingMapEntryOrderWorkaround {

  // Map.newBuilder[String, Any] would seem more sensible here, but that
  // can result in different iteration order for small Maps.
  def newBuilder[T]: mutable.Builder[(String, T), Map[String, T]] = mutable.Map.newBuilder[String, T].mapResult(_.toMap)

  def newMapForNames(names: Array[String], addTag: Boolean): Map[String, Int] = {
    val map = newBuilder[Int]
    names.zipWithIndex.foreach { case (n, i) => map += (n -> i) }
    if (addTag) map += (PicklingConstants.embeddableTag -> -1)
    map.result()
  }
}
