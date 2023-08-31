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
package optimus.breadcrumbs.crumbs

import optimus.scalacompat.collection._
import spray.json._

import scala.collection.mutable
import scala.language.implicitConversions

/**
 * Represents a tree structure of string keys and arbitrary values. Maps directly to JsObjects. Useful for sending
 * dynamic but tree-like data in Crumb propertioes.
 */
sealed trait NestedMapOrValue[+V]
object NestedMapOrValue {
  final case class NestedMap[+V](map: Map[String, NestedMapOrValue[V]]) extends NestedMapOrValue[V]
  final case class Value[+V](value: V) extends NestedMapOrValue[V]

  object NestedMap {
    def apply[V](pairs: (String, NestedMapOrValue[V])*): NestedMap[V] = NestedMap[V](pairs.toMap)
  }

  /**
   * Converts from a flat dotted-key structure to a nested simple-key structure,
   *
   * i.e. from { a.b.c -> 1, a.b.d -> 2, a.x -> 3, b -> 4 } to { { a -> { b -> { c -> 1, d -> 2 }, x -> 3 }, b -> 4 }
   */
  def fromDottedKeys[V](dotted: Map[String, V]): NestedMapOrValue[V] = {
    val nested = mutable.Map[String, Any]()
    // first convert from dotted keys to mutable maps of mutable maps
    dotted.foreach { case (key, value) =>
      val keyParts = key.split('.')
      val leafMap = {
        keyParts.init.foldLeft(nested) { (subMap, keyPart) =>
          subMap.getOrElseUpdate(keyPart, mutable.Map[String, Any]()) match {
            case m: mutable.Map[String, Any] @unchecked => m
            case _                                      =>
              // this happens if a value was already inserted at a path which then try to create subpaths under
              throw new IllegalArgumentException(s"Conflicting paths at $key")
          }
        }
      }
      if (leafMap.put(keyParts.last, value).isDefined) {
        // this happens if we try to insert a value at a path which we already created subpaths under
        throw new IllegalArgumentException(s"Conflicting paths at $key")
      }
    }

    // then convert mutable maps of maps to the immutable NestedMapOrValues
    def recurse(m: collection.Map[String, Any]): NestedMapOrValue[V] = {
      NestedMap[V](m.mapValuesNow {
        case m: collection.Map[String, Any] @unchecked => recurse(m)
        case v: V @unchecked                           => Value(v)
      }.toMap)
    }

    recurse(nested)
  }

  implicit def jsonFormat[V: JsonWriter: JsonReader]: JsonFormat[NestedMapOrValue[V]] =
    new JsonFormat[NestedMapOrValue[V]] {
      override def write(obj: NestedMapOrValue[V]): JsValue = obj match {
        case NestedMap(map) => JsObject(map.map { case (k, v) => (k, v.toJson) })
        case Value(v)       => v.toJson
      }
      override def read(json: JsValue): NestedMapOrValue[V] = json match {
        case JsObject(fields) => NestedMap(fields.mapValuesNow(read))
        case jv               => Value(implicitly[JsonReader[V]].read(jv))
      }
    }
}
