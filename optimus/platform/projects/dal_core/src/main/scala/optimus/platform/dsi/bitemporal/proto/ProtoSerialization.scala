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
package optimus.platform.dsi.bitemporal.proto

import com.google.protobuf.MessageLite

import scala.collection.mutable

trait ProtoSerializationTo {
  final def toProto[T, TProto <: MessageLite](obj: T)(implicit s: ProtoSerializerTo[T, TProto]): TProto =
    s.serialize(obj)
}
trait ProtoSerializationFrom {
  final def fromProto[T, TProto <: MessageLite](proto: TProto)(implicit s: ProtoSerializerFrom[T, TProto]): T =
    s.deserialize(proto)
}
trait ProtoSerialization extends ProtoSerializationTo with ProtoSerializationFrom

object ProtoSerialization {
  def distinctBy[T, U](xs: Iterator[T], f: T => U): Iterator[T] = {
    val seen = mutable.Set[U]()
    xs.filter { x =>
      val y = f(x)
      if (!seen(y)) {
        seen += y
        true
      } else false
    }
  }
}
