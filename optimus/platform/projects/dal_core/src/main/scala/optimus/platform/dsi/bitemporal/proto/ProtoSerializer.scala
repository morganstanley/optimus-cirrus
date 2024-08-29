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

import scala.annotation.implicitNotFound
import com.google.protobuf.MessageLite

trait ProtoSerializerTo[T, TProto <: MessageLite] {
  def serialize(obj: T): TProto
}
trait ProtoSerializerFrom[T, TProto <: MessageLite] {
  def deserialize(proto: TProto): T
}

@implicitNotFound(msg = "Type ${T} and ${TProto} don't seem to support protobuf serialization (can't find serializer).")
trait ProtoSerializer[T, TProto <: MessageLite] extends ProtoSerializerTo[T, TProto] with ProtoSerializerFrom[T, TProto]
