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

import optimus.platform.storable.PersistentEntity
import optimus.platform.dsi.bitemporal.proto.Dsi.PersistentEntityWithTemporalContextProto
import java.time.Instant
import net.iharder.Base64

object PersistentEntitiesWithTemporalContextImpl {

  def apply(buf: String) = ??? /* {
    PersistentEntityWithTemporalContextProto.parseFrom(Base64.decode(buf, Base64.DONT_GUNZIP))
  } */

  def unapply(buf: PersistentEntityWithTemporalContextProto): String = {
    Base64.encodeBytes(buf.toByteArray())
  }
}

// PersistentEntityWithTemporalContextProto protocol buffers are only used as a work-around for specific performance
// issues and to send entities via GPB from one client side application to another; they are *not* used in the optimus
// code for client-server communication!
class PersistentEntitiesWithTemporalContextImpl(
    val entities: Iterable[PersistentEntity],
    val validTime: Instant,
    val txTime: Instant,
    val cascadedEntities: Iterable[PersistentEntity],
    clsName: Option[String] = None) {

  require(!entities.isEmpty)

  def toProto = PersistentEntitiesWithTemporalContextImplSerializer.serialize(this)
  def className = clsName.getOrElse(entities.head.className)
}
