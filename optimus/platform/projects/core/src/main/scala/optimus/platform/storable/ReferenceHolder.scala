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

import optimus.graph.NodeFuture
import optimus.platform.TemporalContext
import optimus.platform.annotations._

/**
 */
trait ReferenceHolder[T <: Storable] {
  @nodeSync def payload: T
  def payload$queued: NodeFuture[T]

  /**
   * the most specific type of the underlying reference that is known. This result is not stable, if a more exact type
   * is determined, for example by another caller calling the payload, then subsequent calls to this method may return a
   * refined, and more exact definition
   */
  def payloadType: Option[HolderType[_ <: T]]
}

trait VersionHolder[T <: Storable] {
  def creationContext: TemporalContext
  @nodeSync def payloadAt(tc: TemporalContext): T
  def payloadAt$queued(tc: TemporalContext): NodeFuture[T]
  @nodeSync def payloadOptionAt(tc: TemporalContext, entitledOnly: Boolean = false): Option[T]
  def payloadOptionAt$queued(tc: TemporalContext, entitledOnly: Boolean = false): NodeFuture[Option[T]]
}

object HolderType {
  def unapply[T <: Storable](holder: HolderType[T]): Some[(Boolean, Class[_ <: T])] =
    Some((holder.concrete, holder.clazz))

  def apply[T <: Storable](concrete: Boolean, clazz: Class[T]) =
    if (concrete) ConcreteHolderType(clazz) else AbstractHolderType(clazz)
}
sealed trait HolderType[T <: Storable] {
  def concrete: Boolean
  val clazz: Class[T]
}
final case class ConcreteHolderType[T <: Storable](val clazz: Class[T]) extends HolderType[T] {
  override def concrete: Boolean = true
}
final case class AbstractHolderType[T <: Storable](val clazz: Class[T]) extends HolderType[T] {
  override def concrete: Boolean = false
}
