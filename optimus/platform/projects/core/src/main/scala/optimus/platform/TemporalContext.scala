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
package optimus.platform

import java.time.Instant
import optimus.exceptions.GenericRTException
import optimus.graph.Node
import optimus.graph.NodeFuture
import optimus.platform.annotations.nodeSync
import optimus.platform.storable.Entity
import optimus.platform.storable.PersistentEntity
import optimus.platform.storable.StorageInfo
import optimus.platform.temporalSurface.operations.TemporalSurfaceQuery
import optimus.platform.util.RuntimeServiceLoader
import optimus.platform.temporalSurface.FixedLeafTemporalContext
import optimus.platform.temporalSurface.TemporalSurface

private[optimus] object TemporalContextPermittedListTag extends ForwardingPluginTagKey[String]

sealed trait HasTTContext extends Serializable {
  def ttContext: TransactionTimeContext
}

trait TransactionTimeContext extends HasTTContext with Serializable {
  private[optimus] def unsafeTxTime: Instant = throw new Exception("illegal call to unsafeTxTime on a " + this)

  final def ttContext: TransactionTimeContext = this
  private[optimus] def getTTForEvent(cls: String): Instant

  /**
   * returns a TransactionTimeContext with the same structure as this, but not ticking If this is not ticking then
   * returns this
   */
  private[optimus] def frozen: TransactionTimeContext
}

sealed trait ValidTimeContext extends Serializable {
  private[optimus] def unsafeValidTime: Instant = throw new Exception("illegal call to unsafeValidTime on a " + this)

  /**
   * returns a ValidTimeContext with the same structure as this, but not ticking If this is not ticking then returns
   * this
   */
  private[optimus] def frozen: ValidTimeContext
}

// Rename? BitemporalContext?
trait TemporalContext extends TemporalSurface with HasTTContext {
  final override def addToChain(temporalContextChain: List[TemporalContext]): List[TemporalContext] =
    this :: temporalContextChain

  final override def acceptDelegation = true

  def unsafeValidTime: Instant =
    throw new GenericRTException(
      s"illegal call to unsafeValidTime on a ${this}. Node Stack: ${Node.getTraceForNodeAsString(EvaluationContext.currentNode)}")
  def unsafeTxTime: Instant =
    throw new GenericRTException(
      s"illegal call to unsafeTxTime on a ${this}. Node Stack: ${Node.getTraceForNodeAsString(EvaluationContext.currentNode)}")

  @nodeSync
  private[optimus] def deserialize(pe: PersistentEntity, storageInfo: StorageInfo): Entity
  private[optimus] def deserialize$queued(pe: PersistentEntity, storageInfo: StorageInfo): NodeFuture[Entity]

  /**
   * Actually executes the query operation against the DAL at appropriate temporalities and returns the results
   * (typically entities or entity references).
   *
   * Internally this method first queries the temporal context to find the appropriate temporalities for the operation,
   * then tries to resolve any ambiguities by querying the DAL for actual entity information (if necessary), and then
   * runs the query operation itself against the DAL.
   */
  @nodeSync
  private[optimus] def dataAccess(operation: TemporalSurfaceQuery): operation.ResultType
  private[optimus] def dataAccess$queued(operation: TemporalSurfaceQuery): NodeFuture[operation.ResultType]

  override type surfaceForFrozenType <: TemporalContext
}

trait FlatTemporalSurfaceFactory {
  private[optimus] def createTemporalContext(vt: Instant, tt: Instant, tag: Option[String]): FixedLeafTemporalContext
}
private[optimus] object FlatTemporalSurfaceFactory extends RuntimeServiceLoader[FlatTemporalSurfaceFactory] {
  def createTemporalContext(vt: Instant, tt: Instant, tag: Option[String]): FixedLeafTemporalContext =
    service.createTemporalContext(vt, tt, tag)
}

private[optimus] final case class FixedTransactionTimeContext(override val unsafeTxTime: Instant)
    extends TransactionTimeContext {
  private[optimus] def frozen = this
  private[optimus] override def getTTForEvent(cls: String) = unsafeTxTime
}

private[optimus] final case class FixedValidTimeContext(override val unsafeValidTime: Instant)
    extends ValidTimeContext {
  private[optimus] def frozen = this
}
