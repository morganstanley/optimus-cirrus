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
package optimus.platform.dal

import msjava.base.util.uuid.MSUuid
import optimus.platform._
import optimus.platform.storable.Entity

import java.time.Instant
import optimus.core.CoreAPI
import optimus.platform.dal.Transaction.CommandCreatorResult
import optimus.platform.dsi.bitemporal.WriteCommand
import optimus.platform.dsi.bitemporal.PutApplicationEvent
import optimus.platform.dsi.bitemporal.WriteBusinessEvent
import optimus.platform.storable.EntityReferenceHolder
import optimus.platform.storable.EntityVersionHolder

object DALBlock {

  /**
   * All PluginTagKeys for DALScenario subclasses. Any time a leaf class is added, an entry needs to be added here.
   */
  val persistBlockKeys: Seq[PluginTagKey[_]] = Seq(BasicPersistBlock, AppEventBlock)

  /**
   * Check if we're in a persist block by checking all persistBlockKeys.
   *
   * @return
   *   Whether or not we're in a persist block.
   */
  def hasPersistBlock: Boolean = persistBlockKeys.exists(EvaluationContext.scenarioStack.hasPluginTag(_))

  /**
   * Execute a DAL scenario.
   *
   * @param sc
   *   The Scenario.
   * @param plugin
   *   The PluginTagKeyValue, containing both the persist block data and the proper key.
   * @param f
   *   The operation to execute.
   * @tparam A
   *   The result type
   * @return
   *   The result and the persistence result.
   */
  @async def execute[A, T <: DALBlock](
      sc: Scenario,
      plugin: PluginTagKeyValue[T],
      f: AsyncFunction0[A]): (A, PersistResult) = {
    if (hasPersistBlock) throw new UnsupportedOperationException("persist and correct blocks cannot be nested")

    val nt = AdvancedUtils.impure {
      AdvancedUtils.givenWithPluginTag(plugin.key, plugin.value, sc) {
        CoreAPI.asyncResult {
          val ret = f()
          val pr = plugin.value.flush()
          (ret, pr)
        }
      }
    }
    plugin.value.dispose()
    nt.value
  }

  /**
   * Execute a DAL scenario which is persisted at a different time.
   *
   * @param sc
   *   The Scenario.
   * @param ms
   *   The AppEventBlock metadata.
   * @param f
   *   The operation to execute.
   * @tparam A
   *   The result type
   * @return
   *   The result and the persistence result.
   */
  @async def executeDelayed[A](
      sc: Scenario,
      ms: AppEventBlock with DelayedDALScenario,
      f: AsyncFunction0[A]): Transaction = {
    if (hasPersistBlock) throw new UnsupportedOperationException("persist and correct blocks cannot be nested")
    AdvancedUtils.givenWithPluginTag(AppEventBlock, ms, sc) {
      f()
      ms.flushNoSave()
    }
  }

  def scenario(initTweaks: Iterable[Tweak]): Scenario = Scenario(initTweaks)
}

class PersistResult(
    val tt: Instant,
    private val entityRefHolders: Map[Entity, EntityReferenceHolder[Entity]] = Map.empty,
    val isEmptyTransaction: Boolean = false) {

  def getEntityReferenceHolder[T <: Entity](entity: T): Option[EntityReferenceHolder[T]] = {
    entityRefHolders.get(entity).asInstanceOf[Option[EntityReferenceHolder[T]]]
  }

  def getStoreContext[T <: Entity](entity: T): Option[TemporalContext] = {
    entityRefHolders.get(entity).map(_.tc)
  }

  override def equals(obj: Any): Boolean = obj match {
    case pr: PersistResult =>
      pr.tt == this.tt && pr.isEmptyTransaction == this.isEmptyTransaction && pr.entityRefHolders == this.entityRefHolders
    case _ => false
  }

  override def hashCode(): Int = {
    var h = tt.##
    h = 37 * h + entityRefHolders.##
    h = 53 * h + isEmptyTransaction.##
    h
  }
}

object PersistResult {
  def apply(
      tt: Instant,
      entityRefHolders: Map[Entity, EntityReferenceHolder[Entity]] = Map.empty,
      isEmptyTransaction: Boolean = false): PersistResult = {
    new PersistResult(tt, entityRefHolders, isEmptyTransaction)
  }

  def unapply(arg: PersistResult): Option[(Instant, Map[Entity, EntityReferenceHolder[Entity]])] = Some(
    (arg.tt, arg.entityRefHolders))
}

/**
 * Plugin tags that can be turned into a set of DAL commands. There should only ever be one instance of any given
 * subclass of this class in a given scenario stack at once. It is an error if anyone tries to create another one when
 * one already exists.
 */
abstract class DALBlock {

  /**
   * Execute the commands represented by this DALScenario.
   *
   * @return
   *   The result of persisting this DALScenario.
   */
  @async def flush(): PersistResult

  /**
   * Create a set of commands for the DALScenario, but return them instead of committing them.
   *
   * @return
   *   The commands represented by the state of this DALScenario.
   */
  def createCommands(): Seq[WriteCommand]

  /**
   * Reset this DALScenario and delete all saved state.
   */
  private[optimus /*dal*/ ] def dispose(): Unit
}

trait DelayedDALScenario { self: DALBlock =>
  def flushNoSave(): Transaction
}

sealed trait BatchExecutable {
  private[dal] def dispose(): Unit
}

object Transaction {
  final case class CommandCreatorResult(pae: PutApplicationEvent, cmdToEntityMap: Map[WriteBusinessEvent.Put, Entity])
}

/** Intentionally not @{case class} to have original equals/hashCode semantics  * */
class Transaction protected[dal] (
    commandCreator: AsyncFunction0[CommandCreatorResult],
    onDispose: () => Unit,
    private[optimus /*dal*/ ] val writeMass: Int,
    val clientTxTime: Option[Instant])
    extends BatchExecutable {
  @async private[dal] def createCommand(): CommandCreatorResult = commandCreator()
  private[dal] override def dispose() = onDispose()
  override def toString = s"${super.toString}(writeMass: ${writeMass}, clientTxTime: ${clientTxTime})"
}

final class TransactionSequence(val transactions: Seq[Transaction]) extends BatchExecutable {
  private[dal] override def dispose() = transactions foreach { _.dispose() }
}

abstract class PersistBlock extends DALBlock with PersistBlockUnsafe {
  def persist[E <: Entity](entity: E): E
  def persist[E <: Entity](entity: E, vt: Instant): E
  def put(entity: Entity): Unit
  def put(entity: Entity, cmid: MSUuid): Unit
  def put(entity: Entity, vt: Instant): Unit
  def upsert(entity: Entity): Unit
  def upsert(entity: Entity, cmid: MSUuid): Unit
  def upsert(entity: Entity, vt: Instant): Unit
  def invalidate(e: Entity): Unit
  def invalidate(e: Entity, vt: Instant): Unit
  def replace[E <: Entity](existing: E, updated: E): Unit
  def revert(entity: Entity): Unit = throw new IllegalArgumentException(
    s"revert not supported on block: ${this.getClass.getCanonicalName}")
}

trait PersistBlockUnsafe {
  private[optimus /*platform*/ ] def unsafeInvalidate[E <: Entity: Manifest](holder: EntityVersionHolder[E]): Unit
  private[optimus /*platform*/ ] def unsafeInvalidate[E <: Entity: Manifest](holder: EntityReferenceHolder[E]): Unit
}

object PersistBlock {
  def scenario[D <: PersistBlock](initTweaks: Iterable[Tweak]): Scenario = DALBlock.scenario(initTweaks)
}
