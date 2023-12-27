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
package optimus.buildtool.cache.silverking

import optimus.buildtool.cache.silverking.SilverKingOperations.Namespace
import optimus.buildtool.cache.silverking.SilverKingOperations.RequestInfo
import optimus.buildtool.cache.silverking.SilverKingOperationsImpl.Config
import optimus.buildtool.cache.silverking.SilverKingStore.StoredKey
import optimus.core.CoreAPI
import optimus.graph.CompletableNode
import optimus.graph.GroupingNodeBatcherSchedulerPlugin
import optimus.graph.NodeTask
import optimus.graph.OGSchedulerContext
import optimus.graph.Scheduler
import optimus.graph.SchedulerPlugin
import optimus.platform._
import optimus.platform.dal.config.DalAsyncBatchingConfigProperties
import optimus.platform.dsi.protobufutils.AsyncBatchingOutOfWorkQueueBase
import optimus.platform.dsi.protobufutils.AsyncBatchingQueue
import optimus.platform.dsi.protobufutils.RequestBatcher
import optimus.platform.util.Log

object SilverKingBatching extends Log {

  def setup(): Unit = {
    if (Config.batchRequests) {
      if (Config.advancedRequestBatching) {
        log.info(s"SilverKing advanced batching enabled (config: ${AdvancedSilverKingBatching.config})")
        SilverKingOperationsImpl.readUntyped.setPlugin(new AdvancedSilverKingBatching.ReadBatcher)
        SilverKingOperationsImpl.writeUntyped.setPlugin(new AdvancedSilverKingBatching.WriteBatcher)
        SilverKingOperationsImpl.validKeysUntyped.setPlugin(new AdvancedSilverKingBatching.ValidKeysBatcher)
      } else {
        log.info(s"SilverKing batching enabled (max auto batch size: ${Config.maxAutoBatchSize})")
        SilverKingOperationsImpl.readUntyped.setPlugin(new SilverKingBatching.ReadBatcher(Config.maxAutoBatchSize))
        SilverKingOperationsImpl.writeUntyped.setPlugin(new SilverKingBatching.WriteBatcher(Config.maxAutoBatchSize))
        SilverKingOperationsImpl.validKeysUntyped.setPlugin(
          new SilverKingBatching.ValidKeysBatcher(Config.maxAutoBatchSize)
        )
      }
    } else {
      log.info(s"SilverKing batching disabled")
    }
  }

  private[cache] abstract class SimpleBatcher[T, NodeT <: CompletableNode[T]](maxAutoBatchSize: Int)
      extends GroupingNodeBatcherSchedulerPlugin[T, NodeT] {
    this.maxBatchSize = maxAutoBatchSize

    override type PerScenarioInfoT = Null
    override type GroupKeyT = (SilverKingOperationsImpl, Namespace)
  }

  private[cache] class ReadBatcher(maxAutoBatchSize: Int)
      extends SimpleBatcher[Option[Any], SilverKingOperationsImpl#readUntyped$node](maxAutoBatchSize) {

    override protected def getGroupBy(
        psinfo: Null,
        inputNode: SilverKingOperationsImpl#readUntyped$node
    ): (SilverKingOperationsImpl, Namespace) =
      (inputNode.entity, inputNode.n)

    @node override def run(
        groupKey: (SilverKingOperationsImpl, Namespace),
        inputs: Seq[SilverKingOperationsImpl#readUntyped$node]
    ): Seq[Option[Any]] = {
      val (ops, namespace) = groupKey
      val infos = RequestInfo.merge(inputs.map(_.r))
      val keys = inputs.map(_.key)
      val result = ops.readAllUntyped(infos, namespace)(keys)
      keys.map(result.get(_).flatten)
    }
  }

  private[cache] class WriteBatcher(maxAutoBatchSize: Int)
      extends SimpleBatcher[Unit, SilverKingOperationsImpl#writeUntyped$node](maxAutoBatchSize) {

    override protected def getGroupBy(
        psinfo: Null,
        inputNode: SilverKingOperationsImpl#writeUntyped$node
    ): (SilverKingOperationsImpl, Namespace) =
      (inputNode.entity, inputNode.n)

    @node override def run(
        groupKey: (SilverKingOperationsImpl, Namespace),
        inputs: Seq[SilverKingOperationsImpl#writeUntyped$node]
    ): Seq[Unit] = {
      val (ops, namespace) = groupKey
      val infos = RequestInfo.merge(inputs.map(_.r))
      val entries = inputs.map(i => (i.key, i.value))
      ops.writeAllUntyped(infos, namespace)(entries)
      entries.map(_ => ())
    }
  }

  private[cache] class ValidKeysBatcher(maxAutoBatchSize: Int)
      extends SimpleBatcher[Set[StoredKey], SilverKingOperationsImpl#validKeysUntyped$node](maxAutoBatchSize) {

    override protected def getGroupBy(
        psinfo: Null,
        inputNode: SilverKingOperationsImpl#validKeysUntyped$node
    ): (SilverKingOperationsImpl, Namespace) =
      (inputNode.entity, inputNode.n)

    @node override def run(
        groupKey: (SilverKingOperationsImpl, Namespace),
        inputs: Seq[SilverKingOperationsImpl#validKeysUntyped$node]
    ): Seq[Set[StoredKey]] = {
      val (ops, namespace) = groupKey
      val infos = RequestInfo.merge(inputs.map(_.r))
      val keyInputs = inputs.map(_.keys)
      val result = ops._validKeysUntyped(infos, namespace)(keyInputs.flatten)
      keyInputs.map(ks => result intersect ks.toSet)
    }
  }
}

object AdvancedSilverKingBatching extends Log {
  val config = new DalAsyncBatchingConfigProperties("buildtool.silverking").apply(async = true)
  val batcherConfig = RequestBatcher.Configuration("optimus.buildtool.silverking", config.async, () => ())

  sealed trait Batchable[A] {
    def node: CompletableNode[A]
    def ops: SilverKingOperationsImpl
    def requestInfo: RequestInfo
    def namespace: Namespace
    def scenarioStack: ScenarioStack
    def scheduler: Scheduler
  }

  final case class BatchKey(
      ops: SilverKingOperationsImpl,
      namespace: Namespace,
      scenarioStack: ScenarioStack,
      scheduler: Scheduler
  )

  class OutOfWorkQueue[A, B >: Null <: Batchable[A]]
      extends AsyncBatchingOutOfWorkQueueBase[B](
        config.batchAccumulationWaitTime,
        config.maxBatchSize,
        config.maxBatchBuildTime
      ) {
    override protected val outOfWorkToken: B = null
    override protected def shutdownWithLock(): Unit = {}
    override protected def offerOnShutdown(req: B): Unit = {}
  }

  class Queue[A, B >: Null <: Batchable[A]]
      extends AsyncBatchingQueue[B](
        config.batchAccumulationWaitTime,
        config.maxBatchSize,
        config.maxBatchBuildTime
      )

  object Queue {
    def apply[A, B >: Null <: Batchable[A]](): AsyncBatchingQueue[B] =
      if (config.useOutOfWorkListener) new OutOfWorkQueue[A, B]
      else new Queue[A, B]
  }

  abstract class SilverKingRequestBatcher[A, B >: Null <: Batchable[A], C]
      extends RequestBatcher[B, BatchKey, Seq[B]](Queue[A, B](), batcherConfig) {
    override protected def filterRequests(
        batchables: Seq[B]
    ): Seq[(BatchKey, Seq[B])] =
      batchables.groupBy { b =>
        BatchKey(b.ops, b.namespace, b.scenarioStack, b.scheduler)
      }.toSeq

    override protected def sendBatch(batchKey: BatchKey, batch: Seq[B]): Unit = {
      if (batch.nonEmpty) {
        val info = RequestInfo.merge(batch.map(_.requestInfo))

        val batchNode = CoreAPI.nodify(executeBatch(batchKey, info, batch))
        batchNode.attach(batchKey.scenarioStack)
        batch.foreach(_.node.setWaitingOn(batchNode))
        batchKey.scheduler.enqueue(batchNode)

        batchNode.continueWith(
          (eq: EvaluationQueue, _: NodeTask) => {
            batch.foreach { q =>
              q.node.setWaitingOn(null)
              q.node.combineInfo(batchNode, eq)
            }
            if (batchNode.isDoneWithResult) {
              val res = batchNode.result
              batch.foreach { b =>
                b.node.completeWithResult(resultFor(res, b), eq)
              }
            } else {
              batch.foreach(_.node.completeWithException(batchNode.exception(), eq))
            }
          },
          batchKey.scheduler
        )
      }
    }

    override protected def handleInterrupt(remaining: Seq[(BatchKey, Seq[B])]): Unit = {}
    override protected def shutdown(): Unit = {}

    @async protected def executeBatch(batchKey: BatchKey, batchInfo: RequestInfo, batch: Seq[B]): C

    protected def resultFor(result: C, batchable: B): A
  }

  final case class Read(
      node: SilverKingOperationsImpl#readUntyped$node,
      ops: SilverKingOperationsImpl,
      requestInfo: RequestInfo,
      namespace: Namespace,
      key: StoredKey,
      scenarioStack: ScenarioStack,
      scheduler: Scheduler
  ) extends Batchable[Option[Any]]

  class ReadBatcher extends SchedulerPlugin {
    val batcher = new SilverKingRequestBatcher[Option[Any], Read, Map[StoredKey, Option[Any]]] {
      @async override protected def executeBatch(
          batchKey: BatchKey,
          batchInfo: RequestInfo,
          batch: Seq[Read]
      ): Map[StoredKey, Option[Any]] =
        batchKey.ops.readAllUntyped(batchInfo, batchKey.namespace)(batch.map(_.key))

      override protected def resultFor(result: Map[StoredKey, Option[Any]], batchable: Read): Option[Any] =
        result.get(batchable.key).flatten
    }
    batcher.start()

    override def adapt(n: NodeTask, ec: OGSchedulerContext): Boolean = n match {
      case r: SilverKingOperationsImpl#readUntyped$node =>
        batcher.offer(Read(r, r.entity, r.r, r.n, r.key, r.scenarioStack, ec.scheduler))
        true
      case _ =>
        log.warn(s"Received unsupported task type: $n")
        false
    }
  }

  final case class Write(
      node: SilverKingOperationsImpl#writeUntyped$node,
      ops: SilverKingOperationsImpl,
      requestInfo: RequestInfo,
      namespace: Namespace,
      key: StoredKey,
      value: Any,
      scenarioStack: ScenarioStack,
      scheduler: Scheduler
  ) extends Batchable[Unit]

  class WriteBatcher extends SchedulerPlugin {
    val batcher = new SilverKingRequestBatcher[Unit, Write, Unit] {
      @async override protected def executeBatch(
          batchKey: BatchKey,
          batchInfo: RequestInfo,
          batch: Seq[Write]
      ): Unit =
        batchKey.ops.writeAllUntyped(batchInfo, batchKey.namespace)(batch.map(b => (b.key, b.value)))

      override protected def resultFor(result: Unit, batchable: Write): Unit = ()
    }
    batcher.start()

    override def adapt(n: NodeTask, ec: OGSchedulerContext): Boolean = n match {
      case w: SilverKingOperationsImpl#writeUntyped$node =>
        batcher.offer(Write(w, w.entity, w.r, w.n, w.key, w.value, w.scenarioStack, ec.scheduler))
        true
      case _ =>
        log.warn(s"Received unsupported task type: $n")
        false
    }
  }

  final case class ValidKeys(
      node: SilverKingOperationsImpl#validKeysUntyped$node,
      ops: SilverKingOperationsImpl,
      requestInfo: RequestInfo,
      namespace: Namespace,
      keys: Iterable[StoredKey],
      scenarioStack: ScenarioStack,
      scheduler: Scheduler
  ) extends Batchable[Set[StoredKey]]

  class ValidKeysBatcher extends SchedulerPlugin {
    val batcher = new SilverKingRequestBatcher[Set[StoredKey], ValidKeys, Set[StoredKey]] {
      @async override protected def executeBatch(
          batchKey: BatchKey,
          batchInfo: RequestInfo,
          batch: Seq[ValidKeys]
      ): Set[StoredKey] =
        batchKey.ops._validKeysUntyped(batchInfo, batchKey.namespace)(batch.flatMap(_.keys))

      override protected def resultFor(result: Set[StoredKey], batchable: ValidKeys): Set[StoredKey] =
        result intersect batchable.keys.toSet
    }
    batcher.start()

    override def adapt(n: NodeTask, ec: OGSchedulerContext): Boolean = n match {
      case v: SilverKingOperationsImpl#validKeysUntyped$node =>
        batcher.offer(ValidKeys(v, v.entity, v.r, v.n, v.keys, v.scenarioStack, ec.scheduler))
        true
      case _ =>
        log.warn(s"Received unsupported task type: $n")
        false
    }
  }
}
