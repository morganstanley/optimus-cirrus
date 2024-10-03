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
package optimus.dsi.trace

import java.net.URI
import optimus.breadcrumbs.BreadcrumbLevel
import optimus.breadcrumbs.crumbs.EventCrumb
import optimus.platform.dal.config.Host
import optimus.platform.storable.SerializedEntity

object TraceLevel {
  sealed trait Level
  case object Info extends Level
  case object Debug extends Level

  private[trace] implicit def toCrumbLevel(level: Level): BreadcrumbLevel.Level = {
    level match {
      case Info  => BreadcrumbLevel.Info
      case Debug => BreadcrumbLevel.Debug
    }
  }
}

object TraceRelevance {
  sealed trait Relevance extends Ordered[Relevance] {
    protected def value: Int
    override def compare(that: Relevance): Int = this.value.compare(that.value)
  }
  // One is the least relevant, Ten is the most relevant
  object One extends Relevance { val value = 1 }
  object Two extends Relevance { val value = 2 }
  object Three extends Relevance { val value = 3 }
  object Four extends Relevance { val value = 4 }
  object Five extends Relevance { val value = 5 }
  object Six extends Relevance { val value = 6 }
  object Seven extends Relevance { val value = 7 }
  object Eight extends Relevance { val value = 8 }
  object Nine extends Relevance { val value = 9 }
  object Ten extends Relevance { val value = 10 }
}

object Tracing {
  sealed trait Phase { def name: String }
  object Received extends Phase { val name = "Received" }
  object Queued extends Phase { val name = "Queued" }
  object Blocked extends Phase { val name = "Blocked" }
  object Execution extends Phase { val name = "Execution" }
  object Completed extends Phase { val name = "Completed" }
}

object TraceBackEnd {
  sealed trait BackEnd { def name: String }
  case object Mongo extends BackEnd { val name = "MONGO" }
  case object InMemory extends BackEnd { val name = "InMemory" }
  case object Postgres extends BackEnd { val name = "POSTGRES" }
  case object Ddc extends BackEnd { val name = "DDC" }
  case object Dpc extends BackEnd { val name = "DPC" }
  case object SkDpc extends BackEnd { val name = "SKDPC" }
  case object Local extends BackEnd { val name = "LOCAL" }
  case object Versioning extends BackEnd { val name = "VERSIONING" }
  case object Notification extends BackEnd { val name = "NOTIFICATION" }
  case object Gpfs extends BackEnd { val name = "GPFS" }
  case object S3 extends BackEnd { val name = "S3" }
  case object PayloadProxy extends BackEnd { val name = "PLPROXY" }
  case object Atlas extends BackEnd { val name = "ATLAS" }
}

object TraceIdentity {
  sealed trait Identity

  final case class CacheIdentity(cmdIdx: Int) extends Identity
}

object TraceEvent {
  import TraceBackEnd.BackEnd
  import TraceIdentity._
  import TraceLevel._
  import TraceRelevance._
  import Tracing._

  sealed trait Event extends EventCrumb.EventDEPRECATED {
    def level: Level
    def phase: Phase
    def relevance: Relevance
    def values = Map("event" -> name)
  }

  sealed trait BackEndAware { _: Event =>
    def backEnd: BackEnd
  }

  sealed trait HasHost {
    def host: Option[Host]
  }

  sealed trait ImpliesWallclockTiming

  /*
    implementing trace events indicate some sort of stalling. For eg - waiting in a queue for an executor or blocked for
    a lock.
   */
  sealed trait ImpliesStallTiming {
    self: ImpliesWallclockTiming =>
  }

  sealed trait ImpliesFirstTrace

  case object DsiRequest extends Event with ImpliesWallclockTiming with ImpliesFirstTrace {
    val name = "DSI_REQUEST"
    val level = Info
    val phase = Received
    val relevance = Ten
  }

  case object VersioningRequest extends Event with BackEndAware with ImpliesWallclockTiming {
    override val backEnd = TraceBackEnd.Versioning
    val name = "VERSIONING_REQUEST"
    val level = Info
    val phase = Execution
    val relevance = Ten
  }

  case object All extends Event with ImpliesWallclockTiming {
    val name = "ALL"
    val level = Info
    val phase = Execution
    val relevance = One
  }

  // This is an internal event marking a phase transition into the throttling layer queue
  // We don't emit this timing, as it is covered by Q and PreExecQ.
  // However we do track phases elsewhere so it's useful to make the start of the Queued phase.
  case object ScheduledForThrottling extends Event with ImpliesWallclockTiming {
    val name = "THROT_SCHED"
    val level = Debug
    val phase = Queued
    val relevance = One
  }

  // This is the time request spent in throttling Q for which it was throttled
  case object Q extends Event with ImpliesWallclockTiming {
    val name = "Q"
    val level = Info
    val phase = Queued
    val relevance = One
  }

  // This is the time request spent in throttling Q for which it was stalled
  case object PreExecQ extends Event with ImpliesWallclockTiming with ImpliesStallTiming {
    val name = "PRE_EXEC_Q"
    val level = Info
    val phase = Queued
    val relevance = One
  }

  case object ExecQ extends Event with ImpliesWallclockTiming with ImpliesStallTiming {
    val name = "EXEC_Q"
    val level = Info
    val phase = Queued
    val relevance = One
  }

  case object Ex extends Event with ImpliesWallclockTiming {
    val name = "EX"
    val level = Info
    val phase = Execution
    val relevance = One
  }

  case object CacheEx extends Event with ImpliesWallclockTiming {
    val name = "CACHE_EX"
    val level = Info
    val phase = Execution
    val relevance = Five
  }

  case object DsiQ extends Event with ImpliesWallclockTiming with ImpliesStallTiming {
    val name = "DSI_Q"
    val level = Info
    val phase = Queued
    val relevance = Two
  }

  case object DdcRequestLock extends Event with ImpliesWallclockTiming with ImpliesStallTiming {
    val name = "DDC_REQUEST_LOCK"
    val level = Info
    val phase = Blocked
    val relevance = Two
  }

  case object ParallelIndexWritesThreadPoolQ extends Event with ImpliesWallclockTiming with ImpliesStallTiming {
    val name = "PIW_Q"
    val level = Info
    val phase = Queued
    val relevance = One
  }

  case object ShardedWriterThreadPoolQ extends Event with ImpliesWallclockTiming with ImpliesStallTiming {
    val name = "SW_Q"
    val level = Info
    val phase = Queued
    val relevance = One
  }

  case object AsyncWriteOpsThreadPoolQ extends Event with ImpliesWallclockTiming with ImpliesStallTiming {
    val name = "AWO_Q"
    val level = Info
    val phase = Queued
    val relevance = One
  }

  case object DsiEx extends Event with ImpliesWallclockTiming {
    val name = "DSI_EX"
    val level = Info
    val phase = Execution
    val relevance = Five
  }

  case object DdcSnd extends Event with ImpliesWallclockTiming {
    val name = "DDC_SND"
    val level = Info
    val phase = Execution
    val relevance = Two
  }

  case object DdcSerialization extends Event with ImpliesWallclockTiming {
    val name = "DDC_SER"
    val level = Info
    val phase = Execution
    val relevance = Two
  }

  case object IdealTtLock extends Event with ImpliesWallclockTiming {
    val name = "IDEAL_TT_LOCK"
    val level: Level = Info
    val phase: Phase = Execution
    val relevance: Relevance = One
  }

  case object MongoPrep extends Event with ImpliesWallclockTiming {
    val name = "MONGO_PREP"
    val level: Level = Info
    val phase: Phase = Execution
    val relevance: Relevance = One
  }

  final case class ChannelQ(id: CacheIdentity) extends Event with ImpliesWallclockTiming with ImpliesStallTiming {
    val name = "CHANNEL_Q"
    val level = Info
    val phase = Queued
    val relevance = Two
    override def values: Map[String, String] = super.values ++ Map[String, String]("id" -> id.toString)
  }

  final case class ChannelThrottling(batch: Set[CacheIdentity]) extends Event with ImpliesWallclockTiming {
    val name = "CHANNEL_THROTTLING"
    val level = Info
    val phase = Queued
    val relevance = Two
    override def values: Map[String, String] = super.values ++ Map[String, String]("batch" -> batch.toString)
  }

  final case class ChannelEx(batch: Set[CacheIdentity]) extends Event with ImpliesWallclockTiming {
    val name = "CHANNEL_EX"
    val level = Info
    val phase = Execution
    val relevance = Two
    override def values: Map[String, String] = super.values ++ Map[String, String]("batch" -> batch.toString)
  }

  case object DdcResponseQ extends Event with ImpliesWallclockTiming with ImpliesStallTiming {
    val name = "DDC_RESPONSE_Q"
    val level = Info
    val phase = Queued
    val relevance = Two
  }

  case object DdcResponseBatchQ extends Event with ImpliesWallclockTiming with ImpliesStallTiming {
    val name = "DDC_RESPONSE_BATCHQ"
    val level = Info
    val phase = Queued
    val relevance = One
  }

  case object DdcResponseEx extends Event with ImpliesWallclockTiming {
    val name = "DDC_RESPONSE_EX"
    val level = Info
    val phase = Execution
    val relevance = Two
  }

  case object SessionEstablishment extends Event with ImpliesWallclockTiming {
    val name = "SES_EST"
    val level = Info
    val phase = Execution
    val relevance = Seven
  }

  case object SessionCacheMiss extends Event with ImpliesWallclockTiming {
    val name = "SES_CACHE_MISS"
    val level = Info
    val phase = Execution
    val relevance = Seven
  }

  case object DHTMiss extends Event with ImpliesWallclockTiming {
    val name = "DHT_MISS"
    val level = Info
    val phase = Execution
    val relevance = Seven
  }

  /**
   * This tracks the execution of a PRC request on SK proxy node
   */
  private[optimus] case object SKRequest extends Event with ImpliesWallclockTiming with ImpliesFirstTrace {
    val name = "SK_REQUEST"
    val level = Debug
    val phase = Execution
    val relevance = Seven
  }

  /**
   * This tracks the execution of a (proxy forwarded) PRC request.
   */
  private[optimus] case object SKForwardRequest extends Event with ImpliesWallclockTiming with ImpliesFirstTrace {
    val name = "SK_FORWARD_REQUEST"
    val level = Debug
    val phase = Execution
    val relevance = Seven
  }

  /**
   * This tracks fetching and adding a payload to a SerializedEntity that has a (purposefully) empty payload.
   */
  private[optimus] case object PrcEnrichWithPayload extends Event with ImpliesWallclockTiming {
    val name = "PRC_PAYLOAD"
    val level = Debug
    val phase = Execution
    val relevance = Seven
  }

  private[optimus] case object PrcPayloadFetch extends Event with ImpliesWallclockTiming {
    val name = "PRC_PAYLOAD_FETCH"
    val level = Debug
    val phase = Execution
    val relevance = Five
  }

  private[optimus] case object PrcPayloadPostProcessing extends Event with ImpliesWallclockTiming {
    val name = "PRC_PAYLOAD_POSTPROC"
    val level = Debug
    val phase = Execution
    val relevance = Five
  }

  /**
   * This tracks entitlement checking a PRC result with a result.
   */
  private[optimus] case object PrcEntitlementCheckCmdWithResult extends Event with ImpliesWallclockTiming {
    val name = "PRC_ENTITLEMENT_CHECK_CMD_WITH_RESULT"
    val level = Debug
    val phase = Execution
    val relevance = Seven
  }

  /**
   * This tracks entitlement pre-checking a PRC result.
   */
  private[optimus] case object PrcEntitlementPreCheck extends Event with ImpliesWallclockTiming {
    val name = "PRC_ENTITLEMENT_PRE_CHECK"
    val level = Debug
    val phase = Execution
    val relevance = Seven
  }

  /**
   * This tracks entitlement post-checking a PRC result.
   */
  private[optimus] case object PrcEntitlementPostCheck extends Event with ImpliesWallclockTiming {
    val name = "PRC_ENTITLEMENT_POST_CHECK"
    val level = Debug
    val phase = Execution
    val relevance = Seven
  }

  /**
   * This tracks time when PRC is waiting for the LSQT.
   */
  private[optimus] case object PrcLsqtWait extends Event with ImpliesWallclockTiming {
    val name = "PRC_LSQT_WAIT"
    val level = Debug
    val phase = Execution
    val relevance = Seven
  }

  /**
   * This tracks checking whether or not we have a bitemporal space in the in-memory buffer.
   */
  private[optimus] case object PrcLocalSpaceRetrieval extends Event with ImpliesWallclockTiming {
    val name = "PRC_LOCAL_SPACE_RETRIEVAL"
    val level = Debug
    val phase = Execution
    val relevance = Seven
  }

  /**
   * This tracks the time to lock the read lock.
   */
  private[optimus] case object PrcLockReadLock extends Event with ImpliesWallclockTiming {
    val name = "PRC_LOCK_READ_LOCK"
    val level = Debug
    val phase = Execution
    val relevance = Seven
  }

  /**
   * This tracks the time to lock the write lock.
   */
  private[optimus] case object PrcLockWriteLock extends Event with ImpliesWallclockTiming {
    val name = "PRC_LOCK_WRITE_LOCK"
    val level = Debug
    val phase = Execution
    val relevance = Seven
  }

  /**
   * This tracks bitemporal space writes to SK.
   */
  private[optimus] case object PrcPut extends Event with ImpliesWallclockTiming {
    val name = "PRC_PUT"
    val level = Debug
    val phase = Execution
    val relevance = Seven
  }

  /**
   * This tracks a read-through to the broker when we do not have a bitemporal space either in the in-memory buffer or
   * in SK.
   */
  private[optimus] case object PrcRemoteSpaceRetrieval extends Event with ImpliesWallclockTiming {
    val name = "PRC_REMOTE_SPACE_RETRIEVAL"
    val level = Debug
    val phase = Execution
    val relevance = Seven
  }

  /**
   * This tracks PRC retrieval index processing.
   */
  private[optimus] case object PrcRetrievalIndexProcessing extends Event with ImpliesWallclockTiming {
    val name = "PRC_RETRIEVAL_INDEX_PROCESSING"
    val level = Debug
    val phase = Execution
    val relevance = Seven
  }

  /**
   * This tracks a PRC session establishment.
   */
  private[optimus] case object PrcSessionEstablishment extends Event with ImpliesWallclockTiming {
    val name = "PRC_SESSION_EST"
    val level = Debug
    val phase = Execution
    val relevance = Seven
  }

  /**
   * This tracks SK LRU key marking.
   */
  private[optimus] case object PrcSKMarkKey extends Event with ImpliesWallclockTiming {
    val name = "PRC_SK_MARK_KEY"
    val level = Debug
    val phase = Execution
    val relevance = Seven
  }

  /**
   * This tracks a bitemporal space retrieval from SK.
   */
  private[optimus] case object PrcSKSpaceRetrieval extends Event with ImpliesWallclockTiming {
    val name = "PRC_SK_SPACE_RETRIEVAL"
    val level = Debug
    val phase = Execution
    val relevance = Seven
  }

  /**
   * This tracks deserialisation time for initial requests received at the trigger
   */
  private[optimus] case object PrcTriggerUserOptsDeserde extends Event with ImpliesWallclockTiming {
    val name = "PRC_TRIGGER_USEROPTS_DESERDE"
    val level = Debug
    val phase = Execution
    val relevance = Seven
  }

  /**
   * This tracks processing time for the results of a remote space read-through
   */
  private[optimus] case object PrcRemoteSpaceProcessing extends Event with ImpliesWallclockTiming {
    val name = "PRC_REMOTE_SPACE_RETRIEVAL_PROCESS"
    val level = Debug
    val phase = Execution
    val relevance = Seven
  }

  /**
   * This tracks serialisation time for the final result to be sent to the client
   */
  private[optimus] case object PrcSerdeResult extends Event with ImpliesWallclockTiming {
    val name = "PRC_RESULT_SERDE"
    val level = Debug
    val phase = Execution
    val relevance = Seven
  }

  /**
   * This tracks the global queue time between SK receives a request and starts to handle (then forward to local or
   * remote)
   */
  case object SkReceiveGlobalQ extends Event with ImpliesWallclockTiming with ImpliesStallTiming {
    val name = "SK_RECEIVE_GQ"
    val level = Debug
    val phase = Queued
    val relevance = Seven
  }

  /**
   * This tracks the queue time between SK receives a request and starts to handle (then start to do localOp)
   */
  case object SkReceiveQ extends Event with ImpliesWallclockTiming with ImpliesStallTiming {
    val name = "SK_RECEIVE_Q"
    val level = Debug
    val phase = Queued
    val relevance = Seven
  }

  /**
   * This tracks whole retrieval from within the SK StorageModule.
   */
  private[optimus] case object SkStorageModuleRetrieval extends Event with ImpliesWallclockTiming {
    val name = "SK_STORAGE_MODULE_RETRIEVAL"
    val level = Debug
    val phase = Execution
    val relevance = Seven
  }

  /**
   * This tracks whole retrieval from within the RetrieveTrigger.
   */
  private[optimus] case object SkTriggerRetrieval extends Event with ImpliesWallclockTiming {
    val name = "SK_TRIGGER_RETRIEVAL"
    val level = Debug
    val phase = Execution
    val relevance = Seven
  }

  /**
   * This tracks the queue time between SK receives a request and starts to handle
   */
  private[optimus] case object SkResultQ extends Event with ImpliesWallclockTiming with ImpliesStallTiming {
    val name = "SK_RESULT_Q"
    val level = Debug
    val phase = Queued
    val relevance = Seven
  }

  /**
   * This tracks sending out the result from a Sk replica node to proxy node
   */
  private[optimus] case object SkSendResult extends Event with ImpliesWallclockTiming {
    val name = "SK_SEND_RESULT"
    val level = Debug
    val phase = Execution
    val relevance = Seven
  }

  /**
   * This tracks the execution of a Deriv1 request
   */
  private[optimus] case object Deriv1Request extends Event with ImpliesWallclockTiming with ImpliesFirstTrace {
    val name = "DERIV1_REQUEST"
    val level = Info
    val phase = Execution
    val relevance = Seven
  }

  case object SessionSerialization extends Event with ImpliesWallclockTiming {
    val name = "SES_SER"
    val level = Info
    val phase = Execution
    val relevance = Seven
  }

  case object SessionHydrate extends Event with ImpliesWallclockTiming {
    val name = "SES_HYD"
    val level = Info
    val phase = Execution
    val relevance = Seven
  }

  case object SessionDalLoad extends Event with ImpliesWallclockTiming {
    val name = "SES_DAL_LOAD"
    val level = Info
    val phase = Execution
    val relevance = Seven
  }

  case object SessionDhtLoad extends Event with ImpliesWallclockTiming {
    val name = "SES_DHT_LOAD"
    val level = Info
    val phase = Execution
    val relevance = Seven
  }

  case object GpbSerialization extends Event {
    val name = "GPB_S"
    val level = Debug
    val phase = Execution
    val relevance = One
  }

  case object GpbSize extends Event {
    val name = "GPB_SIZE"
    val level = Info
    val phase = Execution
    val relevance = Ten
  }

  case object BackEndSerialization extends Event {
    val name = "BACKEND_SER"
    val level = Debug
    val phase = Execution
    val relevance = Two
  }

  case object Snd extends Event with ImpliesWallclockTiming {
    val name = "SND"
    val level = Info
    val phase = Execution
    val relevance = Ten
  }

  case object Read extends Event {
    val name = "READ"
    val level = Info
    val phase = Execution
    val relevance = Ten
  }

  case object GpfsRead extends Event {
    val name = "GPFS_READ"
    val level = Info
    val phase = Execution
    val relevance = One
  }

  case object S3Read extends Event {
    val name = "S3_READ"
    val level = Info
    val phase = Execution
    val relevance = One
  }

  case object Write extends Event {
    val name = "WRITE"
    val level = Info
    val phase = Execution
    val relevance = Ten
  }

  case object ReadDemuxer extends Event {
    val name = "READ_DEMUXER"
    val level = Info
    val phase = Execution
    val relevance = Six
  }

  case object WriteDemuxer extends Event {
    val name = "WRITE_DEMUXER"
    val level = Info
    val phase = Execution
    val relevance = Six
  }

  case object LsqtWait extends Event {
    val name = s"LSQT_WAIT"
    val level = Info
    val phase = Execution
    val relevance = Seven
  }

  case object WriteBlobs extends Event {
    val name = "WRITE_BLOBS"
    val level = Info
    val phase = Execution
    val relevance = Five
  }

  case object WriteBeBlobs extends Event {
    val name = "WRITE_BE_BLOBS"
    val level = Info
    val phase = Execution
    val relevance = Five
  }

  case object RemoveBlobs extends Event {
    val name = "REMOVE_BLOBS"
    val level = Info
    val phase = Execution
    val relevance = Five
  }

  case object RemoveBeBlobs extends Event {
    val name = "REMOVE_BE_BLOBS"
    val level = Info
    val phase = Execution
    val relevance = Five
  }

  case object WriteMakeTxn extends Event {
    val name = "WRITE_MAKE_TXN"
    val level = Info
    val phase = Execution
    val relevance = Two
  }

  case object WriteMetadata extends Event {
    val name = "WRITE_METADATA"
    val level = Info
    val phase = Execution
    val relevance = Two
  }

  case object BucketBlocked extends Event {
    val name = "BUCKET_BLOCKED"
    val level = Info
    val phase = Execution
    val relevance = Two
  }

  case object PayloadAggregate extends Event {
    val name = "PAYLOAD"
    val level = Info
    val phase = Execution
    val relevance = Four
  }

  final case class PayloadOp(opName: String) extends Event {
    val name = "PAYLOAD_OP:" + opName
    val level = Debug
    val phase = Execution
    val relevance = One
  }

  final case class CursorOpen(backEnd: BackEnd, host: Host) extends Event {
    val name = "CURSOR_OPEN"
    val level = Debug
    val phase = Execution
    val relevance = Ten
  }

  final case class ReadSessionCreation(hosts: Set[Host]) extends Event {
    val name = "READ_SESSION_CREATION"
    val level = Debug
    val phase = Execution
    val relevance = Ten
  }

  final case class ReadOp(opName: String, backEnd: BackEnd, host: Option[Host] = None)
      extends Event
      with BackEndAware
      with HasHost {
    val name = s"${backEnd.name}_READ_OP:${opName}"
    val level = Debug
    val phase = Execution
    val relevance = One
    override def values: Map[String, String] = super.values ++ Map[String, String]("host" -> host.toString)
  }

  final case class WriteOp(opName: String, backEnd: BackEnd, host: Option[Host] = None)
      extends Event
      with BackEndAware
      with HasHost {
    val name = s"${backEnd.name}_WRITE_OP:${opName}"
    val level = Debug
    val phase = Execution
    val relevance = One
    override def values: Map[String, String] = super.values ++ Map[String, String]("host" -> host.toString)
  }

  final case class PayloadRead(opName: String, backEnd: BackEnd, slowReadCount: Int = 0)
      extends Event
      with BackEndAware {
    val name = s"${backEnd.name}_PAYLOAD_READ:${opName}"
    val level = Debug
    val phase = Execution
    val relevance = One
    override def values: Map[String, String] =
      super.values ++ Map[String, String]("slowReadCount" -> slowReadCount.toString)
  }

  final case class PayloadWrite(opName: String, backEnd: BackEnd) extends Event with BackEndAware {
    val name = s"${backEnd.name}_PAYLOAD_WRITE:${opName}"
    val level = Debug
    val phase = Execution
    val relevance = One
  }

  final case class ReadBatcher(opName: String, batchSize: Int) extends Event {
    val name = s"READ_BATCHER_${opName}"
    val level = Debug
    def phase = Execution
    val relevance = Two
    override def values: Map[String, String] = super.values ++ Map[String, String]("batchSize" -> batchSize.toString)
  }

  final case class LockManagerConflictCheck(count: Int) extends Event {
    val name = "LOCKMANAGER_CONFLICT_CHECK"
    val level = Info
    val phase = Execution
    val relevance = Three
  }

  case object TxnManagerLogBlobs extends Event {
    val name = "TXNMANAGER_LOG_BLOBS"
    val level = Info
    val phase = Execution
    val relevance = Three
  }

  case object TxnManagerLogMetadata extends Event {
    val name = "TXNMANAGER_LOG_METADATA"
    val level = Info
    val phase = Execution
    val relevance = Three
  }

  case object TxnManagerLogWriteCompleted extends Event {
    val name = "TXNMANAGER_LOG_WRITECOMPLETED"
    val level = Info
    val phase = Execution
    val relevance = Three
  }

  case object TxnManagerFinalizeCommit extends Event {
    val name = "TXNMANAGER_FINALIZE_COMMIT"
    val level = Info
    val phase = Execution
    val relevance = Three
  }

  case object PublishWrite extends Event {
    val name = "PUBLISH_WRITE"
    val level = Info
    val phase = Execution
    val relevance = Three
  }

  final case class PublishWriteOn(publisher: String) extends Event {
    val name = "PUBLISH_WRITE_ON:" + publisher
    val level = Debug
    val phase = Execution
    val relevance = One
  }

  // Entitlements
  final case class EntitlementCheck(info: String, count: Int) extends Event {
    val name = s"ENTITLEMENT_CHECK:${info}"
    val level = Info
    val phase = Execution
    val relevance = Two
    override def values: Map[String, String] = super.values ++ Map[String, String]("count" -> count.toString)
  }

  sealed trait EntitlementCheckStage extends Event {
    val count: Int
    override val level = Info
    override val phase = Execution
    override val relevance = Two
  }
  final case class PreEntitlementCheck(override val count: Int = 0) extends EntitlementCheckStage {
    override val name: String = "ENTITLEMENT_PRE"
  }
  final case class PostEntitlementCheck(override val count: Int = 0) extends EntitlementCheckStage {
    override val name: String = "ENTITLEMENT_POST"
  }

  sealed trait EntitlementLoadEventBase extends Event {
    override val level = Info
    override val phase = Execution
    override val relevance = Two
  }
  case object EntitlementLoad extends EntitlementLoadEventBase {
    override val name = "ENTITLEMENT_LOAD"
  }
  case object EntitlementLoadLoadEntitlements extends EntitlementLoadEventBase {
    override val name = "ENTITLEMENT_LOAD_LOAD_ENTITLEMENTS"
  }
  case object EntitlementLoadLoadFeatureEntitlements extends EntitlementLoadEventBase {
    override val name = "ENTITLEMENT_LOAD_LOAD_FEATURE_ENTITLEMENTS"
  }
  case object EntitlementLoadLoadLinkages extends EntitlementLoadEventBase {
    override val name = "ENTITLEMENT_LOAD_LOAD_LINKAGES"
  }
  case object EntitlementLoadLoadDalEntitlements extends EntitlementLoadEventBase {
    override val name = "ENTITLEMENT_LOAD_LOAD_DAL_ENTITLEMENTS"
  }
  case object EntitlementLoadGetPayloads extends EntitlementLoadEventBase {
    override val name = "ENTITLEMENT_LOAD_GET_PAYLOADS"
  }
  case object EntitlementLoadPreloadPermissions extends EntitlementLoadEventBase {
    override val name = "ENTITLEMENT_LOAD_PRELOAD_PERMISSIONS"
  }
  case object EntitlementLoadEntitlementTransform extends EntitlementLoadEventBase {
    override val name = "ENTITLEMENT_LOAD_ENTITLEMENT_TRANSFORM"
  }
  case object EntitlementLoadCacheMiss extends EntitlementLoadEventBase {
    override val name = "ENTITLEMENT_LOAD_CACHE_MISS"
  }

  /* Events used to trace query execution on server side */
  case object ExpressionQuery extends Event {
    val name = "EXPRESSION_QUERY"
    val level = Info
    val phase = Execution
    val relevance = One
  }
  case object ExpressionQueryStream extends Event {
    val name = "EXPRESSION_QUERY_STREAM"
    val level = Info
    val phase = Execution
    val relevance = One
  }
  case object TemporalityQueryCommand extends Event {
    val name = "TEMPORALITY_QUERY_COMMAND"
    val level = Info
    val phase = Execution
    val relevance = One
  }
  case object TemporalityQueryCommandReference extends Event {
    val name = "TEMPORALITY_QUERY_COMMAND_REFERENCE"
    val level = Info
    val phase = Execution
    val relevance = One
  }
  case object TemporalityQueryCommandStream extends Event {
    val name = "TEMPORALITY_QUERY_COMMAND_STREAM"
    val level = Info
    val phase = Execution
    val relevance = One
  }
  case object TemporalityQueryCommandReferenceStream extends Event {
    val name = "TEMPORALITY_QUERY_COMMAND_REFERENCE_STREAM"
    val level = Info
    val phase = Execution
    val relevance = One
  }
  case object EntityClassQueryStream extends Event {
    val name = "ENTITY_CLASS_QUERY_STREAM"
    val level = Info
    val phase = Execution
    val relevance = One
  }
  case object EventClassQueryStream extends Event {
    val name = "EVENT_CLASS_QUERY_STREAM"
    val level = Info
    val phase = Execution
    val relevance = One
  }
  case object SelectSpaceQueryStream extends Event {
    val name = "SELECT_SPACE_QUERY_STREAM"
    val level = Info
    val phase = Execution
    val relevance = One
  }
  case object EntityGroupingClassQueryStream extends Event {
    val name = "ENTITY_GROUPING_CLASS_QUERY_STREAM"
    val level = Info
    val phase = Execution
    val relevance = One
  }
  case object EntityReferenceQueryStream extends Event {
    val name = "ENTITY_REFERENCE_QUERY_STREAM"
    val level = Info
    val phase = Execution
    val relevance = One
  }
  case object EntityKeyQueryStream extends Event {
    val name = "ENTITY_KEY_QUERY_STREAM"
    val level = Info
    val phase = Execution
    val relevance = One
  }
  case object EntityIndexQueryStream extends Event {
    val name = "ENTITY_INDEX_QUERY_STREAM"
    val level = Info
    val phase = Execution
    val relevance = One
  }
  case object EventIndexQueryStream extends Event {
    val name = "EVENT_INDEX_QUERY_STREAM"
    val level = Info
    val phase = Execution
    val relevance = One
  }
  case object LinkageEntityQueryStream extends Event {
    val name = "LINKAGE_ENTITY_QUERY_STREAM"
    val level = Info
    val phase = Execution
    val relevance = One
  }
  case object EnumerateKeysQueryStream extends Event {
    val name = "ENUMERATE_KEYS_QUERY_STREAM"
    val level = Info
    val phase = Execution
    val relevance = One
  }
  case object EnumerateIndicesQueryStream extends Event {
    val name = "ENUMERATE_INDICES_QUERY_STREAM"
    val level = Info
    val phase = Execution
    val relevance = One
  }
  case object AuditInfoQueryStream extends Event {
    val name = "AUDIT_INFO_QUERY_STREAM"
    val level = Info
    val phase = Execution
    val relevance = One
  }

  case object TxnRetry extends Event {
    val name = "TXN_RETRY"
    val level = Info
    val phase = Execution
    val relevance = Eight
  }

  case object TxnManagerRecovery extends Event with ImpliesWallclockTiming {
    // Not included in phase timing lines, since this is only non-zero
    // in exceptional circumstances, when it will appear on breadcrumbs.
    val name = "TXNMANAGER_RECOVERY"
    val level = Info
    val phase = Execution
    val relevance = Ten
  }

  // Event to record status on Cache
  final case class CacheRead(opName: String, hitCount: Long, missCount: Long, computeHitCount: Long, backEnd: BackEnd)
      extends Event {
    val name = s"${backEnd.name}_CACHE_READ:${opName}"
    val level = Debug
    val phase = Execution
    val relevance = One
    override def values: Map[String, String] =
      super.values ++ Map[String, String](
        "hit" -> hitCount.toString,
        "miss" -> missCount.toString,
        "computeHit" -> computeHitCount.toString)
  }

  object CacheRead {
    sealed trait Status
    case object Hit extends Status
    case object Miss extends Status
    case object ComputeHit extends Status

    def apply(name: String, status: Status, backEnd: BackEnd): CacheRead = {
      status match {
        case Hit        => CacheRead(name, 1L, 0L, 0L, backEnd)
        case Miss       => CacheRead(name, 0L, 1L, 0L, backEnd)
        case ComputeHit => CacheRead(name, 0L, 0L, 1L, backEnd)
      }
    }

    def apply(name: String, hitCount: Long, missCount: Long, backEnd: BackEnd): CacheRead = {
      CacheRead(name, hitCount, missCount, 0L, backEnd)
    }
  }

  // below are DAL notification related events
  // We set `relevance` to `One`, because we do not need the associated breadcrumbs for most cases
  // If we do in the future, we may want to bump this value to `Nine` or `Ten` so that we publish crumbs for all requests
  sealed trait TimePointEvent
  sealed trait TimeDurationEvent
  case object ReplicationEntryEnqueue extends Event with BackEndAware with TimePointEvent {
    override val backEnd = TraceBackEnd.Notification
    val name = "RE_ENQUEUE"
    val level = Info
    val phase = Execution
    override val relevance = One
  }

  case object ReplicationEntryDequeue extends Event with BackEndAware with TimePointEvent {
    override val backEnd = TraceBackEnd.Notification
    val name = "RE_DEQUEUE"
    val level = Info
    val phase = Execution
    override val relevance = One
  }

  case object NotificationTransform extends Event with BackEndAware with TimeDurationEvent {
    override val backEnd = TraceBackEnd.Notification
    val name = "NE_TRANSFORM"
    val level = Info
    val phase = Execution
    override val relevance = One
  }

  case object NotificationEntryEnqueue extends Event with BackEndAware with TimePointEvent {
    override val backEnd = TraceBackEnd.Notification
    val name = "NE_ENQUEUE"
    val level = Info
    val phase = Execution
    override val relevance = One
  }

  case object NotificationEntryDequeue extends Event with BackEndAware with TimePointEvent {
    override val backEnd = TraceBackEnd.Notification
    val name = "NE_DEQUEUE"
    val level = Info
    val phase = Execution
    override val relevance = One
  }

  case object NotificationEntryPublish extends Event with BackEndAware with TimeDurationEvent {
    override val backEnd = TraceBackEnd.Notification
    val name = "NE_ENQUEUE"
    val level = Info
    val phase = Execution
    override val relevance = One
  }

  final case class NotificationReset(reason: String) extends Event with BackEndAware with TimePointEvent {
    override val backEnd = TraceBackEnd.Notification
    val name = "NE_RESET"
    val level = Info
    val phase = Execution
    override val relevance = One
  }

  // Below are DAL PubSub related events..

  case object PubSubTransform extends Event with ImpliesWallclockTiming {
    val name = "PUBSUB_TRANSFORM"
    val level = Info
    val phase = Execution
    val relevance = One
  }

  case object PubSubMatcher extends Event with ImpliesWallclockTiming {
    val name = "PUBSUB_MATCHER"
    val level = Info
    val phase = Execution
    val relevance = One
  }

  case object SessionEstablishmentQueue extends Event with ImpliesWallclockTiming with ImpliesStallTiming {
    val name = "SESSION_EST_Q"
    val level = Info
    val phase = Queued
    val relevance = Two
  }

  case object PreparingToHandle extends Event with ImpliesWallclockTiming {
    val name = "PREPARE"
    val level = Info
    val phase = Execution
    val relevance = One
  }

  // This is essentially a DbUri, but it is in dsi_base..
  private type DbUriType = (URI, Option[String], Option[String], Option[String])
  final case class ShardWriteEvent(
      uri: DbUriType,
      backEnd: BackEnd,
      host: Option[Host],
      sourceName: String,
      actionToCounts: Map[String, Int],
      sourceRule: SourceShardRule = UnknownSourceShardRule)
      extends Event
      with BackEndAware
      with HasHost {
    val name = s"SHARD_WRITE:${sourceName}"
    val level = Debug
    val phase = Execution
    val relevance = One
    override def values: Map[String, String] =
      super.values ++ Map[String, String]("shard" -> s"$uri?rule=${uri._2}") ++ actionToCounts.map { case (k, v) =>
        k -> v.toString
      }
    def totalOpCont: Int = actionToCounts.values.sum
  }
  // IndexWriteEvent and IndexReadEvent are used for performance comparison when read and/or write modes are set to EntityIndexMode.Dual
  final case class IndexWriteEvent(cNameToCounts: Map[String, Int], isRegistered: Boolean) extends Event {
    val name = s"IDX_WRITE:${cNameToCounts.keys}"
    val level = Debug
    val phase = Execution
    val relevance = One
    def totalOpCount(className: String): Int = cNameToCounts(className)
    lazy val typeNames = cNameToCounts.keys.toSeq
  }

  final case class IndexReadEvent(typeName: SerializedEntity.TypeRef, opCount: Int, isRegistered: Boolean)
      extends Event {
    val name = s"IDX_READ:${typeName}"
    val level = Debug
    val phase = Execution
    val relevance = One
  }
  final case class ShardReadEvent(
      uri: DbUriType,
      rule: String, // Shard rule for which read happened.
      backEnd: BackEnd,
      host: Option[Host],
      sourceName: String,
      opCount: Int,
      sourceRule: SourceShardRule = UnknownSourceShardRule)
      extends Event
      with BackEndAware
      with HasHost {
    val name = s"SHARD_READ:${sourceName}"
    val level = Debug
    val phase = Execution
    val relevance = One
    override def values: Map[String, String] =
      super.values ++ Map[String, String]("shard" -> s"$uri?rule=${rule}", "count" -> opCount.toString)
  }
  sealed trait SourceShardRule
  case object UnknownSourceShardRule extends SourceShardRule
  final case class RotationalSourceShardRule(
      ruleIndex: Int,
      uriSchemaIndex: Int
  ) extends SourceShardRule
}
