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
import msjava.slf4jutils.scalalog.getLogger
import optimus.dsi.base.getTTTooFarAheadMsgPrefix
import optimus.dsi.partitioning.DefaultPartition
import optimus.dsi.partitioning.Partition
import optimus.dsi.partitioning.PartitionMap
import optimus.graph.DiagnosticSettings
import optimus.platform._
import optimus.platform.dsi.Feature.PartitionedServerTime
import optimus.platform.dsi.bitemporal
import optimus.platform.dsi.bitemporal._
import optimus.platform.dsi.bitemporal.proto.ProtoFileProperties

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

object HasLastWitnessedTime {
  private final case class CatchupId(uuid: MSUuid) {
    override def toString: String = s"CatchupId($uuid)"
  }

  private[HasLastWitnessedTime] val log = getLogger[HasLastWitnessedTime]
  private val logOnTtDifferenceThresholdMs =
    // 5 minutes by default
    java.lang.Long.getLong("optimus.platform.dal.lastWitnessedTtDifferenceLogThresholdMs", 5 * 60 * 1000)
  private val MaxAtNowWaitingTime = Integer.getInteger("optimus.dsi.maxAtNowWaitingTime", 60000)
  private val enableAtNowMaxCatchupFix =
    DiagnosticSettings.getBoolProperty("optimus.dsi.enableAtNowMaxCatchupFix", true)
}

private[optimus] trait HasLastWitnessedTime {
  import HasLastWitnessedTime._

  protected def allPartitions: Set[Partition]

  protected lazy val lastWitnessedTimeMap: ConcurrentHashMap[Partition, Instant] = {
    val lastWitnessedTimeMap = new ConcurrentHashMap[Partition, Instant]
    allPartitions.map(i => lastWitnessedTimeMap.put(i, TimeInterval.NegInfinity))
    lastWitnessedTimeMap
  }

  protected lazy val lastWrittenPartitions: ConcurrentHashMap.KeySetView[Partition, java.lang.Boolean] =
    ConcurrentHashMap.newKeySet[Partition]()

  protected lazy val lastReadWitnessedTimeMap: ConcurrentHashMap[Partition, Instant] = {
    val lastWitnessedTimeMap = new ConcurrentHashMap[Partition, Instant]
    allPartitions.map(i => lastWitnessedTimeMap.put(i, TimeInterval.NegInfinity))
    lastWitnessedTimeMap
  }

  protected def initLastWitnessedTimeMap(): Unit = {
    allPartitions.foreach { i =>
      lastWitnessedTimeMap.put(i, TimeInterval.NegInfinity)
      lastReadWitnessedTimeMap.put(i, TimeInterval.NegInfinity)
    }
  }

  private[optimus] def lastWitnessedTime(p: Partition): Instant = lastWitnessedTimeMap.get(p)

  private[optimus] def getLastWrittenPartitions: Set[Partition] = lastWrittenPartitions.asScala.toSet

  private[optimus] def lastReadWitnessedTime(p: Partition): Instant = lastReadWitnessedTimeMap.get(p)

  protected def logOnTtDifferenceThresholdMs: Long = HasLastWitnessedTime.logOnTtDifferenceThresholdMs
  protected def onTryToMoveEarlierLastWitnessedTime(): Unit = {}

  @async final private /*[platform]*/ def catchUp(
      fetchTime: AsyncFunction0[
        Map[Partition, Instant]
      ], // NodeFunction0[...] changed to AsyncFunction0[...] because they behave the same except NodeFunction0[...] expects RT, and current usages of the fetchTime arg are not RT
      source: String,
      queryPartitions: Set[Partition],
      waitForMaxCatchup: Boolean,
      waitingTime: Int,
      witnessedTimeMap: ConcurrentHashMap[Partition, Instant]): Map[Partition, Instant] = {
    val lsqtCatchupId = CatchupId(new MSUuid(""))
    var retried = 0
    val frozenLastWitnessedTime: Map[Partition, Instant] = {
      // This is done to keep the change backward compatible
      // if the code is like
      // persist { write entity }; given(transactionTimeNow) { read back the persisted entity }
      // If the entity was written at tt2 and if default partition is lagging behind tt2, then we must
      // wait till tt2 before reading that entity back. With this, we wouldn't be making a backward incompatible change.
      if (waitForMaxCatchup) {
        require(
          queryPartitions == Set(DefaultPartition),
          s"Wait for max catchup is only supported for DefaultPartition. " +
            s"Got $queryPartitions, lsqtCatchupId = $lsqtCatchupId"
        )
        val frozenMap =
          if (enableAtNowMaxCatchupFix)
            witnessedTimeMap.asScala.filter { case (p, _) => lastWrittenPartitions.contains(p) }.toMap
          else witnessedTimeMap.asScala.toMap
        val maxTxTime = if (frozenMap.isEmpty) TimeInterval.NegInfinity else frozenMap.values.max
        log.debug(
          s"Waiting for max txTime for default partition to catchup till $maxTxTime, " +
            s"lastWriteWitnessedTimes = $frozenMap, lsqtCatchupId = $lsqtCatchupId")
        Map(DefaultPartition -> maxTxTime)
      } else
        witnessedTimeMap.asScala.toMap.filter { case (k, _) =>
          queryPartitions.contains(k)
        }
    }

    var current = fetchTime()
    val reqStartTime = System.currentTimeMillis()
    log.info(
      s"Catch up id for the current call to fetch server time is $lsqtCatchupId with reqStartTime = $reqStartTime")
    var retriedTime = reqStartTime

    def upToDate: Boolean = frozenLastWitnessedTime.keySet.forall { p =>
      val upToDate = !frozenLastWitnessedTime(p).isAfter(current(p))
      if (!upToDate)
        log.info(
          s"Lsqt fetch for partition ${p} = ${current(p)} is still lagging behind the " +
            s"last witnessed time ${frozenLastWitnessedTime(p)} for lsqtCatchupId = $lsqtCatchupId " +
            s"even after $retried retries.")
      upToDate
    }

    while (!upToDate) {
      // wait for a finite time to avoid infinite loop
      if ((retriedTime - reqStartTime) > waitingTime)
        throw new GeneralDALException(
          s"$source: Cannot complete get server time request as read replica time $current is still lagging behind " +
            s"last witnessed time $witnessedTimeMap and frozenTxTimeMap = $frozenLastWitnessedTime " +
            s"even after waiting for ${waitingTime / 1000} seconds for query partitions $queryPartitions. " +
            s"Retried time = $retriedTime, Request start time = $reqStartTime, " +
            s"NumRetries = $retried and lsqtCatchupId = $lsqtCatchupId")
      // TODO (OPTIMUS-16909): use async delay, adaptive backoff.
      DALImpl.delay(100)
      retriedTime = System.currentTimeMillis()
      current = fetchTime()
      retried += 1
    }

    if (retried > 0)
      log.info(s"$source: read replica time caught up after $retried retries for lsqtCatchupId = $lsqtCatchupId")
    val queriedTimes = current.filter { case (k, _) => queryPartitions.contains(k) }
    updateWitnessedTime(source, queriedTimes, witnessedTimeMap)
    queriedTimes
  }

  @async final private[optimus] /*[platform]*/ def catchUpWitnessedTime(
      fetchTime: AsyncFunction0[Map[Partition, Instant]],
      source: String,
      queryPartitions: Set[Partition],
      waitForMaxCatchup: Boolean,
      waitingTime: Int = MaxAtNowWaitingTime): Map[Partition, Instant] = {
    val result = catchUp(fetchTime, source, queryPartitions, waitForMaxCatchup, waitingTime, lastWitnessedTimeMap)
    /*
        To make the read/write witness times consistent:
        This is to make sure client never see stale server time irrespective of order in which At.now or At.lsqt being called
     */
    updateWitnessedTime(s"$source(sync)", result, lastReadWitnessedTimeMap)
    result
  }

  @async final private[optimus] /*[platform]*/ def catchUpReadWitnessedTime(
      fetchTime: AsyncFunction0[Map[Partition, Instant]],
      source: String,
      queryPartitions: Set[Partition],
      waitForMaxCatchup: Boolean,
      waitingTime: Int = MaxAtNowWaitingTime): Map[Partition, Instant] = {
    val result = catchUp(fetchTime, source, queryPartitions, waitForMaxCatchup, waitingTime, lastReadWitnessedTimeMap)
    /*
           To make the read/write witness times consistent:
           This is to make sure client never see stale server time irrespective of order in which At.now or At.lsqt being called
     */
    updateWitnessedTime(s"$source(sync)", result, lastWitnessedTimeMap)
    result
  }

  @async final def atNow(queryPartitions: Set[Partition], waitForMaxCatchUp: Boolean): Map[Partition, Instant] = {
    catchUpWitnessedTime(asAsync(() => serverTime), "atNow", queryPartitions, waitForMaxCatchUp)
  }

  @async final def atLsqt(queryPartitions: Set[Partition], waitForMaxCatchUp: Boolean): Map[Partition, Instant] = {
    catchUpReadWitnessedTime(asAsync(() => serverTime), "atLsqt", queryPartitions, waitForMaxCatchUp)
  }

  private def updateWitnessedTime(
      source: String,
      newTime: Map[Partition, Instant],
      witnessTimeMap: ConcurrentHashMap[Partition, Instant]): Unit = {
    val orgLastWitnessedMap: Map[Partition, Instant] = witnessTimeMap.asScala.toMap

    @tailrec def updateIfLater(p: Partition, updatedTime: Instant): Unit = {
      val current = witnessTimeMap.get(p)
      notifyCurrentWitnessTime(source)
      if (updatedTime.isBefore(current)) {
        if (
          updatedTime.isAfter(TimeInterval.NegInfinity) &&
          updatedTime.plusMillis(logOnTtDifferenceThresholdMs).isBefore(current)
        ) {
          // Empty txn responses have tt = -inf, we want to exclude those. Also, we don't want to warn unless TTs are
          // drifting quite dramatically from last witnessed time -- they might be arbitrarily out-of-order in write
          // responses anyway.
          onTryToMoveEarlierLastWitnessedTime()
          log.warn(
            s"$this $source - partition ${p} last: ${current} - attempted change to an earlier ${updatedTime} rejected " +
              s"(original lastWitnessedTime: ${orgLastWitnessedMap(p)}")
        }
      } else if (current.equals(updatedTime))
        log.debug(
          s"$this $source - partition ${p} last: $current - no change (" +
            s"original lastWitnessedTime: $updatedTime) and original time ${orgLastWitnessedMap(p)})")
      else if (!witnessTimeMap.replace(p, current, updatedTime))
        updateIfLater(p, updatedTime)
      else
        log.debug(s"$this $source - partition ${p} last: $current - advanced to $updatedTime (current: ${witnessTimeMap
            .get(p)}) (original lastWitnessedTime: ${orgLastWitnessedMap(p)})")
    }

    newTime foreach { case (p, t) => updateIfLater(p, t) }
  }

  private[optimus /*dal*/ ] def updateLastWitnessedTime(newTime: Map[Partition, Instant]): Unit = {
    updateWitnessedTime("newWrite", newTime, lastWitnessedTimeMap)
    lastWrittenPartitions.addAll(newTime.keySet.asJava)
  }

  @async protected[optimus] /*[platform]*/ def serverTime: Map[Partition, Instant]

  // test hack
  protected[optimus] def notifyCurrentWitnessTime(source: String): Unit = {}
}

object DSIResolver {
  private[DSIResolver] val log = getLogger[DSIResolver]
}

// TODO (OPTIMUS-13031): Needs changing to an @transient @entity, but @async not supported on entities
trait DSIResolver extends EntityResolver with HasLastWitnessedTime with HasPartitionMap {
  import DSIResolver._

  @async final protected[optimus] /*[platform]*/ override def serverTime: Map[Partition, Instant] = {
    val results = dsi match {
      // ClientSideDSI's method is @node but DSI's isn't, downcasting to retain async stack
      case clientDSI: ClientSideDSI => clientDSI.executeReadOnlyCommands(GetInfo() :: Nil)
      // TODO (OPTIMUS-39528): remove the suppressSyncStackDetection after fixing sync stack
      case serverDSI => AdvancedUtils.suppressSyncStackDetection(serverDSI.executeReadOnlyCommands(GetInfo() :: Nil))
    }
    results match {
      case Seq(GetInfoResult(t, fi, _, partLsqtMap)) =>
        if (fi.isDefined && !serverInfo.isDefined) {
          serverInfo_ = fi
          log.info(s"Connected to DAL server $fi")
          val diff = serverInfo.daysDiff(ProtoFileProperties.featureInfo)
          diff foreach { d =>
            if (d > 30)
              log.warn(s"Client version is $d days older than server. Consider upgrading")
          }
        }

        if (dsi.serverFeatures().supports(PartitionedServerTime) && allPartitions.diff(partLsqtMap.keySet).isEmpty) {
          if (allPartitions.size == partLsqtMap.size) partLsqtMap
          else allPartitions.iterator.map(i => i -> partLsqtMap(i)).toMap
        } else {
          log.warn(
            "The server side doesn't support newer API's for At.now " +
              s"or returned results don't contain all partitions ${allPartitions}. " +
              s"Using default lsqt ${t} returned in ${results} to initialize missing partitions")
          allPartitions.iterator.map(i => i -> partLsqtMap.getOrElse(i, t)).toMap
        }
      case Seq(ErrorResult(e, _)) => throw new DSISpecificError(e.getMessage)
      case Seq(other, _*)         => throw new GeneralDALException("Unexpected result type " + other)
    }
  }

  private[this] var serverInfo_ : ServerInfo = ProtoFileProperties.NO_FEATURE_INFO
  protected[optimus] /*[platform]*/ def serverInfo: ServerInfo = serverInfo_

  protected def wrapQueryDSIException[T](work: => T): T = {
    try {
      work
    } catch {
      case e: bitemporal.DSISpecificError if (e.getMessage.contains(getTTTooFarAheadMsgPrefix)) =>
        throw new TTTooFarAheadException(e)
      case e: bitemporal.DSISpecificError => throw new UnboundDSIException(e)
      case e: bitemporal.DSIException     => throw new UnboundDSIException(e)
    }
  }

  protected[dal] val dsi: DSI
  final protected[optimus] /*[platform]*/ val partitionMap: PartitionMap = dsi.partitionMap
  final protected[optimus] /*[platform]*/ val allPartitions: Set[Partition] = partitionMap.allPartitions
  @async protected[optimus] def atNow(
      queryPartitions: Set[Partition],
      waitForMaxCatchUp: Boolean): Map[Partition, Instant]
  @node @scenarioIndependent protected[optimus] def count(query: Query, temporality: DSIQueryTemporality): Long
}
