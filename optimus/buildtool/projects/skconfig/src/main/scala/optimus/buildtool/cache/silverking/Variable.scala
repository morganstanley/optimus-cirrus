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

import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

import msjava.slf4jutils.scalalog.Logger

import scala.util.control.NonFatal
import optimus.buildtool.cache.silverking.OperationType._
import org.apache.curator.framework.CuratorFramework
import msjava.slf4jutils.scalalog.getLogger
import optimus.buildtool.cache.silverking.SilverKingConfig.ZookeeperConfig
import optimus.dal.silverking.SilverKingPlantConfig
import optimus.platform.runtime.ZkUtils
import optimus.utils.zookeeper.ReadOnlyDistributedValue

private[silverking] trait Variable[A] {
  def state: A
}

private[silverking] final case class Constant[A](override val state: A) extends Variable[A]

sealed trait FailureState
object FailureState {
  case object NotFailed extends FailureState
  case object Triggered extends FailureState
  case object Failed extends FailureState
}

private[silverking] class MaxFailuresSwitch(
    maxFailures: Int = SilverKingConfig.MaxFailures,
    onTrigger: Option[() => Unit]
) extends Variable[Boolean] {
  @volatile private var _retryTime: Option[Instant] = None
  private val failedOperations = new AtomicInteger(0)

  override def state: Boolean = {
    if (shouldReset) {
      _retryTime = None
      failedOperations.set(0)
    }
    failedOperations.get < maxFailures
  }

  protected def shouldReset: Boolean = retryTime.exists { t =>
    t.isBefore(patch.MilliInstant.now)
  }

  def reportFailure(): FailureState = {
    val failedOps = failedOperations.incrementAndGet()
    if (failedOps < maxFailures) FailureState.NotFailed
    else if (failedOps == maxFailures) {
      // Note that this is deliberately "==" rather than ">=" so that we
      // only set retryTime when it's newly triggered. The return value also
      // captures whether the switch has just been triggered
      _retryTime = Some(patch.MilliInstant.now.plus(SilverKingConfig.RetryTime))
      onTrigger.foreach(_())
      FailureState.Triggered
    } else FailureState.Failed
  }

  def retryTime: Option[Instant] = _retryTime
}

private[silverking] class DistributedSwitch(
    onTrigger: Option[() => Unit]
) extends Variable[Option[(OperationType, String)]] {

  @volatile protected var current: Option[(OperationType, String)] = initialValue

  protected def initialValue: Option[(OperationType, String)] = None

  // None == enabled
  // Some((opType, msg)) == disabled for `opType`, with informational message `msg`
  override def state: Option[(OperationType, String)] = current

  protected def updateState(v: Option[(OperationType, String)]): Unit = {
    if (current != v) {
      logUpdate(v)
      v match {
        case Some((OperationType.ReadAndWrite, _)) => onTrigger.foreach(_())
        case _                                     => // do nothing
      }
      current = v
    }
  }

  protected def logUpdate(v: Option[(OperationType, String)]): Unit = ()
}

private[buildtool] class ZookeeperSwitch(
    curator: CuratorFramework,
    path: String,
    clusterType: ClusterType,
    onTrigger: Option[() => Unit]
) extends DistributedSwitch(onTrigger) {
  import ZookeeperSwitch.log

  private def clusterStr = SilverKingStoreConfig.clusterStr(clusterType)

  override protected def initialValue: Option[(OperationType, String)] = {
    // register listener
    val zkValue = new ReadOnlyDistributedValue[String](
      curator,
      path,
      bytes => new String(bytes, StandardCharsets.UTF_8)
    ) {
      override def onNodeChange(v: Option[String]): Unit = update(v)
    }
    parse(zkValue.value)
  }

  protected def update(v: Option[String]): Unit = {
    val newVal = parse(v)
    updateState(newVal)
  }

  protected def parse(update: Option[String]): Option[(OperationType, String)] = update.flatMap { s =>
    val lines = s.linesIterator.map(_.trim).toSeq
    val disabled = lines.contains("enabled=false")
    val readDisabled = lines.contains("readEnabled=false")
    val writeDisabled = lines.contains("writeEnabled=false")
    val disabledOpType =
      if (disabled || (readDisabled && writeDisabled)) Some(OperationType.ReadAndWrite)
      else if (readDisabled) Some(OperationType.Read)
      else if (writeDisabled) Some(OperationType.Write)
      else None

    disabledOpType.map { t =>
      val typeStr = t match {
        case ReadAndWrite => "reads and writes"
        case Read         => "reads"
        case Write        => "writes"
      }
      val defaultMsg = s"$clusterStr $typeStr ${ZookeeperSwitch.silverKingDisabledMsg}"
      (
        t,
        lines
          .collectFirst { case ZookeeperSwitch.MessageRe(message) => s"$defaultMsg ($message)" }
          .getOrElse(defaultMsg)
      )
    }
  }

  override protected def logUpdate(v: Option[(OperationType, String)]): Unit = {
    log.debug(s"Changing setting of SK ZookeeperSwitch from $current to $v")
    log.info(v match {
      case Some((_, msg)) => msg
      case None           => s"$clusterStr re-enabled"
    })
  }
}

private[buildtool] object ZookeeperSwitch {
  private val log: Logger = getLogger(this.getClass)
  private val MessageRe = "message=(.*)".r

  val silverKingDisabledMsg = "disabled for all users by OBT support team"

  def apply(
      config: SilverKingConfig,
      clusterType: ClusterType,
      onTrigger: Option[() => Unit]
  ): Variable[Option[(OperationType, String)]] = {
    (SilverKingStoreConfig.enabled, config) match {
      case (Some(true), _) =>
        log.info(s"${SilverKingStoreConfig.clusterStr(clusterType)} force-enabled by system property")
        Constant(None)
      case (Some(false), _) =>
        Constant(Some((ReadAndWrite, s"${SilverKingStoreConfig.clusterStr(clusterType)} disabled by system property")))
      case (None, cfg: ZookeeperConfig) =>
        try {
          val curator = ZkUtils.getRootContext(cfg.env).getCurator
          val rootNodePath = SilverKingPlantConfig.getSkRootZNode(cfg.clusterName).path
          new ZookeeperSwitch(curator, rootNodePath, clusterType, onTrigger)
        } catch {
          case NonFatal(e) =>
            log.warn("Failed to create ZookeeperSwitch", e)
            Constant(None)
        }
      case _ =>
        Constant(None)
    }
  }
}
