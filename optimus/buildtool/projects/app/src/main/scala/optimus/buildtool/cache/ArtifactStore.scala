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
package optimus.buildtool.cache

import optimus.buildtool.artifacts.CachedArtifactType
import optimus.buildtool.config.ScopeId
import optimus.buildtool.config.NamingConventions.ConfigPrefix
import optimus.buildtool.trace.ArtifactCacheTraceType
import optimus.buildtool.trace.CacheTraceType
import optimus.buildtool.trace.ObtStats
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.trace.RemoteAssetCacheTraceType
import optimus.buildtool.utils.BlockingQueue
import optimus.platform._

import scala.concurrent.duration._
import java.util.Timer
import java.util.TimerTask

trait ArtifactReader {
  @async def get[A <: CachedArtifactType](
      id: ScopeId,
      fingerprintHash: String,
      tpe: A,
      discriminator: Option[String]
  ): Option[A#A]

  @async def check[A <: CachedArtifactType](
      id: ScopeId,
      fingerprintHashes: Set[String],
      tpe: A,
      discriminator: Option[String]
  ): Set[String]
}

object ArtifactWriter {
  private val log = msjava.slf4jutils.scalalog.getLogger(this)
}

trait ArtifactWriter {
  import ArtifactWriter._

  @async final def put[A <: CachedArtifactType](
      tpe: A
  )(id: ScopeId, fingerprintHash: String, discriminator: Option[String], artifact: A#A): A#A =
    if (artifact.shouldBeStored) {
      write[A](tpe)(id, fingerprintHash, discriminator, artifact)
    } else {
      log.debug(s"Skipping storage of ${artifact.pathString} (contains errors? ${artifact.hasErrors})")
      artifact
    }

  @async protected def write[A <: CachedArtifactType](
      tpe: A
  )(id: ScopeId, fingerprintHash: String, discriminator: Option[String], artifact: A#A): A#A

  def flush(timeoutMillis: Long): Unit
}

trait ArtifactStore extends ArtifactReader with ArtifactWriter {
  @async def close(): Unit
}

trait SearchableArtifactStore extends ArtifactStore {
  @async def getAll[A <: CachedArtifactType](id: ScopeId, tpe: A, discriminator: Option[String]): Seq[A#A]
}

object ArtifactStoreBase {
  private val log = msjava.slf4jutils.scalalog.getLogger(this)

  object Config {
    val dedupIntervalMillis: Long =
      sys.props.get(s"$ConfigPrefix.messageDedup.intervalMillis").map(_.toLong).getOrElse(30.seconds.toMillis)
  }
}

trait ArtifactStoreBase extends ArtifactStore {
  import ArtifactStoreBase._

  protected def cacheType: String
  protected def stat: ObtStats.Cache

  override def flush(timeoutMillis: Long): Unit = () // default impl does nothing
  @async override def close(): Unit = () // default impl does nothing

  final def logFound(id: ScopeId, tpe: CachedArtifactType, detail: String): Unit =
    logFound(id, ArtifactCacheTraceType(tpe), detail)
  final def logFound(id: ScopeId, tpe: CacheTraceType, detail: String): Unit = logFound(id, tpe, Seq(detail))

  final def logFound(id: ScopeId, tpe: CachedArtifactType, details: Seq[String]): Unit =
    logFound(id, ArtifactCacheTraceType(tpe), details)
  final def logFound(id: ScopeId, tpe: CacheTraceType, details: Seq[String]): Unit = {
    if (tpe.isInstanceOf[RemoteAssetCacheTraceType]) ObtTrace.addToStat(stat.ExternalHit, details.size)
    ObtTrace.addToStat(stat.Hit, details.size)
    if (details.size == 1) debug(id, s"Artifact found for $tpe: ${details.mkString("")}")
    else debug(id, s"Artifact(s) found for $tpe:\n  ${details.mkString("\n  ")}")
  }

  final def logNotFound(id: ScopeId, tpe: CachedArtifactType, detail: String): Unit =
    logNotFound(id, ArtifactCacheTraceType(tpe), detail)
  final def logNotFound(id: ScopeId, tpe: CacheTraceType, detail: String): Unit = {
    if (tpe.isInstanceOf[RemoteAssetCacheTraceType]) ObtTrace.addToStat(stat.ExternalMiss, 1)
    ObtTrace.addToStat(stat.Miss, 1)
    debug(id, s"No artifact found for $tpe: $detail")
  }

  final def logWritten(id: ScopeId, tpe: CachedArtifactType, detail: String): Unit =
    logWritten(id, ArtifactCacheTraceType(tpe), detail)
  final def logWritten(id: ScopeId, tpe: CacheTraceType, detail: String): Unit = {
    if (tpe.isInstanceOf[RemoteAssetCacheTraceType]) ObtTrace.addToStat(stat.ExternalWrite, 1)
    ObtTrace.addToStat(stat.Write, 1)
    debug(id, s"Artifact written for $tpe: $detail")
  }

  final def debug(msg: => String): Unit = log.debug(s"$cacheType: $msg")
  final def debug(id: ScopeId, msg: => String): Unit = log.debug(s"[$id] $cacheType: $msg")

  final def info(msg: => String): Unit = log.info(s"$cacheType: $msg")
  final def info(id: ScopeId, msg: => String): Unit = log.info(s"[$id] $cacheType: $msg")

  final def warn(msg: => String): Unit = log.warn(s"$cacheType: $msg")
  final def warn(id: ScopeId, msg: => String): Unit = log.warn(s"[$id] $cacheType: $msg")
  final def warn(msg: => String, t: Throwable): Unit = log.warn(s"$cacheType: $msg", t)
  final def warn(id: ScopeId, msg: => String, t: Throwable): Unit = log.warn(s"[$id] $cacheType: $msg", t)

  private val dedupMessageTimer = new Timer("DedupedMessagePublisher", /*isDaemon*/ true)
  private val warningsToDedup = new BlockingQueue[String]()

  // these messages are accumulated for time (see ArtifactStoreBase.Config.dedupIntervalMillis)
  // and deduplicated before being logged.
  final def warnDedupped(msg: => String): Unit = {
    warningsToDedup.put(msg)
    startPublishingDedupedMessages
  }

  // this is initialized the first time we receive a message to accumulate
  private lazy val startPublishingDedupedMessages: Unit = {
    val task = new TimerTask {
      override def run(): Unit = flushDedupedMessages()
    }
    dedupMessageTimer.schedule(task, /*delay*/ 0L, ArtifactStoreBase.Config.dedupIntervalMillis)
  }

  private def flushDedupedMessages(): Unit =
    warningsToDedup.pollAll().distinct.foreach(msg => warn(msg))

  protected def closeDedupedMessages(): Unit = {
    flushDedupedMessages()
    dedupMessageTimer.cancel()
  }

}
