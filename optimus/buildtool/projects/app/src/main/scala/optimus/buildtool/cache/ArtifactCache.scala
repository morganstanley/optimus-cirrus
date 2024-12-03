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
import optimus.buildtool.artifacts.MessagesArtifact
import optimus.buildtool.config.ScopeId
import optimus.buildtool.resolvers.RemoteDownloadTracker
import optimus.buildtool.trace.ObtTrace
import optimus.platform._
import optimus.platform.annotations.alwaysAutoAsyncArgs
import org.slf4j.LoggerFactory.getLogger

import java.nio.file.Files
import java.util.concurrent.ConcurrentHashMap
import scala.collection.compat._
import scala.collection.mutable
import scala.collection.immutable.Seq
import scala.jdk.CollectionConverters._

sealed abstract class CacheMode(
    val read: Boolean,
    private val write: Boolean,
    private val forceWrite: Boolean = false
) {
  final def canWrite: Boolean = write || forceWrite
}
object CacheMode {
  case object ReadWrite extends CacheMode(read = true, write = true)
  case object ReadOnly extends CacheMode(read = true, write = false)
  case object WriteOnly extends CacheMode(read = false, write = true)
  case object ForceWrite extends CacheMode(read = true, write = true, forceWrite = true)
  case object ForceWriteOnly extends CacheMode(read = false, write = true, forceWrite = true)
  def unapply(cacheMode: CacheMode): Option[(Boolean, Boolean, Boolean)] = {
    Some((cacheMode.read, cacheMode.write, cacheMode.forceWrite))
  }
}

object RemoteArtifactCacheTracker extends RemoteDownloadTracker {
  private val log = getLogger(this.getClass)

  private val _corruptedFiles: mutable.Set[String] = ConcurrentHashMap.newKeySet[String]().asScala

  def addCorruptedFile(cacheKey: String, id: ScopeId): Boolean = {
    val msg = s"[$id] corrupted file found for remote cache: $cacheKey"
    ObtTrace.warn(msg)
    log.warn(msg)
    _corruptedFiles.add(cacheKey)
  }

  def corruptedFiles: Set[String] = _corruptedFiles.toSet

  def clean(): Unit = {
    _corruptedFiles.clear()
  }

  def summary: Seq[String] = {
    val files: Seq[String] = corruptedFiles.to(Seq).sorted
    val summaryLines: Seq[String] =
      if (files.nonEmpty) s"Downloaded ${files.size} corrupted files in this build:" +: files
      else Nil
    summaryLines
  }

}

@entity trait ArtifactCache {
  @alwaysAutoAsyncArgs def getOrCompute[A <: CachedArtifactType](
      id: ScopeId,
      tpe: A,
      discriminator: Option[String],
      fingerprintHash: String
  )(
      computer: => Option[A#A]
  ): Option[A#A] = throw new IllegalStateException
  @node def getOrCompute$NF[A <: CachedArtifactType](
      id: ScopeId,
      tpe: A,
      discriminator: Option[String],
      fingerprintHash: String
  )(
      computer: NodeFunction0[Option[A#A]]
  ): Option[A#A]

  @async def close(): Unit
}

trait HasArtifactStore {
  def store: ArtifactStore
}

@entity trait ArtifactCacheBase extends ArtifactCache with HasArtifactStore {

  protected def cacheMode: CacheMode
  protected def ignoreErroneousArtifacts: Boolean

  @async def onWrite[B <: CachedArtifactType](
      id: ScopeId,
      tpe: B,
      discriminator: Option[String],
      fingerprintHash: String,
      artifact: B#A
  ) = {}

  @node override def getOrCompute$NF[B <: CachedArtifactType](
      id: ScopeId,
      tpe: B,
      discriminator: Option[String],
      fingerprintHash: String
  )(
      computer: NodeFunction0[Option[B#A]]
  ): Option[B#A] = {
    val read: Option[B#A] = if (cacheMode.read) store.get(id, fingerprintHash, tpe, discriminator) else None
    val ret = read match {
      case None =>
        computer() // no artifact found in cache
      case Some(a: MessagesArtifact) if ignoreErroneousArtifacts && a.hasErrors =>
        computer() // don't use cached errors if ignoreErroneousArtifacts == true
      case a =>
        a // we've got a non-erroneous artifact
    }

    val shouldWrite = cacheMode match {
      case CacheMode(true, true, forceWrite) if forceWrite || ret != read => true
      case CacheMode(false, true, forceWrite) // even in write-only mode, don't write if it already exists
          if forceWrite || (ret.nonEmpty && store.check(id, Set(fingerprintHash), tpe, discriminator).isEmpty) =>
        true
      case _ => // write == false
        false
    }
    if (shouldWrite) {
      log.trace(s"${store.getClass.getName}:$cacheMode PUT $id:$tpe:$discriminator:$fingerprintHash")
      ret.foreach { a =>
        store.put(tpe)(id, fingerprintHash, discriminator, a)
        onWrite(id, tpe, discriminator, fingerprintHash, a)
      }
    }
    ret
  }

  @async override def close(): Unit = store.close()
}

@entity class SimpleArtifactCache[+A <: ArtifactStore](
    override val store: A,
    override val cacheMode: CacheMode = CacheMode.ReadWrite,
    override val ignoreErroneousArtifacts: Boolean = SimpleArtifactCache.IgnoreErroneousArtifacts
) extends ArtifactCacheBase

object SimpleArtifactCache {
  private[cache] val IgnoreErroneousArtifacts =
    sys.props.get("optimus.buildtool.ignoreErroneousArtifacts").contains("true") // default to false
}

@entity class RemoteReadThroughTriggeringArtifactCache[+A <: ArtifactStore](
    override val store: A,
    val readThroughStores: Set[_ <: WriteableArtifactStore],
    val artifactSizeThresholdBytes: Long,
    override val cacheMode: CacheMode = CacheMode.ReadWrite,
    override val ignoreErroneousArtifacts: Boolean = SimpleArtifactCache.IgnoreErroneousArtifacts
) extends SimpleArtifactCache[A](store, cacheMode, ignoreErroneousArtifacts) {
  @async override def onWrite[B <: CachedArtifactType](
      id: ScopeId,
      tpe: B,
      discriminator: Option[String],
      fingerprintHash: String,
      artifact: B#A): Unit = {
    if (artifact.shouldBeStored) { // this guard matches the guard on put
      val size = Files.size(artifact.path)
      if (size >= artifactSizeThresholdBytes) {
        readThroughStores.apar.foreach { s =>
          log.debug(
            s"Force read through for key '$id:$fingerprintHash:$tpe:$discriminator' ($size bytes) on '${s.toString}'")
          s.check(id, Set(fingerprintHash), tpe, discriminator)
        }
      }
    } else {
      log.debug(s"Skipping forced read through of ${artifact.pathString} (contains errors? ${artifact.hasErrors})")
    }
  }
}
