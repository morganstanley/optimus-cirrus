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
import optimus.platform._
import optimus.platform.annotations.alwaysAutoAsyncArgs

sealed abstract class CacheMode(val read: Boolean, val write: Boolean, val forceWrite: Boolean = false)
object CacheMode {
  case object ReadWrite extends CacheMode(read = true, write = true)
  case object ReadOnly extends CacheMode(read = true, write = false)
  case object WriteOnly extends CacheMode(read = false, write = true)
  case object ForceWrite extends CacheMode(read = true, write = true, forceWrite = true)
  case object ForceWriteOnly extends CacheMode(read = false, write = true, forceWrite = true)
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

  @node override def getOrCompute$NF[B <: CachedArtifactType](
      id: ScopeId,
      tpe: B,
      discriminator: Option[String],
      fingerprintHash: String
  )(
      computer: NodeFunction0[Option[B#A]]
  ): Option[B#A] = {
    val stored = store.get(id, fingerprintHash, tpe, discriminator)
    val read = if (cacheMode.read) stored else None
    val ret = read match {
      case None =>
        computer() // no artifact found in cache
      case Some(a: MessagesArtifact) if ignoreErroneousArtifacts && a.hasErrors =>
        computer() // don't use cached errors if ignoreErroneousArtifacts == true
      case a =>
        a // we've got a non-erroneous artifact
    }
    // even in write-only mode, don't write if it already exists
    if (cacheMode.write && (stored != ret || cacheMode.forceWrite))
      ret.foreach(a => store.put(tpe)(id, fingerprintHash, discriminator, a))
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
  private val IgnoreErroneousArtifacts =
    sys.props.get("optimus.buildtool.ignoreErroneousArtifacts").contains("true") // default to false
}
