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
import optimus.buildtool.utils.AsyncUtils.asyncTry
import optimus.platform._

trait MultiLevelStoreTrait extends ArtifactStore {
  val underlying: ArtifactStore
  val next: ArtifactStore

  @async override def get[A <: CachedArtifactType](
      id: ScopeId,
      fingerprintHash: String,
      tpe: A,
      discriminator: Option[String]): Option[A#A] =
    underlying.get(id, fingerprintHash, tpe, discriminator) orElse next.get(id, fingerprintHash, tpe, discriminator)

  @async override protected def write[A <: CachedArtifactType](
      tpe: A)(id: ScopeId, fingerprintHash: String, discriminator: Option[String], artifact: A#A): A#A = {
    underlying.put(tpe)(id, fingerprintHash, discriminator, artifact)
    next.put(tpe)(id, fingerprintHash, discriminator, artifact)
  }
  @async override def check[A <: CachedArtifactType](
      id: ScopeId,
      fingerprintHashes: Set[String],
      tpe: A,
      discriminator: Option[String]
  ): Set[String] = {
    val ret = underlying.check(id, fingerprintHashes, tpe, discriminator)
    if (ret.size == fingerprintHashes.size) ret
    else ret ++ next.check(id, fingerprintHashes -- ret, tpe, discriminator)
  }
  override def flush(timeoutMillis: Long): Unit = {
    underlying.flush(timeoutMillis)
    next.flush(timeoutMillis)
  }
  @async override def close(): Unit = {
    underlying.close()
    next.close()
  }
}

class MultiLevelStore(a: ArtifactStore, b: ArtifactStore) extends ArtifactStore with MultiLevelStoreTrait {
  override val underlying: ArtifactStore = a
  override val next: ArtifactStore = b
}

class SearchableMultiLevelStore(a: SearchableArtifactStore, b: SearchableArtifactStore)
    extends SearchableArtifactStore
    with MultiLevelStoreTrait {
  override val underlying: SearchableArtifactStore = a
  override val next: SearchableArtifactStore = b

  @async override def getAll[A <: CachedArtifactType](id: ScopeId, tpe: A, discriminator: Option[String]): Seq[A#A] = {
    underlying.getAll(id, tpe, discriminator) ++ b.getAll(id, tpe, discriminator)
  }
}

object MultiLevelStore {
  def apply(stores: ArtifactStore*): MultiLevelStore = stores match {
    case c1 +: tail if tail.size > 1 =>
      new MultiLevelStore(c1, apply(tail: _*))
    case Seq(c1, c2) =>
      new MultiLevelStore(c1, c2)
    case _ =>
      throw new IllegalArgumentException(s"MultiLevelStores must have at least two members: $stores")
  }

  def apply(stores: SearchableArtifactStore*): SearchableMultiLevelStore = stores match {
    case c1 +: tail if tail.size > 1 =>
      new SearchableMultiLevelStore(c1, apply(tail: _*))
    case Seq(c1, c2) =>
      new SearchableMultiLevelStore(c1, c2)
    case _ =>
      throw new IllegalArgumentException(s"MultiLevelStores must have at least two members: $stores")
  }
}

@entity class MultiLevelCache private (
    val store: MultiLevelStore,
    underlying: ArtifactCache with HasArtifactStore,
    next: ArtifactCache with HasArtifactStore
) extends ArtifactCache
    with HasArtifactStore {

  @node override def getOrCompute$NF[A <: CachedArtifactType](
      id: ScopeId,
      tpe: A,
      discriminator: Option[String],
      fingerprintHash: String
  )(
      computer: NodeFunction0[Option[A#A]]
  ): Option[A#A] = {
    underlying.getOrCompute(id, tpe, discriminator, fingerprintHash)(
      next.getOrCompute(id, tpe, discriminator, fingerprintHash)(computer())
    )
  }

  @async def close(): Unit =
    asyncTry {
      underlying.close()
    } asyncFinally next.close()
}

object MultiLevelCache {
  def apply(caches: ArtifactCache with HasArtifactStore*): MultiLevelCache = caches match {
    case c1 +: tail if tail.size > 1 =>
      MultiLevelCache(MultiLevelStore(caches.map(_.store): _*), c1, apply(tail: _*))
    case Seq(c1, c2) =>
      MultiLevelCache(MultiLevelStore(c1.store, c2.store), c1, c2)
    case _ =>
      throw new IllegalArgumentException(s"MultiLevelCaches must have at least two members: $caches")
  }
}
