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
import optimus.buildtool.files.FileAsset
import optimus.buildtool.trace.ObtStats
import optimus.platform._
import optimus.stratosphere.config.StratoWorkspace

import java.net.URL

trait MultiWriteableArtifactStore extends RemoteAssetStore with ArtifactStore {
  def logStatus(): Seq[String]
  def incompleteWrites: Int
}

class MultiWriteArtifactStore(
    val authoritativeStore: MultiWriteableArtifactStore,
    val otherStores: Seq[MultiWriteableArtifactStore],
    val stratoWorkspace: StratoWorkspace,
    val cacheMode: CacheMode)
    extends ArtifactStoreBase
    with MultiWriteableArtifactStore {

  @async override def close(): Unit = (authoritativeStore +: otherStores).apar.foreach(_.close())

  @async override protected[buildtool] def write[A <: CachedArtifactType](
      tpe: A)(id: ScopeId, fingerprintHash: String, discriminator: Option[String], artifact: A#A): A#A = {
    val result = authoritativeStore.write(tpe)(id, fingerprintHash, discriminator, artifact)
    otherStores.apar.foreach { store =>
      store.write(tpe)(id, fingerprintHash, discriminator, artifact)
    }
    result
  }
  override def flush(timeoutMillis: Long): Unit = {
    authoritativeStore.flush(timeoutMillis)
    otherStores.foreach(_.flush(timeoutMillis))
  }
  @async override def get[A <: CachedArtifactType](
      id: ScopeId,
      fingerprintHash: String,
      tpe: A,
      discriminator: Option[String]): Option[A#A] = {
    val result = authoritativeStore.get(id, fingerprintHash, tpe, discriminator)

    // don't multi-get

    result
  }
  @async override def check[A <: CachedArtifactType](
      id: ScopeId,
      fingerprintHashes: Set[String],
      tpe: A,
      discriminator: Option[String]): Set[String] = {
    val result = authoritativeStore.check(id, fingerprintHashes, tpe, discriminator)

    // don't multi-check

    result
  }
  override protected def cacheType: String = s"RemoteComparing"
  override protected def stat: ObtStats.Cache = ObtStats.RemoteComparing

  @async override def get(url: URL, destination: FileAsset): Option[FileAsset] = {
    implicit val result: Option[FileAsset] = authoritativeStore.get(url, destination)

    // don't multi-read

    result
  }
  @async override def put(url: URL, file: FileAsset): FileAsset = {
    val result = authoritativeStore.put(url, file)

    // DO multi-write
    otherStores.apar.foreach { store =>
      store.put(url, file)
    }

    result
  }
  @async override def check(url: URL): Boolean = {
    val result = authoritativeStore.check(url)
    // don't multi-check
    result
  }
  override def logStatus(): Seq[String] = authoritativeStore.logStatus()
  override def incompleteWrites: Int = authoritativeStore.incompleteWrites + otherStores.map(_.incompleteWrites).sum
}
