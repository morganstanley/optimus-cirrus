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

import optimus.buildtool.files.FileAsset
import optimus.platform._

import java.net.URL

trait RemoteAssetStore {
  @async def get(url: URL, destination: FileAsset): Option[FileAsset]
  @async def put(url: URL, file: FileAsset): FileAsset
  @async def check(url: URL): Boolean

  def cacheMode: CacheMode
}
object RemoteAssetStore {
  lazy val externalArtifactVersion: String =
    sys.props.get("optimus.buildtool.cache.externalArtifactVersion").getOrElse("external-1.1")
}

object NoOpRemoteAssetStore extends RemoteAssetStore {
  @async override def get(url: URL, destination: FileAsset): Option[FileAsset] = None
  @async override def put(url: URL, file: FileAsset): FileAsset = file
  @async def check(url: URL): Boolean = false

  def cacheMode: CacheMode = CacheMode.ReadWrite
}

trait WriteableArtifactStore extends RemoteAssetStore with ArtifactStore {
  def logStatus(): Seq[String]
  def incompleteWrites: Int
}
