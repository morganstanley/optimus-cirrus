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
import optimus.platform._

@entity object EmptyCache extends SimpleArtifactCache(EmptyStore)

object EmptyStore extends SearchableArtifactStore {
  @async override protected def write[A <: CachedArtifactType](
      tpe: A
  )(id: ScopeId, fingerprintHash: String, discriminator: Option[String], artifact: A#A): A#A =
    artifact
  @async override def get[A <: CachedArtifactType](
      id: ScopeId,
      fingerprintHash: String,
      tpe: A,
      discriminator: Option[String]
  ): Option[A#A] = None
  @async override def getAll[A <: CachedArtifactType](id: ScopeId, tpe: A, discriminator: Option[String]): Seq[A#A] =
    Nil
  @async override def check[A <: CachedArtifactType](
      id: ScopeId,
      fingerprintHashes: Set[String],
      tpe: A,
      discriminator: Option[String]
  ): Set[String] =
    Set.empty
  override def flush(timeoutMillis: Long): Unit = ()
  @async override def close(): Unit = ()
}
