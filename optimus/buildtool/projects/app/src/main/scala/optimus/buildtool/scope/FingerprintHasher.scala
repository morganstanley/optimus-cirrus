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
package optimus.buildtool.scope

import optimus.buildtool.artifacts.ArtifactType.CompilationFingerprint
import optimus.buildtool.artifacts.FingerprintArtifact
import optimus.buildtool.artifacts.FingerprintArtifactType
import optimus.buildtool.artifacts.InternalArtifactId
import optimus.buildtool.cache.ArtifactStore
import optimus.buildtool.config.ScopeId
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.CompilePathBuilder
import optimus.buildtool.utils.Hashing
import optimus.buildtool.utils.Utils
import optimus.platform._

import scala.collection.immutable.Seq

@entity class FingerprintHasher(
    id: ScopeId,
    pathBuilder: CompilePathBuilder,
    store: ArtifactStore,
    freezeHash: Option[String],
    mischief: Boolean
) {

  @node def hashFingerprint(
      fingerprint: Seq[String],
      tpe: FingerprintArtifactType,
      discriminator: Option[String] = None
  ): FingerprintArtifact = {
    log.debug(s"[$id] Calculating hashed fingerprint for $tpe...")
    val hashPrefix = Hashing.hashStrings(fingerprint ++ freezeHash)
    // we use Z for freezer because F could be interpreted as part of the hash!
    val hash: String = hashPrefix + (if (freezeHash.nonEmpty) "Z" else "") + (if (mischief) "M" else "")
    log.debug(s"[$id] Hashed fingerprint (${fingerprint.size}) for $tpe: $hash")
    if (fingerprint.nonEmpty) {
      val path = pathBuilder.outputPathFor(id, hash, tpe, discriminator, incremental = false)
      AssetUtils.atomicallyWriteIfMissing(path) { tmp =>
        Utils.writeStringsToFile(tmp, fingerprint)
      }

      // Note that we deliberately create the artifact here (even if we're not going to write it to the store below)
      // so that we watch for its deletion
      val artifact = FingerprintArtifact.create(InternalArtifactId(id, tpe, discriminator), path, fingerprint, hash)
      tpe match {
        // Most of the time, there's no reason to cache the fingerprints.
        // CompilationFingerprints are useful for debugging though, so we would like them to be cached.
        case CompilationFingerprint => store.put(CompilationFingerprint)(id, hash, discriminator, artifact)
        case _                      => // do nothing
      }
      artifact
    } else FingerprintArtifact.empty(InternalArtifactId(id, tpe, discriminator), hash)
  }
}
