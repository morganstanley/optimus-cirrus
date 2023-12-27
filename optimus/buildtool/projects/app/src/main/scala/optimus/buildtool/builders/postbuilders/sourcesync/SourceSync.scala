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
package optimus.buildtool.builders.postbuilders.sourcesync

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.GeneratedSourceArtifact
import optimus.buildtool.builders.postbuilders.FilteredPostBuilder
import optimus.buildtool.config.ScopeId.RootScopeId
import optimus.buildtool.config.ScopeConfiguration
import optimus.buildtool.config.ScopeConfigurationSource
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.trace
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.Hashing
import optimus.buildtool.utils.Jars
import optimus.buildtool.utils.Utils
import optimus.platform._
import optimus.platform.util.Log

import scala.collection.immutable.Seq
import scala.collection.compat._

trait SourceSync extends FilteredPostBuilder with Log {

  protected def scopeConfigSource: ScopeConfigurationSource

  @async override protected def postProcessFilteredScopeArtifacts(id: ScopeId, artifacts: Seq[Artifact]): Unit =
    syncScopeArtifacts(id, artifacts)

  @async override protected def postProcessFilteredTransitiveArtifacts(
      transitiveArtifacts: Map[ScopeId, collection.Seq[Artifact]]
  ): Unit =
    transitiveArtifacts.foreach { case (id, artifacts) => syncScopeArtifacts(id, artifacts.to(Seq)) }

  @async protected def syncScopeArtifacts(id: ScopeId, artifacts: Seq[Artifact]): Unit = if (id != RootScopeId) {
    val scopeConfig = scopeConfigSource.scopeConfiguration(id)
    if (scopeConfig.generatorConfig.nonEmpty) {
      val generatedSourceArtifacts =
        artifacts.collect { case a: GeneratedSourceArtifact => a }.sortBy(_.pathFingerprint)
      if (generatedSourceArtifacts.nonEmpty) {
        target(id, scopeConfig).foreach { targetDir =>
          val hashFile = targetDir.resolveFile("hash.txt")

          val previousHash =
            if (hashFile.exists)
              Some(new String(Files.readAllBytes(hashFile.path), StandardCharsets.UTF_8).trim)
            else
              None

          val newHash = Hashing.hashStrings(generatedSourceArtifacts.apar.map(SourceSync.hash))

          if (!previousHash.contains(newHash)) {
            ObtTrace.traceTask(id, trace.SourceSync) {
              if (targetDir.exists)
                AssetUtils.recursivelyDelete(targetDir, _ != targetDir.path) // delete the contents, but keep targetDir

              log.debug(s"[$id] Syncing $descriptor to $targetDir")
              val (installTime, installedFiles) = AdvancedUtils.timed {
                // aseq here so we handle the case where multiple artifacts contain the same file
                val count = generatedSourceArtifacts.aseq.map { a =>
                  Jars.withJar(a.sourceJar) { root =>
                    val sourceDir = source(a, root)
                    sourceDir
                      .filter(_.exists)
                      .map { d =>
                        Utils.recursivelyCopyAndCount(d, targetDir)
                      }
                      .getOrElse {
                        log.debug(s"Skipping $a which does not contain $sourceDir")
                        0
                      }
                  }
                }.sum
                Files.createDirectories(hashFile.parent.path)
                AssetUtils.atomicallyWrite(hashFile)(Utils.writeStringsToFile(_, Seq(newHash)))
                count
              }
              log.debug(f"[$id] Synced $installedFiles $descriptor in ${installTime / 1000000000.0}%.1fs")
            }
          }
        }
      }
    }
  }

  protected val descriptor: String

  @node protected def source(artifact: GeneratedSourceArtifact, jarRoot: Directory): Option[Directory]
  @node protected def target(id: ScopeId, scopeConfig: ScopeConfiguration): Option[Directory]
}

@entity object SourceSync {
  // Artifact contents are RT, so it's safe to cache the hashed content
  @node private def hash(artifact: GeneratedSourceArtifact): String = Hashing.hashFileContent(artifact.sourceJar)
}
