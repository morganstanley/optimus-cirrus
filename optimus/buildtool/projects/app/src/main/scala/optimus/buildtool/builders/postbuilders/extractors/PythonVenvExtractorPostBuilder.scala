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
package optimus.buildtool.builders.postbuilders.extractors
import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.PythonArtifact
import optimus.buildtool.bsp.BuildServerProtocolService.PythonBspConfig
import optimus.buildtool.builders.postbuilders.FilteredPostBuilder
import optimus.buildtool.compilers.venv.PythonConstants
import optimus.buildtool.compilers.venv.ThinPyappWrapper
import optimus.buildtool.compilers.venv.VenvProvider
import optimus.buildtool.config.PythonConfiguration.OverriddenCommands
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.platform._

import java.io.PrintWriter
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.collection.immutable.Seq
import scala.jdk.CollectionConverters._
import scala.util.Random
object PythonVenvExtractorPostBuilder {
  private[buildtool] def venvFolder(buildDir: Directory): Directory = buildDir.resolveDir("venvs")
  private[buildtool] def mappingFile(buildDir: Directory): FileAsset =
    venvFolder(buildDir).resolveFile("venv-mapping.txt")
  private[buildtool] def latestVenvFile(buildDir: Directory): FileAsset =
    venvFolder(buildDir).resolveFile("latest-venvs.txt")
}

class PythonVenvExtractorPostBuilder(buildDir: Directory, pythonBspConfig: PythonBspConfig)
    extends FilteredPostBuilder {
  private val venvFolder: Directory = PythonVenvExtractorPostBuilder.venvFolder(buildDir)
  private val mappingFile: FileAsset = PythonVenvExtractorPostBuilder.mappingFile(buildDir)
  private val latestVenvFile: FileAsset = PythonVenvExtractorPostBuilder.latestVenvFile(buildDir)

  private val mapping: ConcurrentHashMap[(ScopeId, String), Directory] =
    new ConcurrentHashMap(loadMappingsFile().asJava)
  private val latestVenvs: AtomicReference[LatestVenvs] =
    new AtomicReference[LatestVenvs](LatestVenvs.load(latestVenvFile))

  @async override protected def postProcessFilteredScopeArtifacts(id: ScopeId, artifacts: Seq[Artifact]): Unit = {
    artifacts.foreach {
      case pythonArtifact: PythonArtifact =>
        val venvExtractionDest = venvLocation(id, pythonArtifact.inputsHash)
        if (!venvExtractionDest.exists) {
          Files.createDirectories(venvExtractionDest.path)
          val tpaPath = pythonArtifact.file.path

          val (venv, _) = VenvProvider.ensureVenvExists(
            OverriddenCommands.empty,
            pythonArtifact.python,
            pythonBspConfig.pythonEnvironment)

          ThinPyappWrapper.runTpa(
            pythonArtifact.python,
            Seq(
              PythonConstants.tpa.unpackCmd(
                tpaPath,
                Some(pythonBspConfig.pythonEnvironment.uvCache.path),
                Some(Paths.get(venvExtractionDest.name)),
                Some(venv))
            ),
            Some(venvExtractionDest.parent),
            pythonBspConfig.pythonEnvironment
          )
        }
        latestVenvs.getAndUpdate(old => old.update(pythonArtifact, venvExtractionDest.path))

      case _ => None
    }
    writeMappingsFile()
    latestVenvs.get().writeToFile(latestVenvFile)
  }

  def venvLocation(id: ScopeId, artifactHash: String): Directory =
    mapping.computeIfAbsent((id, artifactHash), { _ => uniqueVenvDir(id) })

  private def venvDir(id: ScopeId, dirName: String): Directory =
    venvFolder.resolveDir(id.meta).resolveDir(id.bundle).resolveDir(dirName)

  private def uniqueVenvDir(id: ScopeId): Directory = {
    @tailrec
    def uniqueDir: Directory = {
      val uniqueDirName: String = id.module + "-" + Random.alphanumeric.take(5).mkString
      val dir = venvDir(id, uniqueDirName)
      if (dir.exists) uniqueDir
      else dir
    }
    uniqueDir
  }

  private def writeMappingsFile(): Unit = {
    val mappingEntries = mapping.asScala.map { case ((scope, hash), mapping) =>
      s"$scope,$hash,${mapping.path.getFileName}"
    }
    IO.using(new PrintWriter(Files.newOutputStream(mappingFile.path)))(pw => mappingEntries.foreach(pw.println))
  }

  private def loadMappingsFile(): Map[(ScopeId, String), Directory] = {
    if (mappingFile.exists) {
      Files
        .readAllLines(mappingFile.path)
        .asScala
        .map { line =>
          line.split(',') match {
            case Array(id, hash, mapping) =>
              val scopeId = ScopeId.parse(id)
              (scopeId, hash) -> venvDir(scopeId, mapping)
          }
        }
        .toMap
    } else {
      Map.empty
    }
  }
}
