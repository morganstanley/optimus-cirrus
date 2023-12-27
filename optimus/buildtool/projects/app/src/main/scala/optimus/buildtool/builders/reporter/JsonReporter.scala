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
package optimus.buildtool.builders.reporter

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import optimus.buildtool.artifacts.MessagesArtifact
import optimus.buildtool.builders.BuildResult.CompletedBuildResult
import optimus.buildtool.builders.postbuilders.codereview.CodeReviewAnalysisProducer
import optimus.buildtool.builders.postbuilders.codereview.CodeReviewSettings
import optimus.buildtool.builders.postbuilders.metadata.MetaBundleReport
import optimus.buildtool.builders.postbuilders.metadata.MetadataSettings
import optimus.buildtool.config.MetaBundle
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.ObtConfig
import optimus.buildtool.config.ScopeConfiguration
import optimus.buildtool.config.ScopeId
import optimus.buildtool.config.StaticConfig
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.utils.FileDiff
import optimus.platform._
import optimus.platform.util.Log
import spray.json._

class JsonReporter(
    obtConfig: ObtConfig,
    codeReviewSettings: Option[CodeReviewSettings],
    metadataSettings: Option[MetadataSettings])
    extends Log {

  @async def writeAnalysis(msgs: Seq[MessagesArtifact], modifiedFiles: Option[FileDiff]): Option[FileAsset] =
    codeReviewSettings.map { settings =>
      val analysis =
        CodeReviewAnalysisProducer.fromCompilationMessages(msgs.flatMap(_.messages), settings, modifiedFiles)
      writeJsonFile(settings.dir, analysis, StaticConfig.string("codeReviewAnalysisFile"))
    }

  @async def writeMetadataReports(buildResult: CompletedBuildResult): Seq[FileAsset] = {
    val scopeIds = buildResult.scopeIds
    metadataSettings.toSeq.apar.flatMap { settings =>
      val isMavenRelease = settings.generatePoms
      val scopeConfigurations: Map[ScopeId, ScopeConfiguration] =
        if (isMavenRelease) {
          scopeIds
            .filter(id => id.tpe == NamingConventions.MavenInstallScope)
            .apar
            .map(id => id -> obtConfig.scopeConfiguration(id))
            .toMap
        } else scopeIds.apar.map(id => id -> obtConfig.scopeConfiguration(id)).toMap

      if (isMavenRelease) {
        scopeConfigurations.toIndexedSeq.apar.map { case (id, config) =>
          val mavenBundle = MetaBundle("com.ms." + id.metaBundle.toString, id.module)
          val metaBundle = NamingConventions.MavenCIScope.split("/")
          val codetreeBundle = MetaBundle(metaBundle(0), metaBundle(1))
          val metadataDir =
            settings.installPathBuilder.primaryInstallDir(codetreeBundle, settings.leafDir.toString)
          val metaBundleReport = MetaBundleReport(settings, mavenBundle, id, config)
          val file = writeJsonFile(metadataDir, metaBundleReport, s"$mavenBundle-metadata.json")
          log.info(s"Metadata for maven lib $mavenBundle generated - see ${file.pathString}")
          file
        }
      } else {
        scopeConfigurations.groupBy { case (id, _) => id.metaBundle }.toIndexedSeq.apar.map { case (bundle, configs) =>
          val metaBundleReport = MetaBundleReport(settings, bundle, configs)
          val metadataDir = settings.installPathBuilder.primaryInstallDir(bundle, settings.leafDir.toString)
          val file = writeJsonFile(metadataDir, metaBundleReport, s"$bundle-metadata.json")
          log.info(s"Metadata for bundle $bundle generated - see ${file.pathString}")
          file
        }
      }
    }
  }

  @async private def writeJsonFile[T: RootJsonFormat](dir: Directory, entity: T, fileName: String): FileAsset = {
    val dest = dir.path.resolve(fileName)
    val content = entity.toJson.compactPrint.getBytes(StandardCharsets.UTF_8)
    Files.createDirectories(dest.getParent)
    FileAsset(Files.write(dest, content))
  }
}
