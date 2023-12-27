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
package optimus.buildtool.builders
package postbuilders.installer.component

import java.nio.file.Files

import optimus.buildtool.app.ScopedCompilationFactory
import optimus.buildtool.artifacts.CompiledRunconfArtifact
import optimus.buildtool.builders.postbuilders.installer.BatchInstallableArtifacts
import optimus.buildtool.builders.postbuilders.installer.Installer
import optimus.buildtool.compilers.runconfc.{RunConfInventory, RunConfInventoryEntry}
import optimus.buildtool.config.ScopeId.RootScopeId
import optimus.buildtool.config.{MetaBundle, NamingConventions => NC}
import optimus.buildtool.files.InstallPathBuilder
import optimus.buildtool.files.{Directory, FileAsset, RelativePath}
import optimus.buildtool.trace.InstallRunconfs
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.{AsyncUtils, ConsistentlyHashedJarOutputStream, Jars, Utils}
import optimus.platform._
import optimus.platform.util.Log

import scala.collection.immutable.SortedMap
import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq
import optimus.scalacompat.collection._

/**
 * @param uniqueFiles
 *   the list of files which are either specific to a scope or will be the same for all scopes
 * @param propertiess
 *   properties files which may differ from scope to scope; we will merge their contents
 * @param inventory
 *   the inventory of runconf<->scope mappings; we will merge these entries
 */
final case class ProcessedFiles(
    uniqueFiles: Set[String] = Set.empty,
    propertiess: Map[String, Map[String, String]] = Map.empty,
    inventory: Seq[RunConfInventoryEntry] = Nil
)

class BundleRunConfsInstaller(
    installer: Installer,
    factory: ScopedCompilationFactory,
    installPathBuilder: InstallPathBuilder
) extends ComponentBatchInstaller
    with Log {
  import installer._

  override val descriptor = "bundle runconfs"

  @async override def install(installable: BatchInstallableArtifacts): Seq[FileAsset] = {
    installable.metaBundles.apar.flatMap(installBundleRunConfs).toIndexedSeq
  }

  @async private def installBundleRunConfs(mb: MetaBundle): Option[FileAsset] = {
    // Consolidated bundle from compiled runconf jars
    val archive = runConfArchive(mb)
    val jars = collectRunConfArtifacts(mb)
    val hashes = jars.map {
      // This is safe/stable because jars is a SortedMap
      case (scope, artifact) =>
        RelativePath(scope) -> artifact.precomputedContentsHash
    }
    val installed = bundleFingerprints(mb).writeIfAnyChanged(archive, hashes) {
      ObtTrace.traceTask(RootScopeId, InstallRunconfs(mb)) {
        if (jars.nonEmpty) writeRunConfArchive(mb, jars)
        else removeRunConfArchive(mb)
      }
    }
    // if the jars are empty then even if we ran the task we will have removed the file rather than creating it
    installed.filter(_ => jars.nonEmpty)
  }

  private def runConfArchive(metaBundle: MetaBundle): FileAsset =
    installPathBuilder.etcDir(metaBundle).resolveFile(NC.bundleRunConfsJar)

  @async private def collectRunConfArtifacts(metaBundle: MetaBundle): SortedMap[String, CompiledRunconfArtifact] = {
    factory.scopeIds
      .filter(_.metaBundle == metaBundle)
      .apar
      .flatMap(factory.lookupScope)
      .apar
      .flatMap { scoped =>
        scoped.runconfArtifacts.collect { case artifact: CompiledRunconfArtifact =>
          scoped.id.toString -> artifact
        }
      }(SortedMap.breakOut)
  }

  // =============================================
  //  CAUTION: this does incremental installation
  // =============================================
  // Precedence is given to the most recently compiled runconf jar to copy shared
  // runconfs (e.g. global, bundle) and properties.
  //
  @entersGraph
  private def writeRunConfArchive(
      metaBundle: MetaBundle,
      runConfJars: SortedMap[String, CompiledRunconfArtifact]): FileAsset = {
    val archive = runConfArchive(metaBundle)
    Utils.atomicallyWrite(archive) { tempJar =>
      // we don't incrementally rewrite these jars, so might as well compress them and save the disk space
      val tempJarStream =
        new ConsistentlyHashedJarOutputStream(Files.newOutputStream(tempJar), None, compressed = true)
      AsyncUtils.asyncTry {
        // Most recent wins
        // TODO (OPTIMUS-39958): this sorting by mtime fails to take reuse into account
        val summary =
          runConfJars.values.toSeq.sortBy(_.file.lastModified).reverse.foldLeft(ProcessedFiles()) {
            case (acc, artifact) =>
              Jars.withJar(artifact.file) { root =>
                val srcFolder = Directory(root.path.getFileSystem.getPath("src").toAbsolutePath)
                val files = Directory.findFiles(srcFolder)
                val (propertiesFiles, uniqueFiles) =
                  files
                    .filterNot(_.name == NC.runConfInventory)
                    .partition(_.name endsWith NC.capturedPropertiesExtension)
                uniqueFiles
                  .filterNot(f => acc.uniqueFiles.contains(f.path.toString))
                  .foreach { srcFile =>
                    tempJarStream.copyInFile(srcFile.path, RelativePath(srcFile.path.toString.dropWhile(_ == '/')))
                  }

                val propertiess = propertiesFiles.map { file =>
                  val old = acc.propertiess.getOrElse(file.name, Map.empty)
                  file.name -> (Map.empty[String, String]
                    ++ Utils.loadProperties(Files.newInputStream(file.path)).asScala // add this file's props first...
                    ++ old // ... so that the previously processed (i.e., newer) jars win
                  )
                }.toDistinctMap // propertiesFiles contains distinct files by name so we are fine to use this

                val inventoryFile = root.resolveFile(NC.runConfInventory)
                val inventory = RunConfInventory.fromFile(inventoryFile)

                acc.copy(
                  uniqueFiles = acc.uniqueFiles ++ uniqueFiles.map(_.path.toString),
                  propertiess = acc.propertiess ++ propertiess,
                  inventory = acc.inventory ++ inventory
                )
              }
          }

        // Analyze collisions among apps in this m/b and complain if found
        summary.inventory.filter(_.isApp).groupBy(_.name).foreach { case (name, hits) =>
          if (hits.length > 1) {
            val message =
              s"The application '$name' is found in multiple places: ${hits.map(_.scopeId).mkString(", ")}"
            log.error(message)
            throw new IllegalStateException(message)
          }
        }

        summary.propertiess.toSeq.sortBy(_._1).foreach { case (filename, props) =>
          tempJarStream.writeFile(
            Utils.generatePropertiesFileContent(props),
            targetName = RelativePath("src") resolvePath filename
          )
        }

        RunConfInventory.writeFile(tempJarStream, summary.inventory)
      } asyncFinally {
        tempJarStream.close()
      }
      archive
    }
  }

  private def removeRunConfArchive(metaBundle: MetaBundle): Unit = {
    Files.deleteIfExists(runConfArchive(metaBundle).path)
  }
}
