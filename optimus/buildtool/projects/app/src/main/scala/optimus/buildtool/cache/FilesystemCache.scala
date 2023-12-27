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

import optimus.buildtool.app.IncrementalMode
import optimus.buildtool.config.ScopeId
import optimus.buildtool.artifacts.CachedArtifactType
import optimus.buildtool.artifacts.IncrementalArtifact
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.trace.ObtStats
import optimus.buildtool.utils.CompilePathBuilder
import optimus.platform._

class FileSystemStore(pathBuilder: CompilePathBuilder, incrementalMode: IncrementalMode)
    extends ArtifactStoreBase
    with SearchableArtifactStore {

  override val cacheType: String = "FileSystem"
  override val stat: ObtStats.Cache = ObtStats.FilesystemStore

  @async override def get[A <: CachedArtifactType](
      id: ScopeId,
      fingerprintHash: String,
      tpe: A,
      discriminator: Option[String]): Option[A#A] = {
    val nonIncrementalFile = pathBuilder.outputPathFor(id, fingerprintHash, tpe, discriminator, incremental = false)
    if (nonIncrementalFile.existsUnsafe) {
      logFound(id, tpe, nonIncrementalFile.pathString)
      Some(tpe.fromAsset(id, nonIncrementalFile, incremental = false))
    } else {
      if (incrementalMode.useIncrementalArtifacts) {
        val incrementalFile = pathBuilder.outputPathFor(id, fingerprintHash, tpe, discriminator, incremental = true)
        if (incrementalFile.existsUnsafe) {
          logFound(id, tpe, incrementalFile.pathString)
          Some(tpe.fromAsset(id, incrementalFile, incremental = true))
        } else {
          // In incremental mode, only log the not-found paths if neither of them could be found
          logNotFound(id, tpe, nonIncrementalFile.pathString)
          logNotFound(id, tpe, incrementalFile.pathString)
          None
        }
      } else {
        logNotFound(id, tpe, nonIncrementalFile.pathString)
        None
      }
    }
  }

  @async override def getAll[A <: CachedArtifactType](id: ScopeId, tpe: A, discriminator: Option[String]): Seq[A#A] = {
    val dir = pathBuilder.outputDirFor(tpe, discriminator)
    // Since this method is legitimately called from a @node in such a way that the non-RT nature of this
    // method is irrelevant, we need to use `listFilesUnsafe` here until we have a construct to allow us to call
    // @impure from @nodes
    val files = Directory.listFilesUnsafe(dir, (path, _) => path.getFileName.toString.startsWith(id.properPath))
    if (files.nonEmpty) {
      logFound(id, tpe, files.map(_.pathString))
      files.apar.map(f => tpe.fromAsset(id, f, NamingConventions.isIncremental(f)))
    } else {
      logNotFound(id, tpe, s"No relevant artifacts in directory ${dir.pathString}")
      Nil
    }
  }

  @async override protected def write[A <: CachedArtifactType](
      tpe: A)(id: ScopeId, fingerprintHash: String, discriminator: Option[String], artifact: A#A): A#A = {
    // no put required - compiler job will automatically place artifacts in the correct location
    val path = artifact.path
    val incremental = artifact match {
      case ia: IncrementalArtifact => ia.incremental
      case _                       => false
    }
    val expectedFile = pathBuilder.outputPathFor(id, fingerprintHash, tpe, discriminator, incremental)
    // likely something else is wrong if these messages pop up, however we don't need to fail the compilation because we
    // have a local cache error
    if (!FileAsset(path).existsUnsafe)
      warn(s"File at $path should be part of the filesystem cache, but it doesn't exist!")
    if (path != expectedFile.path) warn(s"File at $path should really be at ${expectedFile.path}!")
    artifact
  }

  @async override def check[A <: CachedArtifactType](
      id: ScopeId,
      fingerprintHashes: Set[String],
      tpe: A,
      discriminator: Option[String]
  ): Set[String] = {
    val useIncrementalArtifacts = incrementalMode.useIncrementalArtifacts
    val ret = fingerprintHashes.apar.filter { fp =>
      pathBuilder.outputPathFor(id, fp, tpe, discriminator, incremental = false).existsUnsafe || (
        useIncrementalArtifacts && pathBuilder
          .outputPathFor(id, fp, tpe, discriminator, incremental = true)
          .existsUnsafe
      )
    }
    debug(s"[$id] Found $ret out of $fingerprintHashes")
    ret
  }
}

object FilesystemCache {
  def apply(pathBuilder: CompilePathBuilder, incrementalMode: IncrementalMode): SimpleArtifactCache[FileSystemStore] =
    SimpleArtifactCache(new FileSystemStore(pathBuilder, incrementalMode))
}
