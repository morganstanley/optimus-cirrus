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

import java.nio.file.Paths
import optimus.buildtool.app.MischiefOptions
import optimus.buildtool.artifacts.CachedArtifactType
import optimus.buildtool.artifacts.InternalArtifactId
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Asset
import optimus.buildtool.format.FreezerStructure
import optimus.buildtool.trace.ObtStats
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.CompilePathBuilder
import optimus.platform._

import java.nio.file.Files
import java.nio.file.NoSuchFileException
import scala.collection.immutable.Seq
import scala.collection.concurrent.TrieMap

@entity trait FreezerStoreConf {
  @node def freeze(scope: ScopeId): Boolean
  def save: Boolean
}

@entity class ActiveFreezerStore(val config: FreezerStructure, mischiefScope: NodeFunction1[ScopeId, Boolean])
    extends FreezerStoreConf {
  @node override def freeze(scope: ScopeId): Boolean = {
    if (!config.active) false
    else if (mischiefScope(scope)) { false }
    else config.state(scope) == FreezerStructure.Freeze
  }
  override def save: Boolean = config.save
}

@entity class RecordingOnlyFreezerStore() extends FreezerStoreConf {
  @node override def freeze(scope: ScopeId): Boolean = false
  override def save: Boolean = true
}

object FreezerStoreConf {
  @async def load(mischiefOptions: MischiefOptions): FreezerStoreConf =
    mischiefOptions.freezer match {
      case Some(fz) => ActiveFreezerStore(fz, asNode { scope => mischiefOptions.mischief(scope).nonEmpty })
      case None     => RecordingOnlyFreezerStore()
    }
}

class CustomFreezerStore(
    config: FreezerStoreConf,
    pathBuilder: CompilePathBuilder
) extends ArtifactStoreBase
    with SearchableArtifactStore {
  override val cacheType: String = "CustomFreezerStore"
  override val stat: ObtStats.Cache = ObtStats.FilesystemStore
  private val freezerMappingPath = "freezer-mapping.txt"
  private val mappingFile = pathBuilder.outputDir.parent.resolveFile(freezerMappingPath)
  private val mappings: TrieMap[String, String] = readFromFile()

  private def readFromFile() = {
    val map = new TrieMap[String, String]
    new Iterator[String] {
      private val backing =
        try { Files.readAllLines(mappingFile.path) }
        catch {
          case _: NoSuchFileException =>
            info("No freezer mapping file")
            java.util.List.of[String]()
          case t: Throwable =>
            warn("error while trying to retrieve freezer mapping", t)
            java.util.List.of[String]()
        }
      override def hasNext: Boolean = backing.size() > 0
      override def next(): String = backing.remove(0)
    }
      .map(s =>
        s.indexOf("\t") match {
          case i if i > -1 => Some(s.substring(0, i), s.substring(i + 1, s.length))
          case _           => None
        })
      .foreach {
        case Some((scope, path)) => map.update(scope, path)
        case _                   =>
      }

    map
  }

  private def freezerGet[A <: CachedArtifactType](id: ScopeId, tpe: A, discriminator: Option[String]): Option[Asset] = {
    mappings.get(InternalArtifactId(id, tpe, discriminator).fingerprint).map(p => Asset(Paths.get(p))).filter(_.exists)
  }

  @async override def get[A <: CachedArtifactType](
      id: ScopeId,
      fingerprintHash: String,
      tpe: A,
      discriminator: Option[String]
  ): Option[A#A] = {
    if (config.freeze(id)) {
      freezerGet(id, tpe, discriminator).flatMap { asset =>
        if (asset.exists) {
          logFound(id, tpe, s"using ${asset.name}")
          Some(tpe.fromAsset(id, asset))
        } else {
          logNotFound(id, tpe, "frozen artifact has no known asset!")
          None
        }
      }
    } else {
      None
    }
  }

  @async override def check[A <: CachedArtifactType](
      id: ScopeId,
      fpHashes: Set[String],
      tpe: A,
      discriminator: Option[String]
  ): Set[String] = {
    if (config.freeze(id)) {
      if (freezerGet(id, tpe, discriminator).exists(_.exists)) fpHashes else Set.empty
    } else {
      Set.empty
    }
  }

  @async override def getAll[A <: CachedArtifactType](id: ScopeId, tpe: A, discriminator: Option[String]): Seq[A#A] = {
    if (config.freeze(id))
      freezerGet(id, tpe, discriminator)
        .map(a => Seq(tpe.fromAsset(id, a)))
        .getOrElse(Seq.empty)
        .filter(_.exists)
    else Seq.empty
  }

  /**
   * Update the internal mapping dictionary with a new artifact.
   */
  @async override def write[A <: CachedArtifactType](
      tpe: A
  )(id: ScopeId, fpHash: String, discriminator: Option[String], artifact: A#A): A#A = {
    if (config.save)
      mappings.put(InternalArtifactId(id, tpe, discriminator).fingerprint, artifact.path.toAbsolutePath.toString)
    artifact
  }

  /**
   * Write the current mapping dictionary to disk
   */
  override def flush(timeoutMillis: Long): Unit = if (config.save) {
    Files.createDirectories(mappingFile.parent.path)
    try {
      AssetUtils.atomicallyWrite(mappingFile, replaceIfExists = true) { p =>
        val writer = Files.newBufferedWriter(p)
        mappings.iterator
          .map { case (k, v) =>
            s"$k\t${v}\n"
          }
          .foreach(writer.write)
        writer.flush()
        writer.close()
      }
    } catch {
      case e: Throwable => warn("error while writing freezer mapping", e)
    }
  }
}

object FreezerCache {
  def apply(
      conf: FreezerStoreConf,
      compilePathBuilder: CompilePathBuilder
  ): SimpleArtifactCache[CustomFreezerStore] =
    SimpleArtifactCache(new CustomFreezerStore(conf, compilePathBuilder), cacheMode = CacheMode.ReadWrite)
}
