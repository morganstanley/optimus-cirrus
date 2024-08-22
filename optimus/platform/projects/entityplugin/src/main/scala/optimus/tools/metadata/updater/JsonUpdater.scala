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
package optimus.tools.metadata.updater

import optimus.config.spray.json.JsValue
import optimus.config.spray.json.JsonParser
import optimus.config.spray.json.enrichAny
import optimus.platform.metadatas.internal.MetaJsonProtocol._
import optimus.platform.metadatas.internal.BaseMetaData
import optimus.platform.metadatas.internal.EmbeddableBaseMetaData
import optimus.platform.metadatas.internal.EntityBaseMetaData
import optimus.platform.metadatas.internal.EventBaseMetaData
import optimus.platform.metadatas.internal.MetaBaseMetaData
import optimus.platform.metadatas.internal.MetaDataFiles

import java.nio.file._
import java.util.function.BiConsumer
import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap

/**
 * Removes specified classes from the metadata json files. This is *only* needed when sources files have been deleted
 * but *no* modifications or additions have taken place. In that case, Zinc never invokes the compiler so the normal
 * logic in OptimusExportInfoComponent doesn't run.
 */
object JsonUpdater {

  /**
   * Remove classes from entity.json.
   *
   * @param outDir         output direction to be set to scala compiler's outDir property.
   * @param deletedClasses fully qualified class name. Should be relative to outDir e.g. optimus.platform.dal.EntityEventModule$EntityBitemporalSpaceImpl
   */
  private[updater] def cleanWhenClassRemoved(outDir: Path, deletedClasses: Set[String]): Unit = {
    val outDirIsJar = outDir.getFileName.toString.endsWith(".jar")
    var fs: FileSystem = FileSystems.getDefault // we cannot call newFileSystem on outDir if its a directory,
    // no provider exception would be thrown
    try {
      if (outDirIsJar)
        fs = FileSystems.newFileSystem(outDir, null.asInstanceOf[ClassLoader])
      MetaDataFiles.metadataFileNames.foreach(fileName => {
        val metaDataFile = if (outDirIsJar) fs.getPath(fileName) else outDir.resolve(fileName)
        if (Files.exists(metaDataFile)) {
          require(Files.isReadable(metaDataFile), s"$metaDataFile is not readable")
          require(Files.isWritable(metaDataFile), s"$metaDataFile is not writable")

          val json = JsonParser(Files.readAllBytes(metaDataFile))
          var res = parseJson(fileName, json) // SortedMap doesn't have mutable version until scala 2.12,
          // currently using var as a workaround
          val originalSize = res.size
          res --= deletedClasses
          val currentSize = res.size
          if (currentSize < originalSize) {
            if (currentSize > 0) {
              val tmp = Files.createTempFile(null, ".json")
              Files.write(tmp, res.values.toJson.prettyPrint.getBytes(MetaDataFiles.fileCharset))
              Files.move(tmp, metaDataFile, StandardCopyOption.REPLACE_EXISTING)
            } else {
              Files.delete(metaDataFile)
            }
          }
        }
      })
    } finally {
      if (outDirIsJar) // on a windows file system we can't close the filesystem, but we still close the jarFileSystem
        fs.close()
    }
  }

  private def parseJson(fileName: String, json: JsValue): SortedMap[String, BaseMetaData] = {
    fileName match {
      case MetaDataFiles.entityMetaDataFileName =>
        TreeMap(json.convertTo[List[EntityBaseMetaData]].map(md => md.fullClassName -> md): _*)
      case MetaDataFiles.storedEntityMetaDataFileName =>
        TreeMap(json.convertTo[List[EntityBaseMetaData]].map(md => md.fullClassName -> md): _*)
      case MetaDataFiles.embeddableMetaDataFileName =>
        TreeMap(json.convertTo[List[EmbeddableBaseMetaData]].map(md => md.fullClassName -> md): _*)
      case MetaDataFiles.eventMetaDataFileName =>
        TreeMap(json.convertTo[List[EventBaseMetaData]].map(md => md.fullClassName -> md): _*)
      case MetaDataFiles.metaSquaredDataFileName =>
        TreeMap(json.convertTo[List[MetaBaseMetaData]].map(md => md.fullClassName -> md): _*)
    }
  }
}

/**
 * Entry point for OBT - it calls us reflectively across a classloader boundary so we can only exchange basic Java types
 */
class JsonUpdaterReflectiveStub extends BiConsumer[Path, Array[String]] {
  override def accept(outDir: Path, classesToRemove: Array[String]): Unit = {
    JsonUpdater.cleanWhenClassRemoved(outDir, classesToRemove.toSet)
  }
}
