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
package optimus.buildtool.builders.postinstallers.uploaders

import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.RelativePath
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.UnhashedJarOutputStream
import optimus.platform.util.Log

import java.nio.file.Files
import scala.collection.mutable
object ArchiveUtil extends Log {
  final case class ArchiveEntry(file: FileAsset, targetPath: RelativePath, sizeBytes: Long = 0)


  def populateZip(
      zipFile: FileAsset,
      files: Seq[ArchiveEntry]
  ): FileAsset =
    AssetUtils.atomicallyWrite(zipFile, replaceIfExists = true, localTemp = true) { tempFile =>
      val jos = new UnhashedJarOutputStream(Files.newOutputStream(tempFile), manifest = None, compressed = true)
      try {
        val paths = mutable.Map[String, FileAsset]()
        files.foreach { f =>
          val rpString = f.targetPath.pathString
          if (!paths.contains(rpString)) {
            paths.put(rpString, f.file)
            jos.copyInFile(f.file.path, f.targetPath)
          } else {
            log.warn(s"Duplicate paths: ${paths.get(rpString)}, $f")
          }
        }
      } finally {
        jos.close()
      }
      zipFile
    }

  def populateDirectory(
      dir: Directory,
      files: Seq[ArchiveEntry]
  ): Directory = {
    val fileTargets = files.map { f =>
      val target = dir.resolveFile(f.targetPath)
      f.file -> target
    }

    val targetDirs = fileTargets.map { case (_, t) => t.parent }.distinct
    targetDirs.foreach(d => Files.createDirectories(d.path))

    fileTargets.foreach { case (s, t) =>
      AssetUtils.atomicallyCopy(s, t)
    }
    dir
  }
}
