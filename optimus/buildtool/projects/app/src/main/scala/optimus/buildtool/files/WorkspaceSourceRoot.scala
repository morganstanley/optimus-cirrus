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
package optimus.buildtool.files

import java.io.IOException
import java.nio.file.FileVisitResult
import java.nio.file.FileVisitor
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes
import java.time.Instant

import optimus.buildtool.files.Directory.PathFilter
import optimus.platform._
import optimus.platform.util.Log

import scala.collection.mutable

@entity class WorkspaceSourceRoot(val path: Path) extends ReactiveDirectory {
  import WorkspaceSourceRoot._

  @node def obtFiles: Seq[FileAsset] = configFilesByExtension("obt")

  @node def testplanFiles: Seq[FileAsset] = configFilesByExtension("testplan.json")

  @node private def configFilesByExtension(ext: String): Seq[FileAsset] = {
    declareVersionDependence()
    try {
      if (existsUnsafe) {
        val buffer = mutable.ArrayBuffer[(Path, Instant)]()
        Files.walkFileTree(path, new ExtensionFilterFileVisitor(ext, path, buffer))
        buffer.sorted.map { case (file, lastModified) =>
          FileAsset.timestamped(file, lastModified)
        }
      } else Nil
    } catch {
      case e: NoSuchFileException =>
        log.error(s"Workspace source root folder $path or files within were not found", e)
        Nil
    }
  }
}

object WorkspaceSourceRoot extends Log {

  def apply(d: Directory): WorkspaceSourceRoot = WorkspaceSourceRoot(d.path)

  private[buildtool] val isConfigFile: PathFilter =
    Directory.fileExtensionPredicate("conf")

  private class ExtensionFilterFileVisitor(ext: String, rootPath: Path, files: mutable.ArrayBuffer[(Path, Instant)])
      extends FileVisitor[Path] {
    override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult =
      if (skipDirectory(dir)) FileVisitResult.SKIP_SUBTREE else FileVisitResult.CONTINUE

    private val skippableDirectories =
      Set("ivy-repo", "profiles", "main", "generated", "tests-unit", "tests-functional", "tests-functional-unstable") ++
        Set("tests-smoke", "test-ui")

    private def skipDirectory(dir: Path): Boolean =
      (dir.endsWith("src") && dir != rootPath) || {
        val dirName = dir.getFileName.toString
        dirName.startsWith(".") || skippableDirectories.contains(dirName)
      }

    override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
      if (file.getFileName.toString.endsWith("." + ext)) files.append((file, attrs.lastModifiedTime.toInstant))
      FileVisitResult.CONTINUE
    }

    override def visitFileFailed(file: Path, exc: IOException): FileVisitResult = {
      log.error(s"Failed to visit file $file", exc)
      FileVisitResult.CONTINUE
    }

    override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
      if (exc != null) log.error(s"Failed to visit directory $dir", exc)
      FileVisitResult.CONTINUE
    }

  }
}
