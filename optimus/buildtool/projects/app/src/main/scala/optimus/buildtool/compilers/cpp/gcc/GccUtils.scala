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
package optimus.buildtool.compilers.cpp.gcc

import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes

import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.Pathed
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.PathUtils.ErrorIgnoringFileVisitor

object GccUtils {

  def copyToSandbox(dir: Directory, sandboxRoot: Directory): Unit = {
    val fsRoot = Directory.root(dir.fileSystem)

    // use `toString` here so we can transition between filesystems
    def sandbox(p: Path): Path = sandboxRoot.path.resolve(Pathed.relativize(fsRoot.path, p).toString)

    def copySymLink(p: Path): Unit = {
      val sandboxed = sandbox(p)
      val target = Files.readSymbolicLink(p)
      if (target.isAbsolute)
        Files.createSymbolicLink(sandboxed, sandboxRoot.path.resolve(Pathed.relativize(fsRoot.path, target).toString))
      else
        Files.createSymbolicLink(sandboxed, sandboxRoot.fileSystem.getPath(target.toString))
    }

    // we may have already copied this directory to the sandbox (eg. if we're
    // building debug libs after building release libs)
    if (!Files.exists(sandbox(dir.path))) {
      if (Files.isSymbolicLink(dir.path)) {
        copySymLink(dir.path)
      } else {
        Files.walkFileTree(
          dir.path,
          new ErrorIgnoringFileVisitor() {
            override def preVisitDirectory(d: Path, basicFileAttributes: BasicFileAttributes): FileVisitResult = {
              if (Files.isSymbolicLink(d)) {
                copySymLink(d)
              } else {
                Files.createDirectories(sandbox(d))
              }
              FileVisitResult.CONTINUE
            }

            override def visitFile(f: Path, attrs: BasicFileAttributes): FileVisitResult = {
              if (Files.isReadable(f)) {
                if (Files.isSymbolicLink(f)) {
                  copySymLink(f)
                } else {
                  val sourceFile = FileAsset(f)
                  val targetFile = FileAsset(sandbox(f))
                  AssetUtils.copy(sourceFile, targetFile)
                }
              }
              FileVisitResult.CONTINUE
            }
          }
        )
      }
    }
  }
}
