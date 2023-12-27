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
package optimus.buildtool.oci

import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap

import optimus.buildtool.files.Directory
import optimus.buildtool.utils.PathUtils
import optimus.platform.util.Log

/* It Works on Unix only for now.
 * It uses the unix command 'strip' to remove debug symbols from dependencies
 * (which makes our docker image significantly smaller) */
class DependencyStripper(dir: Directory) extends Log {
  private val strip = "/usr/bin/strip" // just in case is not in $PATH

  private val stripCache = new ConcurrentHashMap[Path, Option[Path]]()
  def copyWithoutDebugSymbols(path: Path): Option[Path] =
    stripCache.computeIfAbsent(
      path,
      { _ =>
        if (canBeStripped(path)) {
          // Dropping AFS root reuse the remaining for our stripped copy
          val relPath = PathUtils.platformIndependentString(path).split("/").drop(4).mkString("/")
          val reduced: Path = dir.path.resolve(relPath)
          // strip assumes the directory exists, fails otherwise
          Files.createDirectories(reduced.getParent)
          val exitCode = runStripDebugCmd(reduced, path)
          if (exitCode != 0) None // reduction failed
          else Some(reduced)
        } else None
      }
    )

  protected def runStripDebugCmd(reduce: Path, path: Path): Int = {
    import scala.sys.process._
    val processLogger = ProcessLogger(msg => log.debug(s"[strip:${PathUtils.platformIndependentString(path)}] $msg"))
    Process(s"$strip --strip-debug -o ${PathUtils.platformIndependentString(reduce)} $path") ! processLogger
  }

  private def canBeStripped(path: Path): Boolean =
    PathUtils.isDisted(PathUtils.platformIndependentString(path))
}
