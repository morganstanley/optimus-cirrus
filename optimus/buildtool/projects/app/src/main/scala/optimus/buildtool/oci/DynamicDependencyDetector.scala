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
import java.nio.file.Paths
import java.util.concurrent.ConcurrentHashMap

import optimus.buildtool.utils.PathUtils
import optimus.platform.util.Log

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

/* It Works on Unix only for now.
 * It uses the unix command 'ldd' to try and detect all AFS dynamic dependencies recursively */
object DynamicDependencyDetector extends DynamicDependencyDetector
trait DynamicDependencyDetector extends Log {
  private val ldd = "/usr/bin/ldd" // just in case is not in $PATH

  def getDynamicDependencies(path: Path, excludeF: Path => Boolean): Set[Path] = {

    @tailrec
    def loop(toVisit: Seq[Path], visited: Set[Path]): Set[Path] = toVisit match {
      case Nil =>
        log.debug(s"Found a total of ${visited.size} MS dynamic dependencies for path $path")
        visited
      case head :: tail =>
        val newDeps = (lddCommand(head).filterNot(excludeF) -- visited)
        loop(tail ++ newDeps, visited ++ newDeps)
    }

    if (shouldBeAnalyzed(path)) loop(toVisit = Seq(path), visited = Set.empty)
    else Set.empty
  }

  // This is a best effort exclusion list for performance reasons
  // this shouldn't really change, but if so we should consider make this configurable
  private val executableToIgnore: Set[String] = Set(
    "bas",
    "cat",
    "cfg",
    "cls",
    "csv",
    "dat",
    "dbg",
    "h",
    "jar",
    "ksh",
    "la",
    "lua",
    "o",
    "pl",
    "pm",
    "policy",
    "proto",
    "py",
    "q",
    "sh",
    "sql",
    "txt",
    "vbs",
    "x42dio",
    "xls",
    "xlsm",
    "xml",
    "xsd",
    "xsl",
    "yml"
  )

  private def shouldBeAnalyzed(path: Path): Boolean = {
    def extension = path.getFileName.toString.split("\\.").last.toLowerCase
    isExecutable(path) && !executableToIgnore.contains(extension)
  }

  protected def isExecutable(path: Path): Boolean = Files.isExecutable(path)

  private val lddCache = new ConcurrentHashMap[Path, Set[Path]]()
  private def lddCommand(path: Path): Set[Path] =
    lddCache.computeIfAbsent(
      path,
      { _ =>
        runLddCmd(path).flatMap { line =>
          line.split("\\s+").collect {
            case token if isMsDistPath(token) => Paths.get(token)
          }
        }.toSet
      })

  private def isMsDistPath(pathString: String): Boolean =
    PathUtils.isDisted(PathUtils.platformIndependentString(pathString))

  protected def runLddCmd(path: Path): Seq[String] = {
    import scala.sys.process._
    val lines = new ArrayBuffer[String]
    // no need to parse std err when looking for AFS paths...
    Process(s"$ldd $path") ! ProcessLogger(line => lines.append(line), _ => () /* ignoring stderr */ )
    lines
  }
}
