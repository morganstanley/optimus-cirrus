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

import java.nio.file.FileVisitOption
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger
import com.google.common.hash.HashCode
import com.google.common.hash.Hasher
import optimus.buildtool.files.Directory.PathFilter
import optimus.buildtool.trace.ObtStats
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.Hashing
import optimus.buildtool.utils.PathUtils
import optimus.platform.util.Log
import optimus.platform._
import optimus.utils.ErrorIgnoringFileVisitor

import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * A directory watcher which recursively scans the registered directories on each call to #getModifiedPathsAndReset().
 *
 * This isn't as slow as you might expect - all of codetree takes less than a second on a good machine. Because it
 * rescans every time, you are guaranteed to see any changes immediately, unlike with WatchService
 */
private[buildtool] class ScanningDirectoryWatcher extends DirectoryWatcher with Log {
  import ScanningDirectoryWatcher._

  private val scanners: mutable.Set[Scanner] = ConcurrentHashMap.newKeySet[Scanner]().asScala

  override def registerPath(
      rootPath: Path,
      notificationFilter: PathFilter,
      dirFilter: PathFilter,
      maxDepth: Int): Unit = if (!PathUtils.isDisted(rootPath)) { // assume disted paths are immutable
    val scanner = Scanner(rootPath, dirFilter, notificationFilter, maxDepth)
    if (scanners.add(scanner)) {
      log.debug(s"Registering path: $rootPath")
      scanner.hasChanged()
    }
  }

  override def close(): Unit = scanners.clear()

  override def getModifiedPathsAndReset(): Set[Path] = rescan()

  override def watchedPaths: Set[Path] = scanners.map(_.path).toSet

  override def scannedFiles: Int = scanners.map(_.scanned).sum

  @entersGraph private def rescan(): Set[Path] = {
    val currentScanners = scanners.toSet
    val dirs = currentScanners.apar.filter(_.hasChanged()).map(_.path)
    ObtTrace.setStat(ObtStats.ScannedDirectories, currentScanners.size)
    ObtTrace.setStat(ObtStats.ModifiedDirectories, dirs.size)
    dirs
  }
}

object ScanningDirectoryWatcher {
  final case class Scanner(path: Path, dirFilter: PathFilter, filter: PathFilter, maxDepth: Int) {
    private var lastHash: HashCode = _
    private val files = new AtomicInteger(0)

    private class Visitor(val hasher: Hasher = Hashing.hashFunction.newHasher()) extends ErrorIgnoringFileVisitor {
      override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult = {
        if (dirFilter(dir, attrs)) visitFile(dir, attrs)
        else FileVisitResult.SKIP_SUBTREE
      }

      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
        // we hash every matching file/directory along with its last modified time; it's not enough to simply find
        // the maximum last modified time because file deletions won't necessarily change that
        if (filter(file, attrs)) {
          hasher.putInt(file.hashCode()) // Path requires a non-default hashCode implementation
          if (!attrs.isDirectory) {
            // Don't bother with mtime for directories because any changes we care about will be picked up by files
            // Still hash the directory name because sometimes we care if it exists (e.g. for checking for sparsity)
            hasher.putLong(attrs.lastModifiedTime().toMillis)
          }
          files.getAndIncrement()
        }
        FileVisitResult.CONTINUE
      }
    }

    private def scan(): HashCode = {
      val v = new Visitor
      files.set(0)
      Files.walkFileTree(path, Collections.emptySet[FileVisitOption], maxDepth, v)
      v.hasher.hash()
    }

    // noinspection AccessorLikeMethodIsEmptyParen
    def hasChanged(): Boolean = {
      val newHash = scan()
      val changed = lastHash != newHash
      lastHash = newHash
      changed
    }

    def scanned: Int = files.get
  }
}
