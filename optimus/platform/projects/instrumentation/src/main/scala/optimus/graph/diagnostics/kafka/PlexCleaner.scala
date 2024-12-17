package optimus.graph.diagnostics.kafka
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

import optimus.graph.diagnostics.kafka.CrumbPlexerUtils.partsSuffix
import optimus.platform.util.Log
import org.apache.commons.io.FileUtils
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.{Option => ArgOption}

import java.io.IOException
import java.nio.file.FileVisitResult
import java.nio.file.FileVisitor
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.attribute.BasicFileAttributes
import java.time.Instant
import scala.collection.mutable

class PlexCleanerArgs {
  @ArgOption(name = "--plexdir")
  val dirName = "crumbs"

  @ArgOption(name = "--trimpct", usage = "Trim oldest percent")
  val pct = 20

  @ArgOption(name = "--minGB")
  val minGB = 500

  @ArgOption(name = "--minAgeMinutes")
  val minAgeMn = 5000

  @ArgOption(name = "--sleepSec")
  val sleep = 3600

  @ArgOption(name = "--dry-run")
  val dryRun = false
}

object PlexCleaner extends App with Log {

  val plexCleanerArgs = new PlexCleanerArgs
  val parser = new CmdLineParser(plexCleanerArgs)
  try {
    parser.parseArgument(args: _*)
  } catch {
    case e: CmdLineException =>
      parser.printUsage(System.err)
      System.exit(-1)
  }

  import plexCleanerArgs._
  private val plexDir = Paths.get(dirName)

  val store = Files.getFileStore(plexDir)

  while (true) {

    val avail = store.getUsableSpace / (1024L * 1024L * 1024L)
    if (avail > minGB) {
      log.info(s"$avail GB available > $minGB.  Sleeping for $sleep sec")
      Thread.sleep(sleep * 1000L)
    } else {
      log.info(s"$avail GB available < $minGB.  Generating listing of $plexDir")
      val leaves = mutable.ArrayBuffer.empty[(Path, Long)]
      // Walk tree searching for either a parts directory, or some other leaf file.  Add these to collection of leaves,
      // along with their modification time.
      var nDirs = 0
      var nFiles = 0
      def logWalking(p: Path): Unit = {
        if ((nDirs + nFiles) % 10000 == 0)
          log.info(
            s"Walked $nDirs directories, $nFiles files so far; most recently $p ${Files.getLastModifiedTime(p)} ")
      }

      Files.walkFileTree(
        plexDir,
        new FileVisitor[Path] {
          override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult = {
            // If it's a .parts directory, it's effectively a leaf, and we'll either keep it or delete it
            // with its contents.
            if (dir.getFileName.toString.endsWith(partsSuffix)) {
              nDirs += 1
              logWalking(dir)
              leaves += dir -> Files.getLastModifiedTime(dir).toMillis
              FileVisitResult.SKIP_SUBTREE
            } else FileVisitResult.CONTINUE
          }
          override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
            nFiles += 1
            logWalking(file)
            leaves += plexDir -> Files.getLastModifiedTime(plexDir).toMillis
            FileVisitResult.CONTINUE
          }
          override def visitFileFailed(file: Path, exc: IOException): FileVisitResult = FileVisitResult.CONTINUE
          override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = FileVisitResult.CONTINUE
        }
      )
      log.info(s"Found ${leaves.size} files; sorting...")
      val listing = leaves.sortBy(_._2)
      log.info(s"will consider oldest $pct percent for deletion")
      var deleted = 0
      var tooYoung = 0
      val earliestToDelete = System.currentTimeMillis() - minAgeMn * 60L * 1000L

      listing.take(listing.size * pct / 100).foreach { case (entry, t) =>
        if (t > earliestToDelete) {
          tooYoung += 1
          if (dryRun)
            log.info(s"Not deleting young $entry from ${Instant.ofEpochMilli(t)}")
        } else {
          deleted += 1
          if (dryRun) {
            log.info(s"Pretending to delete $entry from ${Instant.ofEpochMilli(t)}")
          } else {
            if (!FileUtils.deleteQuietly(entry.toFile))
              log.warn(s"Failed to delete $entry")
          }
        }
        if (((deleted + tooYoung) % 10000) == 0)
          log.info(s"So far deleted $deleted entries so far, pardoned $tooYoung, most recent entry $entry from ${Instant
              .ofEpochMilli(t)}")
      }

      log.info(s"Deleted $deleted entries so far, pardoned $tooYoung")

      if (!dryRun) {
        deleted = 0
        log.info(s"Clearing now empty subdirectories")
        Files.walkFileTree(
          plexDir,
          new FileVisitor[Path] {
            var occupied = false // if true, don't delete
            override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult = {
              if (dir.getFileName.toString.endsWith(partsSuffix)) {
                // This parts directory apparently wasn't deleted, so obviously we keep its path to root
                occupied = true
                FileVisitResult.SKIP_SUBTREE
              } else {
                // This subdirectory may or may not be empty.  We don't know yet.
                occupied = false
                FileVisitResult.CONTINUE
              }
            }
            override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
              occupied = true
              FileVisitResult.SKIP_SIBLINGS
            }
            override def visitFileFailed(file: Path, exc: IOException): FileVisitResult = FileVisitResult.CONTINUE
            override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
              if (!occupied && dir != plexDir) {
                Files.delete(dir)
                deleted += 1
                if ((deleted % 5000) == 0)
                  log.info(s"Deleted $deleted empty subdirectories")
              }
              FileVisitResult.CONTINUE
            }
          }
        )
      }
    }
  }
}
