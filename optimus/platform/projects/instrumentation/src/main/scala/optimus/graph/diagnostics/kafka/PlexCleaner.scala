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
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

class PlexCleanerArgs {
  @ArgOption(name = "--plexdir")
  val dirName = "crumbs"

  @ArgOption(name = "--trimpct", usage = "Trim oldest percent")
  val pct = 10

  @ArgOption(name = "--minGB")
  val minGB = 500

  @ArgOption(name = "--minAgeMinutes")
  val minAgeMn = 5000

  @ArgOption(name = "--sleepSec")
  val sleep = 3600

  @ArgOption(name = "--dry-run")
  val dryRun = false

  @ArgOption(name = "--threads")
  val nThreads = 10

  @ArgOption(name = "--logThreshold")
  val logThresh = 50000
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
      // Walk tree searching for either a parts directory, or some other leaf file.  Add these to collection of leaves,
      // along with their modification time.
      val nDirs = new AtomicInteger(0)
      val nFiles = new AtomicInteger(0)
      val thresh = new AtomicInteger(0)

      // Divide up top level directories into nThreads groups.
      val dirss: Seq[Seq[Path]] = {
        val ds = Files.list(plexDir).iterator().asScala.filter(Files.isDirectory(_)).toSeq
        ds.grouped(Math.max(1, ds.size / nThreads)).toSeq
      }
      // Each thread walks the directories in its group, collecting .parts directories tagged with modification time.
      // Keep paths as string to avoid accumulating Path internals
      // (Semicolon at the end of the next line prevents auto-format from putting the immediately following curly bracket
      // on the same line, then causing scala to believe that the block is supposed to be a CanBuildFrom.
      val leavess: Seq[ArrayBuffer[(String, Long)]] = dirss.map(_ => mutable.ArrayBuffer.empty[(String, Long)]);
      {
        def logWalking(p: Path): Unit = {
          if ((nDirs.get + nFiles.get) > thresh.get) {
            thresh.addAndGet(logThresh)
            log.info(
              s"Walked $nDirs parts directories, $nFiles files so far; most recently $p ${Files.getLastModifiedTime(p)} ")
          }
        }

        val walkThreads: Seq[Thread] = dirss.zip(leavess).map { case (dirs, leaves) =>
          new Thread {
            override def run() = {
              log.info(s"Walking ${dirs.size} directories in ${Thread.currentThread().getName}")
              dirs.foreach { d =>
                Files.walkFileTree(
                  d,
                  new FileVisitor[Path] {
                    override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult = {
                      // If it's a .parts directory, it's effectively a leaf, and we'll either keep it or delete it
                      // with its contents.
                      if (dir.getFileName.toString.endsWith(partsSuffix)) {
                        nDirs.incrementAndGet()
                        logWalking(dir)
                        leaves += dir.toString -> Files.getLastModifiedTime(dir).toMillis
                        FileVisitResult.SKIP_SUBTREE
                      } else FileVisitResult.CONTINUE
                    }
                    override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
                      nFiles.incrementAndGet()
                      logWalking(file)
                      leaves += file.toString -> Files.getLastModifiedTime(plexDir).toMillis
                      FileVisitResult.CONTINUE
                    }
                    override def visitFileFailed(file: Path, exc: IOException): FileVisitResult =
                      FileVisitResult.CONTINUE
                    override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult =
                      FileVisitResult.CONTINUE
                  }
                )
              }
            }
          }
        }
        walkThreads.foreach(t => t.setName("Walk-" + t.getName))
        walkThreads.foreach(_.start())
        walkThreads.foreach(_.join())

        assert(walkThreads.forall(t => !t.isAlive), s"Walking threads still running! $walkThreads")
        log.info(s"Walked total of $nDirs parts directories, $nFiles files")
      }

      // And add in any top level files (i.e. not in those subdirectories).
      val topLevelFiles: Seq[(String, Long)] =
        Files
          .list(plexDir)
          .iterator
          .asScala
          .filter(Files.isRegularFile(_))
          .map { f =>
            (f.toString, Files.getLastModifiedTime(f).toMillis)
          }
          .toSeq

      // Without the toList, this can for some reason throw in ArrayBuffer#apply
      val leaves: Seq[(String, Long)] = leavess.map(_.toList).flatten ++ topLevelFiles
      leavess.map(_.clear())

      // Sort the collected .parts and file entries by time, and delete the oldest pct
      if (leaves.size > 0) {
        val deleted = new AtomicInteger(0)
        val tooYoung = new AtomicInteger(0)

        log.info(s"Found ${leaves.size} files; sorting...")
        val sorted = leaves.sortBy(_._2)
        log.info(s"will consider oldest $pct percent for deletion")

        val cutoff = if (sorted.nonEmpty) sorted(sorted.size * pct / 100)._2 else 0L
        val earliestToDelete = System.currentTimeMillis() - minAgeMn * 60L * 1000L

        log.info(
          s"Cutoff ${Instant.ofEpochMilli(cutoff)}, but always preserve after ${Instant.ofEpochMilli(earliestToDelete)} ")

        val deleteGroups: Seq[Seq[(String, Long)]] = {
          val l = sorted.take(sorted.size * pct / 100)
          l.grouped(Math.max(1, l.size / nThreads)).toSeq // We do NOT want an Iterator!
        }
        thresh.set(0)
        val deleteThreads: Seq[Thread] = deleteGroups.map { entries =>
          new Thread {
            override def run(): Unit = {
              log.info(s"Starting deletion thread ${Thread.currentThread().getName} for ${entries.size} entries")
              entries.foreach { case (entry, t) =>
                if (t > earliestToDelete) {
                  tooYoung.incrementAndGet()
                  if (dryRun)
                    log.info(s"Not deleting young $entry from ${Instant.ofEpochMilli(t)}")
                } else {
                  if (dryRun) {
                    log.info(s"Pretending to delete $entry from ${Instant.ofEpochMilli(t)}")
                  } else {
                    val p = Paths.get(entry)
                    try {
                      FileUtils.deleteDirectory(p.toFile)
                      deleted.incrementAndGet()
                    } catch {
                      case NonFatal(e) =>
                        log.warn(s"Unable to to delete $entry $p", e)
                    }
                  }
                }
                if ((deleted.get() + tooYoung.get()) > thresh.get) {
                  thresh.addAndGet(logThresh)
                  log.info(s"Deleted $deleted entries so far, pardoned $tooYoung")
                }
              }
            }
          }
        }
        deleteThreads.foreach(t => t.setName("Delete-" + t.getName))
        deleteThreads.foreach(_.start())
        deleteThreads.foreach(_.join())
        assert(deleteThreads.forall(t => !t.isAlive), s"Deletion threads still running! $deleteThreads")
        log.info(s"Deleted $deleted entries altogether, pardoned $tooYoung")
      }

      log.info(s"Cleaning cycle complete.")

    }
  }
}
