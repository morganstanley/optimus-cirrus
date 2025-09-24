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

import msjava.zkapi.ZkaAttr
import msjava.zkapi.ZkaConfig
import msjava.zkapi.internal.ZkaContext
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.crumbs.{Properties => P}
import optimus.graph.diagnostics.sampling.BaseSamplers
import optimus.graph.diagnostics.sampling.SamplingProfiler
import optimus.platform.util.Log
import org.apache.commons.io
import optimus.utils.FileUtils
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.{Option => ArgOption}

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.attribute.FileTime
import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Success
import scala.util.control.NonFatal
import scala.util.Try

class PlexCleanerArgs {
  @ArgOption(name = "--plexdir")
  val dirName = "crumbs"

  @ArgOption(name = "--trimpct", usage = "Trim oldest percent")
  val pct = 10

  @ArgOption(name = "--minGB", usage = "Maintain this many GB minimum")
  val minGB = 500

  @ArgOption(name = "--minAgeMinutes", usage = "Don't delete if younger than ")
  val minAgeMn = 5000

  @ArgOption(name = "--zipAgeMinutes")
  val zipAgeMn = -1

  @ArgOption(name = "--zstd")
  val useZstd = false

  @ArgOption(name = "--sleepSec")
  val sleep = 3600

  @ArgOption(name = "--dry-run")
  val dryRun = false

  @ArgOption(name = "--threads")
  val nThreads = 5

  @ArgOption(name = "--logThreshold")
  val logThresh = 5000

  @ArgOption(
    name = "--publish",
    usage =
      "Crumbplexer's own publication config, in form \"zkenv/node\", where zkenv is qa or prod, and node is the starting point under optimus/breadcrumbs"
  )
  val crumbPublication = ""
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

  private val publishing =
    if (crumbPublication.contains("/")) {
      val Array(env, configNode) = crumbPublication.split("/")
      Breadcrumbs.customizedInit(
        Map("breadcrumb.config" -> configNode),
        new ZkaContext(ZkaConfig.fromURI(s"zpm://$env.na/optimus").attr(ZkaAttr.KERBEROS, false))
      )
      SamplingProfiler.applicationSetup("CrumbPlexer")
      true
    } else false

  val store = Files.getFileStore(plexDir)

  // Run f on multiple threads
  private def crunch[T](f: => Unit): Unit = {
    val threads = (1 to nThreads).map { _ =>
      new Thread {
        override def run(): Unit = f
      }
    }
    threads.foreach(t => t.setName("Delete-" + t.getName))
    threads.foreach(_.start())
    threads.foreach(_.join())
    assert(threads.forall(t => !t.isAlive), s"Deletion threads still running! $threads")
  }

  private implicit class Syncherator[T](val it: Iterator[T]) extends AnyVal {
    def safeNext(): Option[T] = it.synchronized {
      if (it.hasNext)
        Some(it.next())
      else None
    }
  }

  // Pull items safely off iterator to process, adding zero or more results to the output queue
  private def harvest[T, U](it: Iterator[T])(f: (T, ConcurrentLinkedQueue[U]) => Unit): Iterable[U] = {
    val qout = new ConcurrentLinkedQueue[U]()
    crunch {
      @tailrec def loop(): Unit = {
        it.safeNext() match {
          case Some(e) =>
            try {
              f(e, qout)
            } catch {
              case NonFatal(t) =>
                log.warn(s"Skipping $e, $t")
            }
            loop()
          case None =>
        }
      }
      loop()
    }
    qout.asScala
  }

  // Pull items safely off iterator to process, adding zero or one results to output queue
  private def process[T, U](it: Iterator[T])(f: T => Option[U]): Iterable[U] =
    harvest(it)((t: T, q: ConcurrentLinkedQueue[U]) => f(t).foreach(q.add))

  // Main loop
  while (true) {

    // Assemble top level files names and their modification times, possibly compressing the file
    if (zipAgeMn > 0)
      log.info(s"Zipping files older than $zipAgeMn minutes")
    val fileAndTimes: Iterable[(String, Long)] = {
      val in = Files.walk(plexDir).iterator.asScala
      val t0 = System.currentTimeMillis()
      val tLatestToZip = t0 - zipAgeMn * 60L * 1000L
      val nf = new AtomicInteger()
      val nz = new AtomicInteger()
      val totBytesUnzipped = new AtomicLong(0)
      val totBytesZipped = new AtomicLong(0)
      val ret: Iterable[(String, Long)] = process[Path, (String, Long)](in) { p: Path =>
        if (Files.isRegularFile(p) && Files.isReadable(p)) {
          nf.incrementAndGet()
          val name = p.toString
          val t = Files.getLastModifiedTime(p).toMillis
          if (
            name.endsWith(CrumbPlexerUtils.zipSuffix)
            || name.endsWith(CrumbPlexerUtils.zstdSuffix)
            || name.endsWith(CrumbPlexerUtils.sizeSuffix)
            || (zipAgeMn < 0) || t > tLatestToZip
          )
            Some((name, t)) // not zippable; output original file
          else {
            val p1 = Paths.get(name)
            val name2 = name + (if (useZstd) CrumbPlexerUtils.zstdSuffix else CrumbPlexerUtils.zipSuffix)
            val p2 = Paths.get(name2)
            val compress =
              if (useZstd) FileUtils.zstdCompress(p1, p2, removeOld = true, compressionLevel = 10)
              else FileUtils.gzip(p1, p2, removeOld = true)
            compress match {
              case Failure(e) =>
                log.warn(s"Unable to compress $p1: $e")
                // Not clear what the failure mode was, but try to delete both original and zipped now.
                Try(Files.delete(p1)) recover (e => log.warn(s"Unable to delete $p1: $e"))
                Try(Files.delete(p2)) recover (e => log.warn(s"Unable to delete $p2: $e"))
                None
              case Success((orig, zipped)) =>
                // Set modification time to that of uncompressed file, to track its age for deletion
                Try(Files.setLastModifiedTime(p2, FileTime.fromMillis(t))) recover (e =>
                  log.warn(s"Unable to adjust modification time of $p2: $e"))
                val sizename = name + CrumbPlexerUtils.sizeSuffix
                val sizepath = Paths.get(sizename)
                Try {
                  Files.writeString(sizepath, orig.toString)
                  Files.setLastModifiedTime(sizepath, FileTime.fromMillis(t))
                } recover (e => log.warn(s"Unable to write size file $sizepath"))
                val i = nz.incrementAndGet()
                totBytesUnzipped.addAndGet(orig)
                totBytesZipped.addAndGet(zipped)
                if (i % logThresh == 0)
                  log.info(
                    s"Zipped $i files, $totBytesUnzipped -> $totBytesZipped (${totBytesZipped.get.toDouble / (totBytesUnzipped.get + 1)}) so far in ${System
                        .currentTimeMillis() - t0}ms")
                // Retain original modification time!
                Some((name2, t))
            }
          }
        } else None
      }
      log.info(
        s"Zipped $nz files of $nf, $totBytesUnzipped -> $totBytesZipped (${totBytesZipped.get.toDouble / (totBytesUnzipped.get + 1)}) bytes total in ${System
            .currentTimeMillis() - t0}ms")
      ret
    }

    val avail = store.getUsableSpace / (1024L * 1024L * 1024L)
    if (publishing)
      BaseSamplers.setGauge(P.plexerSnapFreeDisk, avail * 1024)
    if (avail > minGB) {
      val t = System.currentTimeMillis()
      val (oldestFile, earliest) = fileAndTimes.toSeq.minByOption(_._2).getOrElse("NA" -> t)
      log.info(
        s"$avail GB available > $minGB.  Earliest file $oldestFile ${Instant.ofEpochMilli(earliest)}  Sleeping for $sleep sec")
      if (publishing)
        BaseSamplers.setGauge(P.plexerSnapOldestHours, (t - earliest) / (3600 * 1000L))
      Thread.sleep(sleep * 1000L)
    } else {
      log.info(s"$avail GB available < $minGB.  Generating listing of $plexDir")
      // Walk tree searching for either a parts directory, or some other leaf file.  Add these to collection of leaves,
      // along with their modification time.
      val nextLog = new AtomicInteger(0)

      val leaves: Iterable[(String, Long)] = fileAndTimes

      log.info(s"Dealing with ${leaves.size} leaves")

      // Sort the collected .parts and file entries by time, and delete the oldest pct
      if (leaves.size > 0) {
        val deleted = new AtomicInteger(0)
        log.info(s"Found ${leaves.size} files; sorting...")
        val earliestToDelete = System.currentTimeMillis() - minAgeMn * 60L * 1000L

        val (toDeleteSeq: Iterable[String], earliest: Long) = {
          // Sort by timestamp, earliest first
          val sorted = leaves.toSeq.sortBy(_._2)
          // Find candidates for deletion
          val toDelete = sorted.take(leaves.size * pct / 100).takeWhile(_._2 < earliestToDelete)
          // We'll spare the first item in toDelete, so we can report it as the oldest
          if (toDelete.size <= 1)
            (Iterator.empty, sorted.head._2)
          else
            (toDelete.map(_._1).drop(1), toDelete.head._2)
        }
        nextLog.set(0)
        // formatting contributed by a machine
        log.info(s"Of top $pct percent, found ${toDeleteSeq.size} older than ${Instant.ofEpochMilli(
            earliestToDelete)}, oldest remaining ${Instant.ofEpochMilli(earliest)}")
        if (toDeleteSeq.nonEmpty)
          harvest[String, Unit](toDeleteSeq.iterator.drop(1)) { case (entry, _) =>
            val p = Paths.get(entry)
            try {
              if (Files.isDirectory(p))
                io.FileUtils.deleteDirectory(p.toFile)
              else
                Files.delete(p)
              val d = deleted.incrementAndGet()
              if (d > nextLog.get) {
                nextLog.addAndGet(logThresh)
                log.info(s"Deleted $d ...")
              }
            } catch {
              case NonFatal(e) =>
                log.warn(s"Unable to to delete $entry $p: $e")
            }
          }
        log.info(s"Deleted $deleted total.")
        if (publishing)
          BaseSamplers.setGauge(P.plexerSnapOldestHours, (System.currentTimeMillis() - earliest) / (3600 * 1000L))
      }
      if (publishing)
        BaseSamplers.setGauge(P.plexerSnapOldestHours, 0L)
      log.info(s"Cleaning cycle complete; sleeping for 10 seconds.")
      Thread.sleep(10 * 1000L)
    }
  }
}
