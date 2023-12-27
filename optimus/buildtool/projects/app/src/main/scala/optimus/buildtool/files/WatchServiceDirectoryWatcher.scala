/*
 * Copyright (c) 2008, 2010, Oracle and/or its affiliates. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *   - Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *
 *   - Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *
 *   - Neither the name of Oracle nor the names of its
 *     contributors may be used to endorse or promote products derived
 *     from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
/* This file borrows heavily from  https://docs.oracle.com/javase/tutorial/essential/io/examples/WatchDir.java which
 * is covered by the above license. Modifications were made by Morgan Stanley to write it into Scala and omit support
 * for certain parameters (maxDepth and dirFilter).
 *
 * For those modifications only:
 *
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

import java.nio.file.AccessDeniedException
import java.nio.file.ClosedWatchServiceException
import java.nio.file.FileVisitOption
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.StandardWatchEventKinds._
import java.nio.file.WatchEvent
import java.nio.file.WatchKey
import java.nio.file.WatchService
import java.nio.file.attribute.BasicFileAttributes
import java.util

import msjava.slf4jutils.scalalog.Logger
import msjava.slf4jutils.scalalog.getLogger
import optimus.buildtool.files.Directory.PathFilter
import optimus.buildtool.files.Directory.PredicateFilter
import optimus.utils.ErrorIgnoringFileVisitor

import scala.jdk.CollectionConverters._
import scala.collection.mutable
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
 * A DirectoryWatcher that uses a java.nio.WatchService to watches for any file changes under rootPath directory and any
 * sub-directories thereof.
 *
 * The main benefit is that calls to #getModifiedPathsAndReset() are almost instantaneous. The main disadvantage is that
 * there is some lag between filesystem changes and WatchService notifications, so #getModifiedPathsAndReset() may be
 * slightly stale (perhaps 10-100ms based on testing on Windows 7).
 *
 * Borrows heavily from https://docs.oracle.com/javase/tutorial/essential/io/examples/WatchDir.java
 *
 * Currently ignores maxDepth parameter (always watches to maximum depth) and dirFilter (visits all directories)
 */
private[buildtool] class WatchServiceDirectoryWatcher(watchService: WatchService, listener: Option[() => Unit] = None)
    extends DirectoryWatcher {
  import WatchServiceDirectoryWatcher._

  // all of the below state is guarded by StateLock
  private[this] object StateLock
  private[this] val watchInfoByWatchedDir: mutable.HashMap[Path, WatchInfo] = mutable.HashMap()
  @volatile private[this] var lifecycleState: LifecycleState = Starting
  private[this] val modifiedDirs: mutable.Set[Path] = mutable.HashSet[Path]()
  private[this] var failure: Option[Throwable] = None

  def registerPath(rootPath: Path, notificationFilter: PathFilter, dirFilter: PathFilter, maxDepth: Int): Unit =
    StateLock.synchronized {
      watchInfoByWatchedDir.get(rootPath) match {
        case None if maxDepth == 0 => // special handling for existence tests on rootPath itself
          registerAllDirs(
            rootPath,
            rootPath.getParent,
            notificationFilter && PredicateFilter(_ == rootPath),
            dirFilter,
            maxDepth,
            maxDepth + 1
          )
        case None =>
          registerAllDirs(rootPath, rootPath, notificationFilter, dirFilter, maxDepth, maxDepth)
        case Some(info) =>
          // it's not illegal from the point of view of the NIO WatchService, but it would mess up our bookkeeping, so
          // since we don't need to support it, we'll ban it
          require(
            info.rootDir == rootPath,
            s"Illegal attempt to register watch on path $rootPath " +
              s"which was already being watched under path  ${info.rootDir}")
      }
    }

  override def close(): Unit = {
    StateLock.synchronized {
      if (lifecycleState == Running || lifecycleState == Starting) {
        lifecycleState = Stopping
        watchInfoByWatchedDir.values.foreach { d =>
          try d.key.cancel()
          catch {
            case e: RuntimeException =>
              // JimFS watch service seems to transiently NPE when cancelling keys
              log.warn(s"Exception when cancelling watch key for $d", e)
          }
        }
        watchInfoByWatchedDir.clear()
        watchService.close()
      }
    }
    while (watchThread.isAlive) {
      watchThread.join(5000)
      if (watchThread.isAlive)
        log.warn(s"Waiting for ${watchThread.getName} thread to exit...")
    }
  }

  override def getModifiedPathsAndReset(): Set[Path] = StateLock.synchronized {
    // if there was a failure on the background thread, we throw that (forever, because we don't know how bad a state
    // we are in)
    failure.foreach(throw _)

    val result = modifiedDirs.toSet
    modifiedDirs.clear()
    result
  }

  override def watchedPaths: Set[Path] = StateLock.synchronized { watchInfoByWatchedDir.keySet.toSet }

  override def scannedFiles: Int = 0

  private val watchThread = new Thread("DirectoryWatcher") {
    override def run(): Unit = {
      StateLock.synchronized { lifecycleState = Running }
      try {
        // noinspection LoopVariableNotUpdated
        while (lifecycleState == Running) pollAndProcessEvents()
      } catch {
        case t: Throwable =>
          log.error("DirectoryWatcher thread killed by exception", t)
      } finally {
        StateLock.synchronized { lifecycleState = Stopped }
      }
    }
  }
  watchThread.start()

  private def registerAllDirs(
      rootPath: Path,
      start: Path,
      notificationFilter: PathFilter,
      dirFilter: PathFilter,
      maxDepth: Int,
      remainingDepth: Int
  ): Unit = StateLock.synchronized {
    if (Files.exists(start))
      Files.walkFileTree(
        start,
        util.EnumSet.noneOf(classOf[FileVisitOption]),
        remainingDepth,
        new ErrorIgnoringFileVisitor() {
          override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult =
            if (dirFilter(dir, attrs)) {
              try {
                watchInfoByWatchedDir.get(dir) match {
                  case Some(info) =>
                    require(
                      rootPath == info.rootDir,
                      s"Attempted to register watch for dir $dir under root $rootPath " +
                        s"but was already watched under root ${info.rootDir}")
                  case None =>
                    val key = dir.register(watchService, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY, OVERFLOW)
                    val info =
                      WatchInfo(key, rootDir = rootPath, watchDir = dir, notificationFilter, dirFilter, maxDepth)
                    watchInfoByWatchedDir.put(dir, info)
                }
              } catch {
                case _: AccessDeniedException | _: NoSuchFileException =>
                  log.warn(s"Tried to watch $dir but it was not accessible (already deleted?)")
              }
              FileVisitResult.CONTINUE
            } else FileVisitResult.SKIP_SUBTREE
        }
      )
  }

  private def pollAndProcessEvents(): Unit = {
    pollEvents() match {
      case Success((info, events)) =>
        val filesChanged = processEvents(info, events)
        if (filesChanged) listener.foreach(_())
      case Failure(e: InterruptedException) =>
        log.error("Watch service interrupted", e) // watchService.take() interrupted
        failure = Some(e)
      case Failure(_: ClosedWatchServiceException) =>
        // clear the running flag (just in case we got closed other than by stopWatching())
        StateLock.synchronized { lifecycleState = Stopping }
        log.info("Watch service closed")
      case Failure(e: NoSuchElementException) =>
        log.error("Watch service triggered on unmonitored directory", e)
        failure = Some(e) // WatchKey triggered for un-registered directory
      case Failure(e) =>
        throw e
    }
  }

  private def pollEvents(): Try[(WatchInfo, Seq[WatchEvent[Path]])] = {
    Try {
      val watchKey = watchService.take()
      StateLock.synchronized {
        watchInfoByWatchedDir(watchKey.watchable().asInstanceOf[Path])
      }
    }.map { info =>
      info -> info.key.pollEvents().asScala.map(_.asInstanceOf[WatchEvent[Path]])
    }
  }

  private def processEvents(info: WatchInfo, events: Seq[WatchEvent[Path]]): Boolean = {
    val changes = events.map { event =>
      if (event.kind == OVERFLOW) {
        log.warn(s"OVERFLOW event received for ${info.watchDir}. Re-scanning filesystem.")
        checkForExistingFiles(info, info.watchDir)
      } else {
        val changedPath = info.watchDir.resolve(event.context)
        var filesChanged = false

        if (shouldTriggerEvent(info, event.kind, changedPath)) {
          StateLock.synchronized(modifiedDirs.add(info.rootDir))
          log.trace(s"File Change Event: Root(${info.rootDir}) Kind(${event.kind}) ChangedPath($changedPath)")
          filesChanged = true
        }

        if (Files.isDirectory(changedPath) && event.kind == ENTRY_CREATE) {
          val remainingDepth = info.maxDepth - info.rootDir.relativize(changedPath).normalize.getNameCount
          if (remainingDepth > 0)
            registerAllDirs(
              info.rootDir,
              changedPath,
              info.notificationFilter,
              info.dirFilter,
              info.maxDepth,
              remainingDepth
            )
          // Need this check because file may have been created under new dir before we register as a watcher on it
          filesChanged = filesChanged || checkForExistingFiles(info, changedPath)
        }
        filesChanged
      }
    }

    if (!info.key.reset()) StateLock.synchronized {
      log.info(s"Watch no longer valid for ${info.watchDir} (directory removed?)")
      watchInfoByWatchedDir.remove(info.watchDir)
    }

    changes.contains(true)
  }

  private def shouldTriggerEvent(info: WatchInfo, eventKind: WatchEvent.Kind[Path], path: Path): Boolean =
    try info.notificationFilter(path, Files.readAttributes(path, classOf[BasicFileAttributes]))
    catch {
      case _: NoSuchFileException | _: AccessDeniedException => true
    }

  private def checkForExistingFiles(info: WatchInfo, changedPath: Path): Boolean = {
    var filesChanged = false
    Files.walkFileTree(
      changedPath,
      new ErrorIgnoringFileVisitor {
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          if (file != info.watchDir && info.notificationFilter(file, attrs)) {
            StateLock.synchronized(modifiedDirs.add(info.rootDir))
            log.trace(s"Caught missed file creation event: $file")
            filesChanged = true
          }
          FileVisitResult.CONTINUE
        }
      }
    )
    filesChanged
  }
}

object WatchServiceDirectoryWatcher {
  private val log: Logger = getLogger(getClass)

  private final case class WatchInfo(
      key: WatchKey,
      rootDir: Path,
      watchDir: Path,
      notificationFilter: PathFilter,
      dirFilter: PathFilter,
      maxDepth: Int
  )

  private sealed trait LifecycleState
  private case object Starting extends LifecycleState
  private case object Running extends LifecycleState
  private case object Stopping extends LifecycleState
  private case object Stopped extends LifecycleState
}
