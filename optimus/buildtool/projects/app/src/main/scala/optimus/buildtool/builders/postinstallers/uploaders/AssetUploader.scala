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

import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import optimus.buildtool.builders.BackgroundCmdId
import optimus.buildtool.builders.postinstallers.uploaders.ArchiveUtil.ArchiveEntry
import optimus.buildtool.builders.postinstallers.uploaders.AssetUploader._
import optimus.buildtool.builders.postinstallers.PostInstaller
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Asset
import optimus.buildtool.files.Directory
import optimus.buildtool.files.Directory.NotHiddenFilter
import optimus.buildtool.files.Directory.PredicateFilter
import optimus.buildtool.files.DirectoryAsset
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.RelativePath
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.BlockingQueue
import optimus.buildtool.utils.PathUtils
import optimus.buildtool.utils.TarUtils
import optimus.buildtool.utils.Utils
import optimus.platform._
import optimus.platform.util.Log

import scala.annotation.tailrec
import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.util.control.NonFatal
import scala.util.matching.Regex
import scala.jdk.CollectionConverters._

object AssetUploader {
  object UploadFormat {
    def apply(s: String): UploadFormat = s.toLowerCase match {
      case "raw" => Raw
      case "tar" => Tar
      case "zip" => Zip
    }
  }
  sealed trait UploadFormat
  case object Raw extends UploadFormat
  sealed trait ArchiveFormat extends UploadFormat
  case object Tar extends ArchiveFormat
  case object Zip extends ArchiveFormat
}

/**
 * Copies files in batches to different locations (NFS, remote server, or different directory)
 */
class AssetUploader(
    uploadDir: Directory,
    sourceDir: Directory,
    installDir: Directory,
    logDir: Directory,
    toolsDir: Option[Directory],
    locations: Seq[UploadLocation],
    maxConcurrentUploads: Int,
    minBatchSize: Int,
    maxBatchSize: Int,
    maxBatchBytes: Long,
    maxRetry: Int,
    srcPrefix: Option[RelativePath],
    srcFilesToUpload: Seq[Regex],
    uploadFormat: UploadFormat,
    useCrumbs: Boolean
) extends PostInstaller
    with Log {

  @async override def postInstallFiles(ids: Set[ScopeId], files: Seq[FileAsset], successful: Boolean): Unit =
    // No need to upload files if the build contains errors
    if (successful) readyToUpload(files.map(f => ArchiveEntry(f, installDir.relativize(f))))

  @async override def complete(successful: Boolean): Unit =
    // No need to upload the remaining files if the build was not successful
    if (successful) flush()

  private val sharedThrottle = new AdvancedUtils.Throttle(maxConcurrentUploads)

  protected object Tracker {
    // Note - we use Paths here so we're not comparing FileAsset timestamps
    private val _enqueuedFiles: mutable.Set[Path] = ConcurrentHashMap.newKeySet[Path]().asScala
    private val _filesToProcess: BlockingQueue[ArchiveEntry] = new BlockingQueue[ArchiveEntry]
    private val _processedFiles: mutable.Set[ArchiveEntry] = ConcurrentHashMap.newKeySet[ArchiveEntry]().asScala
    private val _uploadDuration: mutable.Set[(String, Long)] = ConcurrentHashMap.newKeySet[(String, Long)]().asScala

    def scheduleForUpload(files: Iterable[ArchiveEntry]): Unit = {
      // ignoring already enqueued files
      val filesToSchedule = files.filterNot(f => _enqueuedFiles.contains(f.file.path))
      enqueue(filesToSchedule)
    }

    def enqueue(files: Iterable[ArchiveEntry]): Unit =
      files.foreach { f =>
        _enqueuedFiles.add(f.file.path)
        _filesToProcess.put(f)
      }

    def pollForUpload(force: Boolean): Seq[ArchiveEntry] = {
      val selectedFiles = _filesToProcess.pollAll()
      if (force || selectedFiles.size >= minBatchSize) {
        // a bit of a lie here in the interest of performance: we register the selected files as already processed
        selectedFiles.foreach(f => _processedFiles.add(f))
        selectedFiles
      } else {
        // let's reschedule and wait to have a few more files
        enqueue(selectedFiles)
        Seq.empty
      }
    }

    def addUploadDuration(location: String, duration: Long) = _uploadDuration.add(location, duration)

    def processedFiles(): Set[ArchiveEntry] = _processedFiles.toSet
    def uploadedDuration(): Set[(String, Long)] = _uploadDuration.toSet

    def countProcessed: Int = _processedFiles.size
    def countToProcess: Int = _filesToProcess.size
  }

  private val uploaders = locations.zipWithIndex.map { case (location, idx) =>
    new LocationUploader(
      id = BackgroundCmdId(s"uploader-${idx + 1}"),
      location = location,
      throttle = sharedThrottle,
      maxRetry = maxRetry,
      logDir = logDir,
      toolsDir = toolsDir,
      useCrumbs = useCrumbs
    )
  }

  private val srcFileFilter = PredicateFilter { path =>
    val pathString = PathUtils.platformIndependentString(path)
    srcFilesToUpload.exists(_.pattern.matcher(pathString).matches)
  }

  /* files from src matching a given list of regexes */
  @async def scheduleUploadConfigFromSource(): Unit = if (uploaders.nonEmpty) {
    val nonHiddenSrcDirs = Directory(sourceDir.path, dirFilter = NotHiddenFilter)
    val files = Directory.findFiles(nonHiddenSrcDirs, srcFileFilter)
    val entries = files.map { f =>
      val targetPath = srcPrefix match {
        case Some(p) =>
          p.resolvePath(sourceDir.relativize(f))
        case None =>
          // We shouldn't write files with ".." path components, so walk up the file tree until we find a container
          // for the files (this is mostly just useful for ensuring "../src//*.obt" files are handled when manually
          // running uploads with standard codetree directory layout)
          @tailrec def relPath(container: Directory): RelativePath =
            if (container.contains(f)) container.relativize(f)
            else relPath(container.parent)

          relPath(installDir)
      }
      ArchiveEntry(f, targetPath)
    }
    readyToUpload(entries)
  }

  @async def readyToUpload(files: Seq[ArchiveEntry], force: Boolean = false): Unit = if (uploaders.nonEmpty) {
    Tracker.scheduleForUpload(files)
    upload(force)
  }

  private val INSTALLED_FILE_REGEX = s"[^/]+/[^/]+/[^/]+/install/.*".r // eg. optimus/platform/local/install/...
  @async def flush(): Unit = if (uploaders.nonEmpty) {
    // Searching the install folder for new installed files not yet uploaded
    // as we know that the background process may create some.
    val installedFiles = Directory.findFiles(
      installDir,
      PredicateFilter { path =>
        val pathString = PathUtils.platformIndependentString(installDir.path.relativize(path))
        INSTALLED_FILE_REGEX.pattern.matcher(pathString).matches && !pathString.contains("TEMP")
      }
    )
    readyToUpload(installedFiles.map(f => ArchiveEntry(f, installDir.relativize(f))), force = true)

    val msgs = Tracker
      .uploadedDuration()
      .groupBy(_._1)
      .map { case (uploaderLocation, times) =>
        val totalTime = times.map(_._2).sum
        s"$uploaderLocation in ${Utils.durationString(totalTime / 1000000L)}"
      }

    locations match {
      case Seq(location) => log.info(s"${Tracker.countProcessed} files uploaded to ${msgs.mkString("")}")
      case _ =>
        log.info(
          s"${Tracker.countProcessed} total files uploaded to ${locations.size} locations [${msgs.mkString(", ")}]")
    }
  }

  @async private def loadFilesSize(files: Seq[ArchiveEntry]): Seq[ArchiveEntry] = files.apar
    .map { entry =>
      val fileSize =
        if (entry.sizeBytes > 0) entry.sizeBytes
        else {
          val channel = FileChannel.open(entry.file.path)
          val readSize = channel.size
          channel.close()
          readSize
        }
      entry.copy(sizeBytes = fileSize)
    }
    .sortBy(_.sizeBytes)(Ordering[Long].reverse) // largest file first

  @node private[buildtool] def groupFilesToBatches(distinctFiles: Seq[ArchiveEntry]): Seq[Seq[ArchiveEntry]] = {
    val filesWithSize = loadFilesSize(distinctFiles)
    var smallFilesToBeUsed = filesWithSize.toList.sortBy(_.sizeBytes) // smallest file first
    val batches = Seq.newBuilder[Seq[ArchiveEntry]]
    val currentBatch = Seq.newBuilder[ArchiveEntry]
    var currentBatchSize: Long = 0
    val usedSmallFiles = Seq.newBuilder[ArchiveEntry]

    @tailrec def fillBatch(availableSpace: Long, availableSlot: Int): Unit = if (
      smallFilesToBeUsed.nonEmpty && availableSlot > 0 && availableSpace > 0
    ) {
      val minSizeFile = smallFilesToBeUsed.head
      if (minSizeFile.sizeBytes <= availableSpace) {
        currentBatch += minSizeFile
        usedSmallFiles += minSizeFile
        smallFilesToBeUsed = smallFilesToBeUsed.drop(1)
        fillBatch(availableSpace - minSizeFile.sizeBytes, availableSlot - 1)
      }
    }

    def putFileToBatch(largestEntry: ArchiveEntry): Unit = {
      currentBatchSize += largestEntry.sizeBytes
      currentBatch += largestEntry
    }

    def cleanBatchContainerForNextRound(): Unit = {
      currentBatch.clear()
      currentBatchSize = 0
    }

    def putFileToNextBatch(curFile: ArchiveEntry, curBatchFileNum: Int): Unit = {
      val remainSize = maxBatchBytes - currentBatchSize
      val remainSlot = maxBatchSize - curBatchFileNum
      if (remainSize > 0 && remainSlot > 0)
        fillBatch(remainSize, remainSlot)
      val filledCurrentBatch = currentBatch.result()
      if (filledCurrentBatch.nonEmpty) batches += filledCurrentBatch
      cleanBatchContainerForNextRound()
      putFileToBatch(curFile) // put file in new created batch
    }

    // apply simplified bin-packing(NP complete) algorithm here, we don't need strictly use the minimum batches since
    // it's may not be helpful to make NFS upload faster
    filesWithSize.foreach { largestEntry =>
      val isFirstEntry = filesWithSize.indexOf(largestEntry) == 0
      val complete = usedSmallFiles.result().contains(largestEntry)

      if (!complete) {
        val currentBatchResult = currentBatch.result()
        val noEnoughSpace =
          largestEntry.sizeBytes + currentBatchSize >= maxBatchBytes || currentBatchResult.size >= maxBatchSize

        if (isFirstEntry || !noEnoughSpace) putFileToBatch(largestEntry)
        else putFileToNextBatch(largestEntry, currentBatchResult.size)
      }
    }
    batches += currentBatch.result() // add last batch into batches
    batches.result()
  }

  @async private def upload(force: Boolean): Unit = {
    val files: Seq[ArchiveEntry] = Tracker.pollForUpload(force)
    if (files.nonEmpty) {
      val batchedFiles = groupFilesToBatches(files)
      batchedFiles.aseq.foreach { batch => uploadBatch(batch, force) }
      // ensuring we process all the files when force = true
      if (force) readyToUpload(Seq.empty, force)
    }
  }

  private def reEnqueuing(source: Asset, batch: Seq[ArchiveEntry], force: Boolean, exception: Throwable): Unit = {
    // no need to keep the source asset around anymore, so we can delete it
    source match {
      case archive: FileAsset  => safelyDelete(archive)
      case dir: DirectoryAsset => safelyDelete(Directory(dir.path))
    }

    if (!force) {
      // force is false, which means we are still building yet and we have time to retry.
      // The background job may be still be messing with the files, which can cause the upload failures.
      log.warn(s"Re-enqueuing ${batch.size} files for re-upload", exception)
      Tracker.enqueue(batch)
    } else {
      // force is true, which means the build is complete.
      // Something has gone really wrong here, and it's time to give up. :(
      throw exception
    }
  }

  private def getUploadLogMsg(files: Seq[ArchiveEntry]): String = {
    val filesTotalSize =
      BigDecimal(files.map(_.sizeBytes).sum / 1e+6).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    s"${files.size} files uploaded ($filesTotalSize MB)"
  }

  @async protected def uploadBatch(batch: Seq[ArchiveEntry], force: Boolean): Unit =
    uploadFormat match {
      case Raw =>
        val source = uploadDir.resolveDir(s"upload-${UUID.randomUUID}")
        asyncResult {
          ArchiveUtil.populateDirectory(source, batch)
          uploadToLocations(source, getUploadLogMsg(batch))
          safelyDelete(source)
        }.valueOrElse(reEnqueuing(source, batch, force, _))
      case format: ArchiveFormat =>
        archiveUpload(batch, format, force)
    }

  @async private def archiveUpload(files: Seq[ArchiveEntry], format: ArchiveFormat, force: Boolean): Unit = {
    val archive = format match {
      case Tar => uploadDir.resolveFile(s"upload-${UUID.randomUUID}.tar.gz")
      case Zip => uploadDir.resolveFile(s"upload-${UUID.randomUUID}.zip")
    }
    asyncResult {
      format match {
        case Tar =>
          TarUtils.populateTarGz(archive, files, deleteOriginals = false)
        case Zip =>
          log.warn("Files will be copied without preserving permissions")
          ArchiveUtil.populateZip(archive, files)
      }

      uploadToLocations(archive, getUploadLogMsg(files))
      safelyDelete(archive)
    }.valueOrElse(reEnqueuing(archive, files, force, _))
  }

  @async protected def uploadToLocations(source: Asset, logMsg: String = ""): Unit = {
    val durationPerLocation = uploaders.apar.map { uploader =>
      val durationInNanos = uploader.uploadToLocation(source, uploadFormat)
      Tracker.addUploadDuration(uploader.location.toString, durationInNanos)
      s"${uploader.location.toString} in ${Utils.durationString(durationInNanos / 1000000L)}"
    }
    log.info(s"$logMsg to ${durationPerLocation.size} locations [${durationPerLocation.mkString(", ")}]")
  }

  private def safelyDelete(zip: FileAsset): Unit =
    try {
      Files.deleteIfExists(zip.path)
    } catch {
      case NonFatal(ex) =>
        // nothing we can do but logging here...
        log.warn(s"Couldn't delete zip $zip", ex)
    }

  private def safelyDelete(dir: Directory): Unit =
    try {
      AssetUtils.recursivelyDelete(dir)
    } catch {
      case NonFatal(ex) =>
        // nothing we can do but logging here...
        log.warn(s"Couldn't delete directory $dir: $ex")
        log.debug("Will continue without deleting", ex)
    }
}
