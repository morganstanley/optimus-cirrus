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
 *
 * This source file is a fork of code from Zinc, to which the following license
 * applies
 *
 * Zinc - The incremental compiler for Scala.
 * Copyright Scala Center, Lightbend, and Mark Harrah
 *
 * Licensed under Apache License 2.0
 * SPDX-License-Identifier: Apache-2.0
 *
 * See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 */
package optimus.buildtool.utils

import org.apache.commons.compress.archivers.zip.ZipUtil
import sbt.internal.inc.IndexBasedZipOps

import java.io.BufferedOutputStream
import java.io.FileInputStream
import java.io.FileOutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.Channels
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import scala.util.Using

/**
 * Provides modified implementation of [[sbt.internal.inc.IndexBasedZipOps]].
 * The changed part is a data transfer logic, which is required for more performant
 * incremental compilations with compression, in both size and performance.
 *
 * Q: Why is this custom implementation necessary ?
 * A: The modification is required because of straight to jar incremental compilation behaviour.
 *    The original implementation was based on fact that central directory is the source of truth.
 *    It modified the central directory in following way:
 *    - removed files were removed from the central directory, but the bytes are still retained in the file header in zip,
 *    - modified files were appended to the data part, and the entry in central directory for this
 *        file has modified offset to point to new data. The previous version data was still retained in the data array,
 *    - added files were simply appended to both central directory and data.
 *
 *    After the jar was built incrementally, it will have dead bytes in the data part.
 *    This makes the jars very expensive in terms of disk usage.
 *
 *    After incremental compilation done by Zinc, OBT stamped this jars with file hashes, which are used to find changed entries.
 *
 *    In order to do this we've walked over zip and rehashed all entries into new directories.
 *    Because we iterated over central directory, dead bytes were skipped.
 *    The problem with previous implementation was lack of compression, because copying and rehashing all files required decompression of each entry.
 *    This was slowing down the process.
 *
 * In order to make the jars smaller, while still maintaining good performance, we've changed how jars are stamped.
 * From now on, stamping is happening only on changed entries. Also we stopped copying of each file to the new archive,
 * in favour of this implementation.
 *
 * Implementation:
 *   The new logic is based on the central directory. We're now merging the jars into a new one without decompression.
 *   This is done by direct byte transfer of compressed entries into a new array, while modifying the central directory
 *   entries offset on the fly.
 */
abstract class TruncatingIndexBasedZipOps extends IndexBasedZipOps {

  protected def getCompressedSize(header: Header): Long
  protected def setLastModifiedTime(header: Header, value: Long): Unit
  protected def setLastAccessedTime(header: Header, value: Long): Unit
  protected def setCreationTime(header: Header, value: Long): Unit

  def readCentralDirFromPath(path: Path): CentralDir = readCentralDir(path)

  /*
    local file header signature     ZIP_LONG   offset: 0
    version needed to extract       ZIP_SHORT  offset: 4
    general purpose bit flag        ZIP_SHORT  offset: 6
    compression method              ZIP_SHORT  offset: 8
    last mod file time              ZIP_SHORT  offset: 10
    last mod file date              ZIP_SHORT  offset: 12
    crc-32                          ZIP_LONG   offset: 14
    compressed size                 ZIP_lONG   offset: 18
    uncompressed size               ZIP_LONG   offset: 22
    file name length                ZIP_SHORT  offset: 26
    extra field length              ZIP_SHORT  offset: 28
   */
  private val LFH_LEN = 30
  private val ZIP_LONG = 4
  private val ZIP_DLONG = 8
  private val GENERAL_PURPOSE_FLAG_OFFSET = 6
  /* We are writing both time and date so it will take 4 bytes */
  private val LAST_MOD_FILE_TIME_OFFSET = 10
  private val COMPRESSED_SIZE_OFFSET = 18
  private val UNCOMPRESSED_SIZE_OFFSET = 22
  private val FILE_NAME_LENGTH_OFFSET = 26
  private val EXTRA_FIELD_LENGTH_OFFSET = 28

  private val WINDOWS_EPOCH_IN_MICROSECONDS = -11644473600000000L

  private val FIXED_DOS_TIME = ZipUtil.toDosTime(Jars.FIXED_EPOCH_MILLIS)
  private val FIXED_WINDOWS_TIME =
    (TimeUnit.MICROSECONDS.convert(Jars.FIXED_EPOCH_MILLIS, TimeUnit.MILLISECONDS) - WINDOWS_EPOCH_IN_MICROSECONDS) * 10

  private val ZIP64_MAGICVAL = 0xffffffffL
  private val DATA_DESCRIPTOR_FLAG = 1 << 3

  /* Extra field header ID */
  private val EXTID_NTFS: Int = 0x000a // NTFS
  private val EXTID_EXTT: Int = 0x5455 // Info-ZIP Extended Timestamp

  /**
   *  READ MORE AT OFFICIAL ZIP SPECIFICATION [[https://pkware.cachefly.net/webdocs/casestudies/APPNOTE.TXT]]
   *  SECTIONS 8.5.3 - 8.5.5
   *
   *  Spanned/Split archives may contain special spanning signature (0x08074b50) [[SINGLE_SEGMENT_SPLIT_MARKER ]]
   *  in the first 4 bytes of first entry. It may also appear for files that
   *  started as though they need spanning but in reality it was not necessary.
   *  archives if the spanning or splitting process starts but
   *  only requires one segment. The signature will then be equal to (0x30304b50)
   *
   *  Some implementations also may prefix Data Descriptor with the special spanning signature
   */
  private val DD_SIGNATURE = 0x08074b50
  private val SINGLE_SEGMENT_SPLIT_MARKER = 0x30304b50

  private def hasDataDescriptor(byteBuffer: ByteBuffer): Boolean = {
    val flags = byteBuffer.getShort(GENERAL_PURPOSE_FLAG_OFFSET)
    (flags & DATA_DESCRIPTOR_FLAG) != 0
  }

  private def updateExtraTimestamps(extra: ByteBuffer, extraLength: Int): Unit = {
    if (extraLength > 0xffff) throw new IllegalArgumentException("invalid extra field length")

    // extra fields are in "HeaderID(2)DataSize(2)Data... format
    var off = 0
    val len = extraLength
    if (off + 4 < len) {
      val tag = extra.getShort(off)
      val sz = extra.getShort(off + 2)
      off += 4
      if (off + sz > len) { return } // invalid data, ignore updating the timestamp

      tag match {
        case EXTID_NTFS =>
          // reserved  4 bytes + tag 2 bytes + size 2 bytes
          // m[a|c]time 24 bytes
          if (sz < 32) { return } // invalid data, ignore updating the timestamp
          val pos = off + 4 // reserved 4 bytes

          // invalid data, ignore updating the timestamp
          if (extra.getShort(pos) != 0x0001 || extra.getShort(pos + 2) != 24) { return }

          extra.putLong(off + 4, FIXED_WINDOWS_TIME)
          extra.putLong(off + 12, FIXED_WINDOWS_TIME)
          extra.putLong(off + 20, FIXED_WINDOWS_TIME)

        case EXTID_EXTT =>
          val flag = extra.get(off).toInt
          var sz0 = 1

          def setFixedUnixTime(bit: Int): Unit = if ((flag & bit) != 0 && (sz0 + 4) <= sz) {
            extra.putInt(off + sz0, Jars.FIXED_EPOCH_SECONDS)
            sz0 += 4
          }

          setFixedUnixTime(0x1)
          setFixedUnixTime(0x2)
          setFixedUnixTime(0x4)

        case _ =>
      }
      off += sz
    }
  }

  /**
   * Deals with splitting/spanning markers that may prefix the first LFH.
   * This method reads from channel and mutates [[lfhBuf]]
   */
  private def getSplittingMarkerOffset(source: FileChannel, lfhBuf: ByteBuffer, sourceEntryOffset: Long) = {
    val sig = lfhBuf.getInt(0)
    if (sig.equals(DD_SIGNATURE))
      throw new UnsupportedOperationException("Reading of split zip entries is not supported.")
    if (sig == SINGLE_SEGMENT_SPLIT_MARKER) {
      lfhBuf.clear()
      // we skip next 4 bytes
      source.read(lfhBuf, sourceEntryOffset + ZIP_LONG)
      ZIP_LONG
    } else 0
  }

  /**
   * Data descriptor is present if general purpose flag has its 3rd bit set.
   * This method only parses and extracts data descriptor length from source
   *
   * SEE MORE AT OFFICIAL ZIP SPECIFICATION [[https://pkware.cachefly.net/webdocs/casestudies/APPNOTE.TXT]]
   */
  private def getDataDescriptorLength(source: FileChannel, possibleDDOffset: Long, isZip64: Boolean): Long = {

    /** We want to ensure, that bytes are loaded in Little Endian order */
    val wordBuf = ByteBuffer.allocate(ZIP_LONG).order(ByteOrder.LITTLE_ENDIAN)

    source.read(wordBuf, possibleDDOffset)
    val descriptor = wordBuf.getInt(0)

    val ddSignatureLength = if (descriptor == DD_SIGNATURE) ZIP_LONG else 0
    val ddCrcLength = ZIP_LONG
    val ddSizeLength =
      if (isZip64) 2 * ZIP_DLONG
      else 2 * ZIP_LONG
    ddSignatureLength + ddCrcLength + ddSizeLength
  }

  private[utils] def transferAll(
      target: FileChannel,
      source: FileChannel,
      startPos: Long,
      sourceCentralDir: CentralDir,
  ): (Seq[Header], Long) = {

    /** We want to ensure, that bytes are loaded in Little Endian order */
    val lfhBuf = ByteBuffer.allocate(LFH_LEN).order(ByteOrder.LITTLE_ENDIAN)

    var writtenBytes = 0L
    var first = true

    def transferIntoTarget(targetOffset: Long, sourceOffset: Long, count: Long): Unit = {
      var remaining = count
      var offset = targetOffset

      while (remaining > 0) {
        source.position(sourceOffset + count - remaining)
        val transferred =
          /** We need to loop according to the documentation [[FileChannel#transferFrom]] */
          target.transferFrom(source, /*position = */ offset, /*count = */ remaining)

        offset += transferred
        writtenBytes += transferred
        remaining -= transferred
      }
    }

    val headers = getHeaders(sourceCentralDir).map { e =>
      val sourceEntryOffset = getFileOffset(e)
      source.read(lfhBuf, sourceEntryOffset)
      val splittingMarkerOffset = if (first) getSplittingMarkerOffset(source, lfhBuf, sourceEntryOffset) else 0
      first = false

      val extraLength = lfhBuf.getShort(EXTRA_FIELD_LENGTH_OFFSET)
      val nameLength = lfhBuf.getShort(FILE_NAME_LENGTH_OFFSET)
      val compressedLength = getCompressedSize(e)

      val compressedLocSize: Long = lfhBuf.getInt(COMPRESSED_SIZE_OFFSET)
      val uncompressedLocSize: Long = lfhBuf.getInt(UNCOMPRESSED_SIZE_OFFSET)
      val isZip64 = compressedLocSize == ZIP64_MAGICVAL || uncompressedLocSize == ZIP64_MAGICVAL

      /* We don't want to include first entry offset as it is used solely to distinguish split archives. */
      val targetEntryOffset = startPos + writtenBytes

      /** 1. Write local file header with timestamps fixed to [[ FIXED_DOS_TIME ]] */
      lfhBuf.put(LAST_MOD_FILE_TIME_OFFSET, FIXED_DOS_TIME)

      lfhBuf.rewind()
      target.write(lfhBuf, targetEntryOffset)
      writtenBytes += LFH_LEN

      /** 2. Transfer file name */
      val targetNameOffset = targetEntryOffset + LFH_LEN
      val sourceNameOffset = splittingMarkerOffset + sourceEntryOffset + LFH_LEN
      transferIntoTarget(targetNameOffset, sourceNameOffset, nameLength)

      /** 3. Read extra data, if required update contained timestamps and write it to target */
      val targetExtraOffset = targetNameOffset + nameLength
      val sourceExtraOffset = sourceNameOffset + nameLength

      if (extraLength > 0) {
        val extra = ByteBuffer.allocate(extraLength).order(ByteOrder.LITTLE_ENDIAN)
        source.read(extra, sourceExtraOffset)
        updateExtraTimestamps(extra, extraLength)

        extra.rewind()
        target.write(extra, targetExtraOffset)
        writtenBytes += extraLength
      }

      /** 4. Transfer the actual data and data descriptor field */

      val targetDataOffset = targetExtraOffset + extraLength
      val sourceDataOffset = sourceExtraOffset + extraLength

      val possibleDDOffset = sourceDataOffset + compressedLength
      val dataDescriptorLength =
        if (hasDataDescriptor(lfhBuf)) getDataDescriptorLength(source, possibleDDOffset, isZip64) else 0

      val sourceDataSize = compressedLength + dataDescriptorLength
      transferIntoTarget(targetDataOffset, sourceDataOffset, sourceDataSize)

      /** 5. Update headers to reflect new local file header timestamps and positions */
      setFileOffset(e, targetEntryOffset)
      setLastModifiedTime(e, Jars.FIXED_EPOCH_MILLIS)

      /**
       * Last access time and creation time are not always present and require NTFS or Unix extra data
       * We delegate the handling whether those fields should be added to [[sbt.internal.inc.zip.ZipCentralDir]]
       * which adds them only when required
       */
      setLastAccessedTime(e, Jars.FIXED_EPOCH_MILLIS)
      setCreationTime(e, Jars.FIXED_EPOCH_MILLIS)

      lfhBuf.clear()
      e
    }

    (headers, writtenBytes)
  }

  private def openFileForReading(path: Path): FileChannel = {
    new FileInputStream(path.toFile).getChannel
  }

  private def openFileForWriting(path: Path): FileChannel = {
    new FileOutputStream(path.toFile, /*append = */ true).getChannel
  }

  private def finalizeZip(
      target: FileChannel,
      centralDir: CentralDir,
      centralDirStart: Long
  ): Unit = {
    setCentralDirStart(centralDir, centralDirStart)
    target.position(centralDirStart)

    Using.resource(new BufferedOutputStream(Channels.newOutputStream(target))) { outputStream =>
      writeCentralDir(centralDir, outputStream)
    }
  }

  /**
   * Merges two archives together without any leftover bytes being copied to target archive.
   * This method should be applied with extra care as it has input requirements:
   * - Target archive and source archive do not contain duplicates, the consumers of this API has to ensure this.
   * - If the input is a jar, the consumer has to manually ensure that either target jar starts with jar magic number OxCAFE,
   *     as it is required to mark jar as executable. This is only true if the user provides non empty target central directory,
   *     or target path is empty but source path does not start with magic number.
   * - The target archive does not contain leftover bytes, as they won't be cleaned up in the process.
   *     (That being said, the method can allow the latter, but the result jar will not be optimal in size)
   *
   * The method is only capable of truncating source archives, because this operation requires copying of bytes.
   * Without this assumption we would need to always copy contents of both entries into a new archive, and
   * it may be suboptimal if this method is used to merge multiple jars (see [[ Jars.mergeJarGivenTokens]])
   *
   * This method does not modify, mutate or clean the original source jar.
   * If desired, the removal / cleanup has to be performed manually.
   */
  def mergeArchivesUnsafe(
      targetCentralDir: CentralDir,
      targetPath: Path,
      sources: Seq[(CentralDir, Path)],
  ): Unit = {
    Using.resource(openFileForWriting(targetPath)) { targetChannel =>
      val targetEntryEnd = getCentralDirStart(targetCentralDir)
      targetChannel.truncate(targetEntryEnd)

      val targetHeaders = getHeaders(targetCentralDir)

      val (newCentralDirectoryOffset, sourceHeaders) = sources.foldLeft[(Long, Seq[Header])](targetEntryEnd, Seq()) {
        (acc, source) =>
          val (currentOffset, currentHeaders) = acc
          val (sourceCentralDir, sourcePath) = source

          Using.resource(openFileForReading(sourcePath)) { sourceChannel =>
            val (sourceHeaders, writtenBytes) =
              transferAll(target = targetChannel, source = sourceChannel, currentOffset, sourceCentralDir)
            val sortedHeaders = sourceHeaders.sortBy(e => getFileOffset(e))

            (writtenBytes + currentOffset, currentHeaders ++ sortedHeaders)
          }
      }

      val mergedHeaders = (targetHeaders ++ sourceHeaders)
      setHeaders(targetCentralDir, mergedHeaders)
      finalizeZip(targetChannel, targetCentralDir, centralDirStart = newCentralDirectoryOffset)
    }
  }

  override def mergeArchives(target: Path, source: Path): Unit = {
    val targetCentralDir = readCentralDir(target)
    val sourceCentralDir = readCentralDir(source)

    val targetEntries = getHeaders(targetCentralDir).map(getFileName).toSet
    val sourceEntries = getHeaders(targetCentralDir).map(getFileName).toSet

    val duplicates = sourceEntries intersect targetEntries
    if (duplicates.nonEmpty)
      throw new IllegalArgumentException(s"Duplicate file entries: ${duplicates.mkString(", ")}")

    mergeArchivesUnsafe(targetCentralDir, target, Seq((sourceCentralDir, source)))
  }
}
