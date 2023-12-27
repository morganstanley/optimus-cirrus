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
package optimus.buildtool.utils

import java.io.ByteArrayInputStream
import java.io.IOException
import java.io.InputStream
import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.AccessDeniedException
import java.nio.file.FileSystem
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.SimpleFileVisitor
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.UserDefinedFileAttributeView
import java.util

import com.google.common.hash
import com.google.common.hash.HashCode
import com.google.common.hash.HashFunction
import com.sun.management.ThreadMXBean
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.files.Asset
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.JarAsset
import org.slf4j.LoggerFactory.getLogger

import scala.collection.mutable
import scala.util.control.NonFatal

final class HashedContent(val hash: String, private val content: Array[Byte]) {
  def contentAsInputStream: InputStream = new ByteArrayInputStream(content)
  def utf8ContentAsString: String = new String(content, StandardCharsets.UTF_8)
  def size: Int = content.length

  // we assume that equal hashes mean equal content (which is a fair assumption unless you think SHA-2 is broken)
  override def equals(obj: Any): Boolean = obj match {
    case h: HashedContent => this.hash == h.hash
    case _                => false
  }
  override def hashCode(): Int = hash.hashCode
  override def toString: String = f"HashedContent($hash, ${content.length}%,d bytes)"
}

object Hashing {
  private val log = getLogger(this.getClass)

  val hashFunction: HashFunction = hash.Hashing.sha256()
  private val optimusHashAttribute = "optimus-hash"
  private[utils] val optimusHashPath = "META-INF/optimus/contentHash.sha256"
  private[utils] val optimusPerEntryHashPath = "META-INF/optimus/entryHashes.sha256"

  // Jars we produce ourselves with fingerprints need to be hashed consistently by their contents
  // not the Jar binary itself, which is not guaranteed to be identical for the same contents
  private val hashSuffix = "." + NamingConventions.HASH

  val DefaultChunkSize: Int = 1024 * 1024
  private[utils] val MaxRetainedBufferSize = DefaultChunkSize * 8

  final class GrowableBuffer(initial: Array[Byte]) {

    def this() = this(new Array[Byte](DefaultChunkSize))

    private var currentBuff: Array[Byte] = initial
    def buffer: Array[Byte] = currentBuff

    def ensureCapacity(size: Int): Unit = {
      if (size >= buffer.length) {
        currentBuff = util.Arrays.copyOf(currentBuff, buffer.length * 2)
      }
    }

    def shrinkIfHuge(): Unit = {
      if (currentBuff.length > MaxRetainedBufferSize) currentBuff = new Array[Byte](DefaultChunkSize)
    }
  }

  /**
   * strips any carriage returns in buffer and returns the new length (which will be shorter by the number of CRs)
   */
  private[buildtool] def convertCrLfToLfInPlace(buffer: Array[Byte], length: Int): Int = {
    var inPos = 0
    var outPos = 0
    while (inPos < length) {
      val inCh = buffer(inPos)

      // this is the same mechanism git uses to detect binary files - it just assumes that text files cannot contain NUL
      // chars and that binary files always do (which is probably true in practice for most non-trivial binary files).
      // this exception will be caught further up and we'll reprocess the file in binary mode instead.
      if (inCh == 0)
        throw new IllegalArgumentException(s"character at offset $inPos was \\0 - this doesn't look like a text file")

      // if the current byte is not a CR, copy it over
      if (inCh != 13) {
        buffer(outPos) = inCh
        outPos += 1
      }
      // if the current byte is a CR but the next is not an LF, (i.e. it's a standalone CR), replace with LF
      else if (inPos + 1 == length || buffer(inPos + 1) != 10) {
        buffer(outPos) = 10
        outPos += 1
      }
      // else drop it
      inPos += 1
    }
    outPos
  }

  private[buildtool] def convertLfToCrLf(buffer: Array[Byte], length: Int): (Array[Byte], Int) = {
    val growable = new GrowableBuffer(util.Arrays.copyOf(buffer, length))
    var inPos = 0
    var outPos = 0
    while (inPos < length) {
      val inCh = buffer(inPos)

      // this is the same mechanism git uses to detect binary files - it just assumes that text files cannot contain NUL
      // chars and that binary files always do (which is probably true in practice for most non-trivial binary files).
      // this exception will be caught further up and we'll reprocess the file in binary mode instead.
      if (inCh == 0)
        throw new IllegalArgumentException(s"character at offset $inPos was \\0 - this doesn't look like a text file")

      // if the current byte is:
      // - not a CR or LF
      // - the leading CR from a CRLF
      // - the trailing LF from a CRLF
      // then copy it over
      if (
        (inCh != 10 && inCh != 13) ||
        (inCh == 13 && inPos + 1 < length && buffer(inPos + 1) == 10) ||
        (inCh == 10 && inPos > 0 && buffer(inPos - 1) == 13)
      ) {
        growable.ensureCapacity(outPos + 1)
        growable.buffer(outPos) = inCh
        outPos += 1
      }
      // the current byte is a standalone CR or LF, so replace with CRLF
      else {
        growable.ensureCapacity(outPos + 2)
        growable.buffer(outPos) = 13
        growable.buffer(outPos + 1) = 10
        outPos += 2
      }
      inPos += 1
    }
    (growable.buffer, outPos)
  }

  def hashString(str: String): String = {
    NamingConventions.HASH + hashFunction.hashString(str, StandardCharsets.UTF_8).toString
  }

  def hashStrings(strs: Seq[String]): String = {
    val hasher = hashFunction.newHasher()
    // much more efficient to hash each string than concatenate them and hash the result
    strs.foreach { s =>
      hasher.putUnencodedChars(s)
      // need to separate the strings, else different inputs can give the same hash
      hasher.putByte(10)
    }
    NamingConventions.HASH + hasher.hash()
  }

  def fileCanBeStampedWithConsistentHash(file: FileAsset): Boolean =
    fileCanBeStampedWithConsistentHash(file.pathString)

  def fileCanBeStampedWithConsistentHash(pathString: String): Boolean = {
    pathString.endsWith(".jar") && pathString.contains(hashSuffix)
  }

  def consistentlyHashDirectory(directory: Directory, strict: Boolean = true): String = {
    val fileHashes = mutable.Buffer[(String, String)]()
    Files.walkFileTree(
      directory.path,
      new SimpleFileVisitor[Path] {
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          if (Files.isReadable(file) && Files.isRegularFile(file))
            fileHashes += (directory.path.relativize(file).toString -> hashFileContent(FileAsset(file), strict))
          FileVisitResult.CONTINUE
        }
        override def visitFileFailed(file: Path, exc: IOException): FileVisitResult = {
          // We can still get AccessDeniedExceptions if we get to directories we can't read (and we get those ADEs
          // before we have a chance to skip the directory in `preVisitDirectory`
          if (Files.isDirectory(file) && exc.isInstanceOf[AccessDeniedException]) FileVisitResult.CONTINUE
          else throw exc
        }
      }
    )
    consistentlyHashFileHashes(fileHashes)
  }

  def consistentlyHashFileHashes(filesToHashes: Seq[(String, String)]): String = {
    val hasher = hashFunction.newHasher()
    val sorted = filesToHashes.map { case (p, h) => (PathUtils.platformIndependentString(p), h) }.sortBy(_._1)
    sorted.foreach { case (p, h) =>
      hasher.putUnencodedChars(p)
      hasher.putUnencodedChars(h)
    }
    hasher.hash().toString
  }

  // Strictly speaking, system dirs aren't immutable. However, we pretend that they are so that we can avoid a
  // deep scan of them.
  private val SystemDirs = Set("//lib", "//lib64", "//usr/include", "//usr/lib", "//usr/lib64")
  def isAssumedImmutable(asset: Asset): Boolean = {
    val fingerprint = asset.pathFingerprint
    fingerprint.startsWith(NamingConventions.AfsDistStr) || NamingConventions.isHttpOrHttps(fingerprint) ||
    SystemDirs.exists(asset.pathString.startsWith)
  }

  def hashFileOrDirectoryContent(asset: Asset, assumedImmutable: Boolean = false, strict: Boolean = true): String = {
    if (assumedImmutable || isAssumedImmutable(asset))
      NamingConventions.IMMUTABLE
    else if (asset.existsUnsafe) asset match {
      case d: Directory => NamingConventions.HASH + consistentlyHashDirectory(d, strict)
      case f: FileAsset => hashFileContent(f, strict)
      case x            => throw new MatchError(x)
    }
    else NamingConventions.EMPTYHASH
  }

  def hashFileContent(file: FileAsset): String =
    hashFileContent(file, returnContent = false, strict = true).hash

  def hashFileContent(file: FileAsset, strict: Boolean): String =
    hashFileContent(file, returnContent = false, strict).hash

  def hashFileWithContent(file: FileAsset): HashedContent = hashFileContent(file, returnContent = true, strict = true)

  def hashFileInputStreamWithContent(file: String, inputStream: () => InputStream): HashedContent =
    hashFileInputStream(file, inputStream, returnContent = true)

  private def consistentlyHashJar(jar: JarAsset): String = {
    val attributeHash = readFileAttribute(jar, optimusHashAttribute)

    attributeHash.getOrElse {
      val zipfs = JarUtils.jarFileSystem(jar)
      val hash =
        try
          readStoredHash(zipfs).getOrElse(
            consistentlyHashDirectory(Directory(zipfs.getPath("/")))
          )
        finally zipfs.close()
      writeFileAttribute(jar, optimusHashAttribute, hash)
      hash
    }
  }

  private val threadLocalByteBuffer: ThreadLocal[ByteBuffer] = new ThreadLocal()
  private[utils] def readFileAttribute(file: FileAsset, name: String): Option[String] =
    try {
      val attrs = Files.getFileAttributeView(file.path, classOf[UserDefinedFileAttributeView])
      if (attrs.list.contains(name)) {
        val size = attrs.size(name)
        val localBuffer = threadLocalByteBuffer.get()
        val buffer = if (localBuffer != null && localBuffer.limit() == size) {
          localBuffer
        } else {
          val newBuffer = ByteBuffer.allocate(size)
          threadLocalByteBuffer.set(newBuffer)
          newBuffer
        }
        attrs.read(name, buffer)
        buffer.flip()
        val hash = StandardCharsets.UTF_8.decode(buffer).toString
        buffer.clear()
        if (hash.nonEmpty) Some(hash)
        else {
          log.debug(s"Read empty attribute '$name' from $file")
          None
        }
      } else None
    } catch {
      case NonFatal(t) =>
        log.debug(s"Failed to read attribute '$name' from $file", t)
        None
    }

  private[utils] def writeFileAttribute(file: FileAsset, name: String, value: String): Boolean =
    try {
      val attrs = Files.getFileAttributeView(file.path, classOf[UserDefinedFileAttributeView])
      attrs.write(name, StandardCharsets.UTF_8.encode(value))
      true
    } catch {
      case NonFatal(t) =>
        log.debug(s"Failed to write attribute '$name' -> '$value' to $file", t)
        false
    }

  private def hashFileContent[A](file: FileAsset, returnContent: Boolean, strict: Boolean): HashedContent = {
    try {
      // If this file is a jar and needs to be hashed consistently
      if (fileCanBeStampedWithConsistentHash(file)) {
        if (returnContent) throw new IllegalArgumentException(s"Cannot return content for $file")
        new HashedContent(NamingConventions.HASH + consistentlyHashJar(file.asJar), null)
      } else {
        _hashFileInputStream(file.name, () => Files.newInputStream(file.path), returnContent, strict)
      }
    } catch {
      case e: Exception => throw new IOException(s"Failed to hash $file", e)
    }
  }

  private def hashFileInputStream(file: String, inputStream: () => InputStream, returnContent: Boolean) = {
    try {
      _hashFileInputStream(file, inputStream, returnContent)
    } catch {
      case e: Exception => throw new IOException(s"Failed to hash $file", e)
    }
  }

  private def _hashFileInputStream(
      file: String,
      inputStream: () => InputStream,
      returnContent: Boolean,
      strict: Boolean = true
  ) = {
    def hashAsBinary =
      if (returnContent) hashBinaryStreamWithContent(inputStream) else (hashBinaryStream(inputStream), null)
    def hashAsText(lfEndings: Boolean, isStrict: Boolean) =
      if (isStrict)
        hashTextStream(inputStream, returnTextContent = returnContent, lfEndings = lfEndings)
      else
        try hashTextStream(inputStream, returnTextContent = returnContent, lfEndings = lfEndings)
        catch {
          case _: IllegalArgumentException =>
            log.debug(
              s"Failed to read file as text: $file. Will read as binary instead. " +
                "Consider changing the extension of the file if it's incorrect or unnecessarily obscure, " +
                "or update the extensions in optimus.buildtool.utils.Hashing.binaryFileExtns"
            )
            hashAsBinary
        }

    val (hc, content) = {
      val extn = NamingConventions.suffix(file)
      // hash text files as text (with CRLF -> LF conversion), and fail if that fails
      if (NamingConventions.isTextExtension(extn)) hashAsText(lfEndings = true, strict)
      // hash windows text files as text (with LF -> CRLF conversion), and fail if that fails
      else if (NamingConventions.isWindowsTextExtension(extn)) hashAsText(lfEndings = false, strict)
      // hash binary files as binary (no CRLF conversion)
      else if (NamingConventions.isBinaryExtension(extn)) hashAsBinary
      // for unknown files, try as text (with CRLF -> LF conversion), and fallback to binary with a warning
      else hashAsText(lfEndings = true, isStrict = false)
    }
    new HashedContent(NamingConventions.HASH + hc, content)
  }

  def hashBytes(data: Array[Byte]): String = NamingConventions.HASH + hashFunction.hashBytes(data)

  private val threadLocalBuffer: ThreadLocal[GrowableBuffer] =
    ThreadLocal.withInitial(() => new GrowableBuffer)

  def hashBinaryStream(inputStream: () => InputStream): HashCode = {
    // we read and hash a chunk at a time. we don't retain the content.
    val s = inputStream()
    try {
      val growable = threadLocalBuffer.get()
      val hasher = hashFunction.newHasher()
      var read = s.read(growable.buffer)
      while (read != -1) {
        hasher.putBytes(growable.buffer, 0, read)
        read = s.read(growable.buffer)
      }
      hasher.hash()
    } finally s.close()
  }

  private def hashBinaryStreamWithContent(inputStream: () => InputStream): (HashCode, Array[Byte]) = {
    val (content, length) = readBytesToSharedBuffer(inputStream, DefaultChunkSize)
    (hashFunction.hashBytes(content, 0, length), util.Arrays.copyOf(content, length))
  }

  /** note that the returned Array[Byte] may get clobbered later, so if you want to keep it you should copy it */
  private def readBytesToSharedBuffer(
      inputStream: () => InputStream,
      chunkSize: Int = DefaultChunkSize
  ): (Array[Byte], Int) = {
    val growable = threadLocalBuffer.get()
    var endPos = 0
    val s = inputStream()
    try {
      var bytesRead = s.read(growable.buffer, 0, chunkSize)
      while (bytesRead != -1) {
        endPos += bytesRead
        growable.ensureCapacity(endPos + chunkSize)
        bytesRead = s.read(growable.buffer, endPos, chunkSize)
      }
    } finally s.close()
    val content = growable.buffer
    // we might have read a really huge file and grown the buffer excessively, so now is a good time to shrink it
    threadLocalBuffer.get().shrinkIfHuge()
    (content, endPos)
  }

  def hashTextStream(
      inputStream: () => InputStream,
      chunkSize: Int = DefaultChunkSize,
      returnTextContent: Boolean = true,
      lfEndings: Boolean = true
  ): (HashCode, Array[Byte]) = {
    val (buffer, length) = readBytesToSharedBuffer(inputStream, chunkSize)
    val (newBuffer, newLength) =
      if (lfEndings) (buffer, convertCrLfToLfInPlace(buffer, length))
      else convertLfToCrLf(buffer, length)

    val content = if (returnTextContent) util.Arrays.copyOf(newBuffer, newLength) else null
    (hashFunction.hashBytes(newBuffer, 0, newLength), content)
  }

  private[utils] def readStoredHash(zip: FileSystem): Option[String] = {
    val hashPath = zip.getPath(optimusHashPath)
    if (Files.isRegularFile(hashPath))
      Some(new String(Files.readAllBytes(hashPath), StandardCharsets.UTF_8))
    else None
  }
}

object HashingBenchmark extends App {
  val log = getLogger(getClass)
  val mx = ManagementFactory.getThreadMXBean
    .asInstanceOf[ThreadMXBean]
  def hashAll(): Unit = {
    val start = System.currentTimeMillis()
    val startBytes = mx.getThreadAllocatedBytes(Thread.currentThread().getId)
    val hash = Hashing.hashFileOrDirectoryContent(Directory(Paths.get(args(0))))
    val duration = System.currentTimeMillis() - start
    val allocated = mx.getThreadAllocatedBytes(Thread.currentThread().getId) - startBytes
    log.info(f"hash = $hash, time = $duration%,dms, allocated = ${allocated / 1024 / 1024}%,dMB")
  }

  while (true) hashAll()
}
