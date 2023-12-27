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

import java.io.BufferedInputStream
import java.io.BufferedWriter
import java.io.IOException
import java.io.InputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.OutputStream
import java.io.OutputStreamWriter
import java.io.Serializable
import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.AccessDeniedException
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.CopyOption
import java.nio.file.FileAlreadyExistsException
import java.nio.file.FileSystemException
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.BasicFileAttributes
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream
import com.sun.management.UnixOperatingSystemMXBean
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.JsonAsset
import org.slf4j.LoggerFactory.getLogger

import java.nio.file.LinkOption
import java.nio.file.SimpleFileVisitor
import java.nio.file.attribute.BasicFileAttributeView
import scala.collection.mutable
import scala.ref.SoftReference
import scala.util.Try
import scala.util.control.NonFatal

object AssetUtils {
  private val log = getLogger(this.getClass)

  /**
   * Facilitates an atomic write of targetFile by generating a temporary file name, passing this to the specified fn
   * (which is expected to create that temporary file with the required content), and then atomically moving the
   * temporary file to the targetFile (clobbering it if it already exists).
   *
   * Note that this method does not create the temporary file - that is the responsibility of the supplied fn. If the fn
   * fails we do attempt to delete the temporary file (if it exists).
   */
  def atomicallyWrite[T](
      targetFile: FileAsset,
      replaceIfExists: Boolean = false,
      localTemp: Boolean = false,
      backupPrevious: Boolean = false
  )(
      fn: Path => T
  ): T = {
    val tempFile = NamingConventions.tempFor(targetFile, localTemp)
    try {
      val t = fn(tempFile.path)
      atomicallyMove(
        tempFile,
        targetFile,
        replaceIfExists = replaceIfExists,
        backupPrevious = backupPrevious,
        sourceMustExists = true)
      t
    } finally Files.deleteIfExists(tempFile.path)
  }

  /**
   * Calls atomicallyWrite if the file doesn't exist, else does nothing.
   *
   * This is a very commonly used pattern in OBT: Since we include a hash of the inputs needed to compute the file in
   * the file's name name, and since we ensure that writes are atomic, we can safely assume that if the file exists with
   * the expected name, then it has the expected content so we don't need to compute and write it again.
   */
  def atomicallyWriteIfMissing[T](targetFile: FileAsset)(fn: Path => T): Option[T] =
    if (!targetFile.existsUnsafe) {
      Some(atomicallyWrite(targetFile)(fn))
    } else None

  /**
   * Atomically moves source to target. If the target already exists, then if replaceIfExists is true we replace it,
   * else if replaceIfExists is false we do nothing
   */
  def atomicallyMove(
      source: FileAsset,
      target: FileAsset,
      replaceIfExists: Boolean = false,
      backupPrevious: Boolean = false,
      sourceMustExists: Boolean = false): Unit = {
    // deletes without throwing for temp files
    def safeDelete(asset: FileAsset): Unit = {
      try Files.deleteIfExists(asset.path)
      catch {
        case NonFatal(t) =>
          log.debug(s"deletion of intermediary file failed", t)
      }
    }

    def atomicMove(src: FileAsset, tgt: FileAsset, opts: Seq[CopyOption]) =
      Try[Unit] {
        Files.move(src.path, tgt.path, (opts :+ StandardCopyOption.ATOMIC_MOVE).toSeq: _*)
      }
        .recover { case _: AtomicMoveNotSupportedException =>
          // non-atomically move to somewhere on the target filesystem, and then atomically move from there
          log.debug("FS does not support atomic move")
          val siblingTempFile = NamingConventions.tempFor(tgt, local = true)
          try {
            Files.move(src.path, siblingTempFile.path)
            Files.move(siblingTempFile.path, tgt.path, opts.toSeq: _*)
          } finally safeDelete(siblingTempFile)
        }

    val opts = if (replaceIfExists) Seq(StandardCopyOption.REPLACE_EXISTING) else Seq.empty
    val backup = target.parent.resolveFile(s"${target.name}.old")

    try {
      // ok so this is a bit weird and it bears explaining.
      //
      // say we are performing an atomic move (with replaceIfExists) from A to B and B happens to exist, the usual
      // approach would be to simply move A onto B, overwriting B. However, this is not as simple as it may seem on
      // Windows. The reason is that B could be locked because another process is reading it. This is actually quite
      // common because of anti-virus protections: whenever we write something, the AV program picks up that we have
      // written the file and immediately scans it, so for a short while we can't write to that file again!
      //
      // This sounds very annoying to fix however because it's a windows problem we actually have a nice solution: move
      // the locked file out before we attempt to write in its location. This doesn't sound like it would work... if I
      // can't write to a location, surely I shouldn't be able to delete it, right? Well that turns out not to be the
      // case! Apparently some windows processes can open files in "delete-allowed" mode that will let deletes go
      // through but NOT writes. To take advantage of this, we thus always move an existing target out if we are
      // replacing it. (We delete this file or use it as backup at the end of this function.
      //
      // The main source of this problem are "updating" functions, such as for classpath-mapping and in
      // stampJarWithConsistentHash. These are also performance bottleneck so we may want to revisit all file updates
      // eventually (OPTIMUS-49568).
      if (replaceIfExists)
        try {
          // this move doesn't need to be atomic because no one is looking at the backup
          Files.move(target.path, backup.path, StandardCopyOption.REPLACE_EXISTING)
        } catch {
          case _: NoSuchFileException =>
        }

      // initial attempt at move
      val firstAttempt = atomicMove(source, target, opts)
      val notReplacingErrors =
        if (replaceIfExists) firstAttempt
        else
          // if we are not replacing, we can ignore target already exists errors
          firstAttempt
            .recover {
              case _: FileAlreadyExistsException =>
                log.debug(s"${target.pathString} already exists, not replacing it")
              case _: AccessDeniedException if target.existsUnsafe => // also this one, the file is probably just locked
                log.debug(
                  s"${target.pathString} already exists but is locked by another thread or process, not replacing it")
            }
      // don't fail if source is nonexistent
      val nonexistentSource =
        if (sourceMustExists) notReplacingErrors
        else
          notReplacingErrors.recover {
            case _: NoSuchFileException if !source.existsUnsafe =>
              log.warn(s"Not moving $source to $target since $source doesn't exist")
          }

      if (replaceIfExists && !backupPrevious) safeDelete(backup)

      nonexistentSource.get //  throw everything else, called for side-effects
    } catch {
      // All uncaught errors are thrown using IllegalStateException to avoid caching of non-RT exceptions, such as when the disk is full.
      // see OPTIMUS-49251 for details.
      case NonFatal(t) =>
        log.warn(s"Failed to move $source to $target: ${StackUtils.oneLineStacktrace(t)}")
        throw new IllegalStateException(s"Failed to move $source to $target", t)
    }
  }

  def atomicallyCopy(source: FileAsset, target: FileAsset, replaceIfExists: Boolean = false): Unit =
    atomicallyWrite(target, replaceIfExists)(temp => copy(source, FileAsset(temp)))

  def copy(source: FileAsset, target: FileAsset, replaceIfExists: Boolean = false): Unit = {
    if (replaceIfExists)
      Files.copy(source.path, target.path, StandardCopyOption.COPY_ATTRIBUTES, StandardCopyOption.REPLACE_EXISTING)
    else
      Files.copy(source.path, target.path, StandardCopyOption.COPY_ATTRIBUTES)
  }

  def loadFileAssetToString(file: FileAsset): String =
    try Files.readString(file.path)
    catch {
      case NonFatal(t) =>
        log.warn(s"Failed to load $file")
        throw new IllegalStateException(s"Failed to load ${file.pathString}", t)
    }

  def recursivelyDelete(
      dir: Directory,
      pred: Path => Boolean = _ => true,
      throwOnFail: Boolean = false,
      retainRoot: Boolean = false): Unit =
    recursivelyDelete(dir, (path, _) => pred(path), throwOnFail, retainRoot)

  def recursivelyDelete(
      dir: Directory,
      pred: (Path, BasicFileAttributes) => Boolean,
      throwOnFail: Boolean,
      retainRoot: Boolean): Unit = {
    var doomstack: List[Boolean] = Nil
    var doomed = false
    Files.walkFileTree(
      dir.path,
      new SimpleFileVisitor[Path]() {
        override def preVisitDirectory(d: Path, attrs: BasicFileAttributes): FileVisitResult = {
          doomstack = doomed :: doomstack
          doomed ||= pred(d, attrs)
          FileVisitResult.CONTINUE
        }
        override def postVisitDirectory(d: Path, exc: IOException): FileVisitResult = {
          if (doomed && !(retainRoot && d == dir.path)) safeDelete(d)
          doomed = doomstack.head
          doomstack = doomstack.tail
          FileVisitResult.CONTINUE
        }

        override def visitFile(f: Path, attrs: BasicFileAttributes): FileVisitResult = {
          if (doomed || pred(f, attrs)) safeDelete(f)
          FileVisitResult.CONTINUE
        }

        override def visitFileFailed(f: Path, exc: IOException): FileVisitResult = {
          if (Files.exists(f, LinkOption.NOFOLLOW_LINKS) && !Files.exists(f)) {
            try {
              // special case for broken symlinks/junctions - these reach visitFileFailed because they aren't walkable,
              // but they can be deleted as normal files

              val attrs = Files.getFileAttributeView(f, classOf[BasicFileAttributeView], LinkOption.NOFOLLOW_LINKS)
              visitFile(f, attrs.readAttributes())
            } catch {
              case e: IOException if !throwOnFail =>
                log.warn(s"Ignoring error when trying to handle broken link: $e")
                FileVisitResult.CONTINUE
            }
          } else if (throwOnFail) throw exc
          else FileVisitResult.CONTINUE
        }

        private def safeDelete(f: Path): Unit = {
          try Files.delete(f)
          catch {
            case e: IOException if !throwOnFail =>
              if (Files.exists(f, LinkOption.NOFOLLOW_LINKS))
                log.warn(s"Ignoring error when trying to delete file: $e")
          }
        }
      }
    )
  }

  def writeZip(file: FileAsset, s: CharSequence): Unit = {
    var zOut: BufferedWriter = null
    try {
      val out = Files.newOutputStream(file.path, StandardOpenOption.CREATE_NEW)
      zOut = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(out), UTF_8))
      zOut.append(s)
    } catch {
      case t: Throwable =>
        log.error(s"Failed to write to ${file.path}", t)
        if (t.isInstanceOf[FileSystemException]) {
          ManagementFactory.getOperatingSystemMXBean match {
            case b: UnixOperatingSystemMXBean =>
              val open = b.getOpenFileDescriptorCount
              val max = b.getMaxFileDescriptorCount
              log.info(s"$open/$max open file descriptors")
            case _ => // do nothing
          }
        }
        throw t
    } finally {
      if (zOut ne null) zOut.close()
    }
  }

  def serializeAtomically[T <: Serializable](file: FileAsset, value: T, replaceIfMissing: Boolean): Unit = {
    atomicallyWrite(file, replaceIfMissing) { tmp =>
      var fos: OutputStream = null
      var oos: ObjectOutputStream = null
      try {
        fos = Files.newOutputStream(tmp, StandardOpenOption.CREATE_NEW)
        oos = new ObjectOutputStream(fos)
        oos.writeObject(value)
      } catch {
        case t: Throwable =>
          log.error(s"Failed to serialize to $tmp", t)
      } finally {
        if (oos ne null)
          oos.close()
        else if (fos ne null)
          fos.close()
      }
    }
  }

  def deserialize[T <: Serializable](file: FileAsset): Option[T] = {
    if (file.existsUnsafe) {
      var fis: InputStream = null
      var ois: ObjectInputStream = null
      try {
        fis = Files.newInputStream(file.path, StandardOpenOption.READ)
        ois = new ObjectInputStream(fis)
        val obj = ois.readObject().asInstanceOf[T]
        Some(obj)
      } catch {
        case t: Throwable =>
          log.error(s"Failed to deserialize ${file.path}", t)
          None
      } finally {
        if (ois ne null)
          ois.close()
        else if (fis ne null)
          fis.close()
      }
    } else
      None
  }

  // avoid reallocating new buffers on each invocation of toByteArray
  private[this] val localBuffers =
    ThreadLocal.withInitial[SoftReference[mutable.Buffer[ByteBuffer]]] { () =>
      SoftReference(mutable.Buffer[ByteBuffer]())
    }
  private[buildtool] def toByteArray(stream: InputStream): Array[Byte] = {
    val buffers = localBuffers.get match {
      case SoftReference(b) => b
      case _ =>
        val b = mutable.Buffer[ByteBuffer]()
        localBuffers.set(SoftReference(b))
        b
    }
    val startingSize = 65536L // Long to protect against overflow
    var bufIx = 0
    def nextBuffer: ByteBuffer = {
      if (!buffers.isDefinedAt(bufIx)) {
        val newSize = math.min(startingSize << buffers.size, Int.MaxValue).toInt
        buffers += ByteBuffer.allocate(newSize)
      }
      try buffers(bufIx)
      finally bufIx += 1
    }

    val channel = Channels.newChannel(stream)

    try {
      var complete = false
      do {
        val buffer = nextBuffer
        val readBytes = channel.read(buffer)
        complete = readBytes < buffer.capacity
      } while (!complete)
      val output = ByteBuffer.allocate(buffers.map(_.position()).sum)
      buffers.foreach { b =>
        if (b.position() > 0) {
          b.flip()
          output.put(b)
          b.clear()
        }
      }
      output.array
    } finally channel.close()
  }

  import spray.json._

  def readJson[T: JsonFormat](file: JsonAsset, unzip: Boolean = true): T = {
    var zIn: GZIPInputStream = null
    try {
      val bytes = if (unzip) {
        val in = Files.newInputStream(file.path, StandardOpenOption.READ)
        zIn = new GZIPInputStream(new BufferedInputStream(in))
        toByteArray(zIn)
      } else {
        Files.readAllBytes(file.path)
      }
      val js: JsValue = JsonParser(bytes)
      js.convertTo[T]
    } catch {
      case t: Throwable =>
        throw new IllegalArgumentException(s"Failed to read json from ${file.path}", t)
    } finally {
      if (zIn ne null) zIn.close()
    }
  }

  def storeJsonAtomically[T: JsonFormat](
      file: JsonAsset,
      value: T,
      replaceIfExists: Boolean,
      zip: Boolean = true): Unit = {
    def store(tmp: Path): Unit = {
      withTempJson(value) { sb =>
        if (zip)
          writeZip(FileAsset(tmp), sb)
        else {
          val writer = Files.newBufferedWriter(tmp, UTF_8)
          try writer.append(sb)
          finally writer.close()
        }
      }
    }
    // If we're not replacing, then call atomicallyWriteIfMissing to save unnecessary writes to the temp file
    if (replaceIfExists) atomicallyWrite(file, replaceIfExists = true)(store)
    else atomicallyWriteIfMissing(file)(store)
  }

  private val jsonStringBuilderCache = new ThreadLocal[java.lang.StringBuilder] {
    override def initialValue(): java.lang.StringBuilder = new java.lang.StringBuilder
  }

  /** @param jsonIn a consumer of a temporary [[CharSequence]] containing the JSON. Don't hold on to it. */
  def withTempJson[T: JsonFormat, U](value: T)(jsonIn: CharSequence => U): U = {
    val sb = jsonStringBuilderCache.get()
    sb.setLength(0)
    PrettyPrinter.print(value.toJson, sb)
    sb.append("\n")
    jsonIn(sb)
  }
}
