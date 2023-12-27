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

import java.io.OutputStream
import java.nio.charset.StandardCharsets
import java.util.jar
import com.google.common.hash.Hasher

import scala.collection.compat._
import scala.collection.mutable

class DuplicateEntriesException(val files: Seq[String])
    extends IllegalArgumentException(s"Duplicate entries: ${files.mkString(", ")}")

/**
 * An improved JarOutputStream which hashes the files as they are written and then adds an order-independent hash file
 * at Hashing.optimusHashPath. Also creates all parent directories for added files automatically.
 */
//noinspection UnstableApiUsage
final class ConsistentlyHashedJarOutputStream(
    outputStream: OutputStream,
    manifest: Option[jar.Manifest],
    compressed: Boolean
) extends BaseConsistentlyHashedJarOutputStream(outputStream, manifest, compressed) {
  override protected def putNextEntry(entryName: String): Unit = {
    commitPreviousHash()
    isDuplicate = filesToHashes.contains(entryName)
    if (!isDuplicate) super.putNextEntry(entryName)
    fileHasherAndName = Some(entryName, Hashing.hashFunction.newHasher())
  }

  private def currentHasher: Hasher =
    fileHasherAndName.getOrElse(throw new IllegalArgumentException("you must call putNextEntry before write"))._2

  override def write(b: Int): Unit = {
    currentHasher.putByte(b.toByte)
    if (!isDuplicate) super.write(b)
  }

  override def write(bytes: Array[Byte], offset: Int, len: Int): Unit = {
    currentHasher.putBytes(bytes, offset, len)
    if (!isDuplicate) super.write(bytes, offset, len)
  }

  private def commitPreviousHash(): Unit = fileHasherAndName match {
    case Some((file, hasher)) =>
      val hash = s"HASH${hasher.hash()}"
      if (isDuplicate) duplicateFilesAndHashes += file -> hash
      else filesToHashes += file -> hash
      isDuplicate = false
      fileHasherAndName = None
    case None =>
  }

  override def close(): Unit = {
    try {
      commitPreviousHash()
      val nonMatchingDupes = duplicateFilesAndHashes
        .filter { case (file, hash) =>
          filesToHashes(file) != hash
        }
        .map(_._1)
      if (nonMatchingDupes.nonEmpty) throw new DuplicateEntriesException(nonMatchingDupes.to(Seq))

      val sortedFileHashes = filesToHashes.to(Seq).sorted
      putNextEntry(Hashing.optimusPerEntryHashPath)
      sortedFileHashes.foreach { case (file, hash) =>
        jarOutputStream.write(s"$file\t$hash\n".getBytes(StandardCharsets.UTF_8))
      }
      val hash = Hashing.consistentlyHashFileHashes(sortedFileHashes)
      putNextEntry(Hashing.optimusHashPath)
      jarOutputStream.write(hash.getBytes(StandardCharsets.UTF_8))
    } finally super.close()
  }
}

class BaseConsistentlyHashedJarOutputStream(
    outputStream: OutputStream,
    manifest: Option[jar.Manifest],
    compressed: Boolean,
    // declare these here as fields so that they're available to `EnhancedJarOutputStream` when it adds the manifest
    protected val filesToHashes: mutable.Map[String, String] = mutable.Map.empty,
    protected val duplicateFilesAndHashes: mutable.Buffer[(String, String)] = mutable.Buffer.empty,
    protected var isDuplicate: Boolean = false,
    protected var fileHasherAndName: Option[(String, Hasher)] = None
) extends EnhancedJarOutputStream(outputStream, manifest, compressed)
