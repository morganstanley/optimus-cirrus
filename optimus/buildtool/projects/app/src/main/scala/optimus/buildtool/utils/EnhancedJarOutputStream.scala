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

import java.io.BufferedOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.util.jar
import java.util.jar.JarFile
import java.util.jar.JarOutputStream
import java.util.zip.Deflater

import optimus.buildtool.files.RelativePath
import org.apache.commons.io.input.CharSequenceInputStream

import scala.collection.mutable

abstract class EnhancedJarOutputStream(outputStream: OutputStream, manifest: Option[jar.Manifest], compressed: Boolean)
    extends OutputStream {

  protected val jarOutputStream = new JarOutputStream(new BufferedOutputStream(outputStream))
  // We avoid compressing certain jars that we know we'll be rereading and rewriting a lot to avoid the overhead of
  // deflation and inflation. Note that even when compression is disabled we still use the Deflater because it computes
  // the mandatory CRC32 checksum for us (otherwise ZipOutputStream expects us to do that and pass it in ourselves).
  jarOutputStream.setLevel(if (compressed) Deflater.BEST_COMPRESSION else Deflater.NO_COMPRESSION)
  private val directoriesCreated: mutable.Set[String] = mutable.Set[String]()

  manifest.foreach { mf =>
    require(
      mf.getMainAttributes.containsKey(jar.Attributes.Name.MANIFEST_VERSION),
      s"Manifest must contain a Manifest-Version")
    // The manifest has to be the first entry in the file otherwise JarInputStream doesn't read it.
    // We write it ourselves rather than using a JarOutputStream so that we can hash it.
    putNextEntry(JarFile.MANIFEST_NAME)
    // Note that Manifest#write outputs CRLF, but when we hash text files we always convert to LF. To ensure that the
    // hash we compute when writing directly to Jar matches what we'd produce if we rehashed the file, we need
    // to write the file with LF line endings
    Jars.writeManifestToStreamWithLf(mf, this)
  }

  protected def putNextEntry(entryName: String): Unit = {
    // get all of the parent dirs of the entry, starting with the parent-most (excluding root)
    entryName.split('/').inits.toSeq.tail.init.reverse.foreach { prefix =>
      val prefixStr = prefix.mkString("", "/", "/")
      // create entries for any missing ones
      if (directoriesCreated.add(prefixStr)) {
        jarOutputStream.putNextEntry(Jars.zipEntry(prefixStr))
      }
    }

    val ze = Jars.zipEntry(entryName)
    jarOutputStream.putNextEntry(ze)
  }

  override def write(b: Int): Unit = {
    // (Yeah, OutputStream#write accepts an Int but writes it as a byte... I don't know why either)
    jarOutputStream.write(b)
  }

  override def write(bytes: Array[Byte], offset: Int, len: Int): Unit = {
    jarOutputStream.write(bytes, offset, len)
  }

  def copyInFile(inputFile: Path, targetName: RelativePath): Unit = {
    val inStream = Files.newInputStream(inputFile)
    try copyInFile(inStream, targetName)
    finally inStream.close()
  }

  def copyInFile(inputStream: InputStream, targetName: RelativePath): Unit = {
    putNextEntry(targetName.pathString)
    val buffer = new Array[Byte](4096)
    var len = inputStream.read(buffer)
    while (len != -1) {
      write(buffer, 0, len)
      len = inputStream.read(buffer)
    }
  }

  def writeFile(data: CharSequence, targetName: RelativePath): Unit = {
    copyInFile(new CharSequenceInputStream(data, StandardCharsets.UTF_8), targetName)
  }

  def writeFile(data: String, targetName: RelativePath, tokens: Map[String, String]): Unit = {
    val newContent = tokens.foldLeft(data) { case (c, (key, value)) =>
      c.replace(s"@$key@", value)
    }
    writeFile(newContent, targetName)
  }

  override def close(): Unit = jarOutputStream.close()
}

class UnhashedJarOutputStream(
    outputStream: OutputStream,
    manifest: Option[jar.Manifest],
    compressed: Boolean
) extends EnhancedJarOutputStream(outputStream, manifest, compressed)
