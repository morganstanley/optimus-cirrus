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
package optimus.platform.tls.pkcs12file

import optimus.platform.tls.simple.KeyStoreReader
import java.io.ByteArrayOutputStream

import java.io.FileInputStream
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.attribute.PosixFilePermission
import java.security.KeyStore
import java.util
import scala.jdk.CollectionConverters._
import scala.util.Try

/**
 * A key store reader for local static PKCS12 files.
 *
 * @param path
 *   the file path.
 * @param askpass
 *   the command that outputs a passphrase to stdout.
 *
 * <p>Example commands for static passphrase: </p>
 *
 * <ul> <li>Windows: <pre>Write-Host PASSPHRASE -NoNewLine</pre></li>
 *
 * <li>POSIX: <pre>echo -n PASSPHRASE</pre></li> </ul>
 */
private class Pkcs12FileKeyStoreReader private (private val path: String, private val askpass: String)
    extends KeyStoreReader {
  override def read(): (KeyStore, Option[Array[Char]]) = {
    val keyStore = KeyStore.getInstance("PKCS12")
    var passphrase: Array[Char] = null

    val filePath = Paths.get(path)
    Try(Files.getPosixFilePermissions(filePath))
      .map(Some(_))
      .recover({ case _: UnsupportedOperationException => None })
      .get
      .map(_.asScala)
      .foreach { permissions =>
        require(
          (permissions -- Set(PosixFilePermission.OWNER_READ, PosixFilePermission.GROUP_READ)).isEmpty,
          "The permission of PKCS12 key store file must be masked by 0330"
        )
      }
    val fileStream = new FileInputStream(filePath.toFile)
    var closed = false
    try {
      passphrase = {
        val isWindows = Set("Windows 10") contains System.getProperty("os.name")

        import scala.sys.process._
        val passphraseBuffer = new ByteArrayOutputStream
        val command =
          if (isWindows) Seq("powershell", askpass)
          else Seq("sh", "-c", askpass)
        val io = new ProcessIO(BasicIO.input(false), BasicIO.transferFully(_, passphraseBuffer), BasicIO.toStdErr)
        if (command.run(io).exitValue() != 0) throw new IllegalStateException("Failed to execute askpass")
        passphraseBuffer.flush()
        passphraseBuffer.close()
        passphraseBuffer.toByteArray.map(_.toChar)
      }

      keyStore.load(fileStream, passphrase)
    } catch {
      case throwable: Throwable =>
        closed = true
        try fileStream.close()
        catch { case exception: Exception => throwable.addSuppressed(exception) }
        throw throwable;
    } finally if (!closed) fileStream.close()

    (keyStore, Some(util.Arrays.copyOf(passphrase, passphrase.length)))
  }
}
private object Pkcs12FileKeyStoreReader {
  def apply(path: String, askpass: String): Pkcs12FileKeyStoreReader =
    new Pkcs12FileKeyStoreReader(path, askpass)
}
