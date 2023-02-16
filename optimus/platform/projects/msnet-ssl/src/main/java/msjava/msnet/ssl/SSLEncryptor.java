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

package msjava.msnet.ssl;

import static javax.net.ssl.SSLEngineResult.Status.BUFFER_UNDERFLOW;

import java.nio.ByteBuffer;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;

import msjava.msnet.MSNetByteBufferManager;
import msjava.msnet.MSNetTCPSocketBuffer;

/** This class uses SSLEngine for encrypting/decrypting incoming buffer. */
public class SSLEncryptor {

  private static final int ENCRYPT_BUFFER_SIZE =
      Integer.parseInt(System.getProperty("msjava.msnet.ssl.encryptBufferSize", "-1"));
  private static final int DECRYPT_BUFFER_SIZE =
      Integer.parseInt(System.getProperty("msjava.msnet.ssl.decryptBufferSize", "-1"));

  private final SSLEngine sslEngine;
  private final boolean slicingBuffers;

  /**
   * SSL Engine encrypts and decrypts data in chunks. There are two local buffers that are reused
   * for that.
   *
   * <p>* Bytes are consumed in chunks from the input buffer and the result is inserted to the one
   * of corresponding encryptedBytesBuffer/decryptedBytesBuffer. * Then content of these buffers is
   * copied to the result buffer. * This repeats until input buffer is empty.
   */
  private ByteBuffer encryptedBytesBuffer;

  private ByteBuffer decryptedBytesBuffer;

  public SSLEncryptor(SSLEngine sslEngine, boolean slicingBuffers) {
    this(
        sslEngine,
        ENCRYPT_BUFFER_SIZE > 0
            ? ENCRYPT_BUFFER_SIZE
            : sslEngine.getSession().getPacketBufferSize(),
        DECRYPT_BUFFER_SIZE > 0
            ? DECRYPT_BUFFER_SIZE
            : sslEngine.getSession().getApplicationBufferSize(),
        slicingBuffers);
  }

  public SSLEncryptor(
      SSLEngine sslEngine, int encryptBufferSize, int decryptBufferSize, boolean slicingBuffers) {
    this.sslEngine = sslEngine;
    this.slicingBuffers = slicingBuffers;
    this.encryptedBytesBuffer = ByteBuffer.allocate(encryptBufferSize);
    this.decryptedBytesBuffer = ByteBuffer.allocate(decryptBufferSize);
  }

  /**
   * Buffer passed as an argument is overwritten with encrypted data and returned as a part of
   * SSLEncryptorResult.
   */
  public SSLEncryptorResult encrypt(MSNetTCPSocketBuffer buf) {
    ByteBuffer bytesToEncrypt = MSNetByteBufferManager.getInstance().getBuffer(buf.size(), false);
    bytesToEncrypt.put(buf.retrieve());
    buf.clear();

    MSNetTCPSocketBuffer resultBuffer = buf;

    SSLEngineResult result;
    bytesToEncrypt.flip();
    int bytesConsumed = 0;
    do {
      int n = encryptedBytesBuffer.remaining() / 2;
      if (n == 0) n = encryptedBytesBuffer.remaining();
      if (!slicingBuffers || bytesToEncrypt.remaining() <= n) {
        result = encryptChunk(bytesToEncrypt, encryptedBytesBuffer);
        addToBuffer(encryptedBytesBuffer, resultBuffer);
      } else {
        ByteBuffer slice = bytesToEncrypt.slice();
        slice.limit(n);
        result = encryptChunk(slice, encryptedBytesBuffer);
        bytesToEncrypt.position(bytesToEncrypt.position() + n);
        addToBuffer(encryptedBytesBuffer, resultBuffer);
      }
      bytesConsumed += result.bytesConsumed();
    } while (bytesToEncrypt.hasRemaining() && result.bytesConsumed() != 0);

    return SSLEncryptorResult.success(resultBuffer.size(), bytesConsumed);
  }

  /**
   * We consume everything from the buffer til its empty or buffer underflow occurs. In case of
   * buffer underflow we return only that piece of data that we managed to decrypt.
   *
   * <p>Returns buffer with decrypted data and statistics of the bytes consumed/produced.
   */
  public SSLEncryptorResult decrypt(
      MSNetTCPSocketBuffer originBuffer, MSNetTCPSocketBuffer destBuffer) {
    ByteBuffer bytesToDecrypt =
        MSNetByteBufferManager.getInstance().getBuffer(originBuffer.size(), false);
    bytesToDecrypt.put(originBuffer.peek());

    int bytesConsumed = 0;
    int bytesProduced = 0;
    SSLEngineResult result;
    bytesToDecrypt.flip();
    do {
      result = decryptChunk(bytesToDecrypt, decryptedBytesBuffer);
      if (result.getStatus() == BUFFER_UNDERFLOW) {
        return SSLEncryptorResult.bufferUnderflow(bytesProduced, bytesConsumed);
      }
      originBuffer.retrieve(result.bytesConsumed());
      bytesConsumed += result.bytesConsumed();
      bytesProduced += result.bytesProduced();
      addToBuffer(decryptedBytesBuffer, destBuffer);
    } while (bytesToDecrypt.hasRemaining() && result.bytesConsumed() != 0);

    return SSLEncryptorResult.success(bytesProduced, bytesConsumed);
  }

  private SSLEngineResult encryptChunk(ByteBuffer bytesToEncrypt, ByteBuffer encryptToBuffer) {
    SSLEngineResult result;

    try {
      result = sslEngine.wrap(bytesToEncrypt, encryptToBuffer);
    } catch (SSLException e) {
      throw new IllegalStateException("Unexpected exception during SSLEngine.warp", e);
    }

    switch (result.getStatus()) {
      case OK:
        break;
      case BUFFER_OVERFLOW:
        // encryptToBuffer is flushed after each iteration - because of that buffer overflow should
        // not happen
        throw new IllegalStateException("Buffer overflow should not occur after wrap.");
      case BUFFER_UNDERFLOW:
        throw new IllegalStateException("Buffer underflow occurred after a wrap");
      case CLOSED:
        throw new IllegalStateException("The sslEngine is closed when encrypting data");
      default:
        throw new IllegalStateException(
            "Invalid SSL status: " + result.getStatus() + " during encryption");
    }
    return result;
  }

  private SSLEngineResult decryptChunk(ByteBuffer bytesToDecrypt, ByteBuffer decryptToBuffer) {
    SSLEngineResult result;

    try {
      result = sslEngine.unwrap(bytesToDecrypt, decryptToBuffer);
    } catch (SSLException e) {
      throw new IllegalStateException("Unexpected exception during SSLEngine.unwarp", e);
    }

    switch (result.getStatus()) {
      case OK:
        break;
      case BUFFER_UNDERFLOW:
        break;
      case BUFFER_OVERFLOW:
        // decryptToBuffer is flushed after each iteration - because of that buffer overflow should
        // not happen
        throw new IllegalStateException("buffer overflow should not occur.");
      case CLOSED:
        throw new IllegalStateException("The sslEngine is closed when decrypting data");
    }

    return result;
  }

  private void addToBuffer(ByteBuffer srcBuffer, MSNetTCPSocketBuffer destBuffer) {
    srcBuffer.flip();
    destBuffer.store(srcBuffer);
    srcBuffer.clear();
  }
}
