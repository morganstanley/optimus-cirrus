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

import msjava.msnet.MSNetTCPSocketBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import java.nio.ByteBuffer;

import static javax.net.ssl.SSLEngineResult.HandshakeStatus.*;

/**
 * This class handles logic responsible to perform ssl handshake. More details in the doHandshake
 * method's documentation.
 */
public class SSLHandshaker {
  private static final Logger LOGGER = LoggerFactory.getLogger(SSLHandshaker.class);

  private static final ByteBuffer EMPTY_APP_DATA_BUFFER = ByteBuffer.allocate(0);

  private boolean handshakeStarted = false;

  private final SSLEngine engine;

  private ByteBuffer outgoingHandshakeData;
  private ByteBuffer incomingHandshakeData;
  private ByteBuffer peerAppData;

  private MSNetTCPSocketBuffer outputBuffer = new MSNetTCPSocketBuffer();

  public SSLHandshaker(SSLEngine engine) {
    this.engine = engine;
  }

  /**
   * SSL handshake consists of several data exchanges between server and client. Read is represented
   * by unwrap and write by wrap.
   *
   * <p>Writes and reads may be followed by some tasks that need to be performed in a separate
   * thread.
   *
   * <p>
   *
   * <p>Typical flow of the SSLHandshake: Client SSL/TLS Message HandshakeStatus wrap() ClientHello
   * NEED_UNWRAP unwrap() ServerHello/Cert/ServerHelloDone NEED_WRAP wrap() ClientKeyExchange
   * NEED_WRAP wrap() ChangeCipherSpec NEED_WRAP wrap() Finished NEED_UNWRAP unwrap()
   * ChangeCipherSpec NEED_UNWRAP unwrap() Finished FINISHED
   *
   * <p>More info about the protocol and java implementation:
   * https://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/JSSERefGuide.html#SSLEngine
   */
  public boolean doHandshake(MSNetTCPSocketBuffer netData) throws Exception {
    beginHandshake();

    SSLEngineResult.HandshakeStatus hs = unwrapWithSubsequentTasks(netData);
    if (hs == SSLEngineResult.HandshakeStatus.FINISHED) {
      return true;
    }

    hs = wrapWithSubsequentTasks();
    if (hs == SSLEngineResult.HandshakeStatus.FINISHED) {
      return true;
    }

    LOGGER.trace("Continuing with handshake");
    return false;
  }

  private void beginHandshake() throws SSLException {
    if (!handshakeStarted) {
      engine.beginHandshake();
      handshakeStarted = true;

      allocateBuffers();
    }
  }

  private void allocateBuffers() {
    SSLSession session = engine.getSession();
    outgoingHandshakeData = ByteBuffer.allocate(session.getPacketBufferSize());
    incomingHandshakeData = ByteBuffer.allocate(session.getPacketBufferSize());
    peerAppData = ByteBuffer.allocate(session.getApplicationBufferSize());
  }

  /** This function simply reads incoming data and changes ssl engine state. */
  private SSLEngineResult.HandshakeStatus unwrapWithSubsequentTasks(MSNetTCPSocketBuffer netData)
      throws SSLException {
    SSLEngineResult.HandshakeStatus hs = engine.getHandshakeStatus();

    boolean bufferUnderflow = false;

    // Because we send batched handshake data it might be needed to do few subsequent unwraps.
    while ((hs == NEED_UNWRAP || hs == NEED_TASK) && !bufferUnderflow) {

      writeToHandshakeBufferAsMuchAsPossible(netData, incomingHandshakeData);

      switch (hs) {
        case NEED_UNWRAP:

          // Process incoming handshaking data
          incomingHandshakeData.flip();
          // Although peerAppData is a non empty buffer we don't expect at this point any user data.
          SSLEngineResult sslEngineResult = engine.unwrap(incomingHandshakeData, peerAppData);
          hs = sslEngineResult.getHandshakeStatus();
          incomingHandshakeData.compact();

          SSLEngineResult.Status status = sslEngineResult.getStatus();
          switch (status) {
            case OK:
              break;

            case BUFFER_OVERFLOW:
              throw new IllegalStateException(
                  "Application data is not supposed to be exchanged at this point");

            case BUFFER_UNDERFLOW:
              // Break the loop and wait for more data
              bufferUnderflow = true;
              break;
          }
          break;
        case NEED_TASK:
          hs = handleTask();
          break;
      }
    }
    return hs;
  }

  /**
   * Since handshake data might be larger than max incomingHandshakeData(16k) size we have to split
   * it and pass to the sslEngine in chunks.
   */
  private void writeToHandshakeBufferAsMuchAsPossible(
      MSNetTCPSocketBuffer netData, ByteBuffer incomingHandshakeData) {
    int bytesToRetrieve = Math.min(incomingHandshakeData.remaining(), netData.size());
    incomingHandshakeData.put(netData.retrieve(bytesToRetrieve));
  }

  /** This functions writes data to the outgoingHandshakeData buffer. */
  private SSLEngineResult.HandshakeStatus wrapWithSubsequentTasks() throws SSLException {
    SSLEngineResult.HandshakeStatus hs = engine.getHandshakeStatus();

    // It might be needed to do 3 subsequent wraps and then send them in batch.
    while (hs == NEED_WRAP || hs == NEED_TASK) {
      switch (hs) {
        case NEED_WRAP:
          outgoingHandshakeData.clear();

          // Generate handshaking data but no user data to send.
          SSLEngineResult sslEngineResult =
              engine.wrap(EMPTY_APP_DATA_BUFFER, outgoingHandshakeData);
          hs = sslEngineResult.getHandshakeStatus();

          switch (sslEngineResult.getStatus()) {
            case OK:
              outgoingHandshakeData.flip();
              outputBuffer.store(outgoingHandshakeData);
              break;

            case BUFFER_OVERFLOW:
              throw new IllegalStateException("Buffer overflow should not happen during wrap");

            case BUFFER_UNDERFLOW:
              throw new IllegalStateException("Buffer underflow should not happen during wrap");
          }
          break;
        case NEED_TASK:
          hs = handleTask();
          break;
      }
    }

    return hs;
  }

  public MSNetTCPSocketBuffer getOutputBuffer() {
    return outputBuffer;
  }

  private SSLEngineResult.HandshakeStatus handleTask() {
    Runnable task;
    while ((task = engine.getDelegatedTask()) != null) {
      task.run();
    }

    return engine.getHandshakeStatus();
  }

  public void cleanupBuffers() {
    outgoingHandshakeData = null;
    incomingHandshakeData = null;
    peerAppData = null;
    handshakeStarted = false;
  }
}
