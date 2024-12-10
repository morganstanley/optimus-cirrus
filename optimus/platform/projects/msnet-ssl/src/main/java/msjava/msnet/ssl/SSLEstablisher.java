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

import javax.annotation.Nullable;

import com.google.common.base.Stopwatch;
import msjava.msnet.MSNetAbstractTCPServer;
import msjava.msnet.MSNetEstablishStatus;
import msjava.msnet.MSNetEstablisher;
import msjava.msnet.MSNetEstablisherFactory;
import msjava.msnet.MSNetSSLSocket;
import msjava.msnet.MSNetSSLSocketFactory;
import msjava.msnet.MSNetTCPConnection;
import msjava.msnet.MSNetTCPSocket;
import msjava.msnet.MSNetTCPSocketBuffer;
import msjava.msnet.MSNetTCPSocketFactory;
// import msjava.msnet.utils.MSNetConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SSLEstablisher is used to to coordinate and supervise SSL handshake performed by {@link
 * MSNetSSLSocket}
 *
 * <p>
 *
 * <p>
 *
 * <h3>Steps needed to set up SSL connection between server and client:</h3>
 *
 * <ul>
 *   <li>Create respective {@link MSNetSSLSocketFactory} for both server and client connection.
 *   <li>Use {@link SSLEngineConfig} to configure {@link MSNetSSLSocketFactory}. For more info refer
 *       to the {@link SSLEngineConfig} doc.
 *   <li>Use {@link MSNetAbstractTCPServer#setSocketFactory(MSNetTCPSocketFactory)} to inject
 *       previously created {@link MSNetSSLSocketFactory}
 *   <li>Use {@link MSNetTCPConnection#setSocketFactory(MSNetTCPSocketFactory)} to set {@link
 *       MSNetSSLSocketFactory} for the client connection.
 *   <li>{@link MSNetSSLSocketFactory} can also be globally set using {@link
 *       MSNetConfiguration#setDefaultMSNetTCPSocketFactory(MSNetTCPSocketFactory)}
 *   <li>Use {@link MSNetAbstractTCPServer#addEstablisherFactory(MSNetEstablisherFactory)} to pass
 *       there {@link SSLEstablisherFactory}
 *   <li>Create {@SSLEstablisher} object using preferably {@link SSLEstablisherFactory} and pass it
 *       to the client connection {@link MSNetTCPConnection#addEstablisher(MSNetEstablisher)}
 * </ul>
 *
 * <h3>Code example. Server configuration:</h3>
 *
 * <pre>
 *     SSLEngineConfig serverSSLConfig = new SSLEngineConfig()
 *          .withKeyPassword(keyPassword)
 *          .withKeystorePassword(keystorePassword)
 *          .withTruststorePassword(trustStorePassword)
 *          .withClientAuthEnabled(true)
 *          .withKeystorePath(keystorePath)
 *          .withTruststorePath(truststorePath);
 *
 *     MSNetSSLSocketFactory serverSocketFactory = new MSNetSSLSocketFactory(sslServerConfig);
 *     server = new MSNetThreadPoolTCPServer(serverLoop, new MSNetInetAddress("localhost:0"), new MSNetID("test ssl server"));
 *     server.addEstablisherFactory(new SSLEstablisherFactory());
 *     server.setSocketFactory(serverSocketFactory);
 *     server.start();
 * </pre>
 *
 * <h3>Code example. Client connection configuration:</h3>
 *
 * <pre>
 *     SSLEngineConfig clientSSLConfig = new SSLEngineConfig()
 *          .withKeyPassword(keyPassword)
 *          .withKeystorePassword(keystorePassword)
 *          .withTruststorePassword(trustStorePassword)
 *          .withKeystorePath(keystorePath)
 *          .withTruststorePath(truststorePath);
 *
 *     MSNetSSLSocketFactory clientSocketFactory = new MSNetSSLSocketFactory(clientSSLConfig);
 *     clientConnection = new MSNetTCPConnection(clientLoop, serverAddress, "test ssl connection");
 *     clientConnection.addEstablisher(new SSLEstablisherFactory().createEstablisher());
 *     clientConnection.setSocketFactory(clientSocketFactory);
 * </pre>
 *
 * <h3>Library limitations</h3>
 *
 * <ul>
 *   <li>Session renegotiation is not supported
 *   <li>Sending user data during handshake is not supported
 * </ul>
 *
 * <h3>Support</h3>
 *
 * <ul>
 *   <li>This is an experimental library created by Optimus/DAL team and therefore is not supported
 *       by msjava team.
 *   <li>If you have any question regarding this particular library contact the DAL support mailing
 *       list.
 * </ul>
 */
public class SSLEstablisher extends MSNetEstablisher {
  private static final Logger LOGGER = LoggerFactory.getLogger(SSLEstablisher.class);

  /** Must be higher than MSNetSOCKS4Establisher.DEFAULT_ESTABLISH_PRIORITY */
  public static final int DEFAULT_ESTABLISHER_PRIORITY = 1100;

  private static final String ESTABLISHER_NAME = "SSL Establisher";

  private final boolean encryptionOnly;
  // The hostname which the client initiates connections to. It might differ from the hostname
  // returned by reverse
  // DNS because of DNS CNAME pointers, load balancers, service discovery methods. We need to check
  // the original
  // hostname to protect against hijack.
  @Nullable private final String serviceHostname;

  private MSNetTCPConnection conn;
  private MSNetSSLSocket sslSocket;
  private MSNetEstablishStatus status = MSNetEstablishStatus.UNKNOWN;

  private boolean isServerSide;

  private Stopwatch stopwatch = Stopwatch.createUnstarted();

  SSLEstablisher() {
    this(DEFAULT_ESTABLISHER_PRIORITY, false, null);
  }

  public SSLEstablisher(int priority, boolean encryptionOnly, @Nullable String serviceHostname) {
    super(priority);
    this.encryptionOnly = encryptionOnly;
    this.serviceHostname = serviceHostname;
  }

  @Override
  public void init(boolean isServerSide, MSNetTCPConnection conn) {
    this.isServerSide = isServerSide;
    this.conn = conn;
  }

  @Override
  public MSNetEstablishStatus establish(MSNetTCPSocketBuffer readBuf) {
    LOGGER.debug("Trying to perform handshake");

    try {
      startStopwatch();
      sslSocket = getSocketFromConnection(conn);
      return doHandshake(readBuf);
    } catch (Exception e) {
      LOGGER.error("Could not establish handshake", e);
      return MSNetEstablishStatus.FAILURE;
    }
  }

  private MSNetEstablishStatus doHandshake(MSNetTCPSocketBuffer readBuf) throws Exception {
    status = handshake(readBuf);
    if (status == MSNetEstablishStatus.COMPLETE) {
      if (isServerSide) {
        // Server side is actually the last one to send handshake data
        // Because of that there is a intermediate state that is changed on the socket when last
        // piece of handshake data is send.
        sslSocket.setHandshakeCompleted();
      } else {
        sslSocket.setReadyForEncrypting();
      }

      if (!encryptionOnly) {
        conn.setAuthContext(sslSocket.getAuthContext());
      }
      LOGGER.info(
          "Successful handshake with {}, it took {}", conn.getAuthContext(), stopwatch.stop());
    }

    return status;
  }

  private MSNetEstablishStatus handshake(MSNetTCPSocketBuffer readBuf) throws Exception {
    if (!sslSocket.doHandshake(readBuf)) {
      return MSNetEstablishStatus.CONTINUE;
    }

    if (!sslSocket.verifyCertificates(encryptionOnly, serviceHostname)) {
      LOGGER.error("Certificate validation failed. Failed to establish connection.");
      return MSNetEstablishStatus.FAILURE;
    }

    return MSNetEstablishStatus.COMPLETE;
  }

  private void startStopwatch() {
    if (!stopwatch.isRunning()) {
      stopwatch.start();
    }
  }

  private MSNetSSLSocket getSocketFromConnection(MSNetTCPConnection conn) {
    MSNetTCPSocket socket = conn.getSocket();
    if (socket instanceof MSNetSSLSocket) {
      return (MSNetSSLSocket) socket;
    }

    throw new IllegalArgumentException(
        "Connection is not a SSL Connection. Cannot extract SSLEngine from the socket.");
  }

  @Override
  public void cleanup() {
    status = MSNetEstablishStatus.UNKNOWN;
    stopwatch.reset();
  }

  @Override
  public MSNetEstablishStatus getStatus() {
    return status;
  }

  @Override
  public String getEstablisherName() {
    return ESTABLISHER_NAME;
  }

  @Override
  public MSNetTCPSocketBuffer getOutputBuffer() {
    LOGGER.debug("isServer=" + isServerSide + ". Sending " + sslSocket.getOutputBuffer());
    return sslSocket.getOutputBuffer();
  }
}
