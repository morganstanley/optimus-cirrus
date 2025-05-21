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

package msjava.msnet;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.security.cert.CertificateException;
import msjava.msnet.auth.MSNetAuthContext;
import msjava.msnet.auth.MSNetAuthStatus;
import msjava.msnet.ssl.SSLEncryptor;
import msjava.msnet.ssl.SSLEncryptorResult;
import msjava.msnet.ssl.SSLHandshaker;
import msjava.msnet.ssl.verification.CertificateVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class MSNetSSLSocket extends MSNetTCPSocket {
    private static final Logger LOGGER = LoggerFactory.getLogger(MSNetSSLSocket.class);
    private enum SSLSocketState {HANDSHAKE_NEEDED, HANDSHAKE_COMPLETED, READY_FOR_ENCRYPTING}
    private final SSLEngine sslEngine;
    private final SSLEncryptor sslEncryptor;
    private final SSLHandshaker sslHandshaker;
    private final CertificateVerifier certificateVerifier;
    private MSNetTCPSocketBuffer encryptedBufferToWrite = new MSNetTCPSocketBuffer();
    private final MSNetTCPSocketBuffer tmpReadBuffer = new MSNetTCPSocketBuffer();
    private SSLSocketState socketState = SSLSocketState.HANDSHAKE_NEEDED;
    MSNetSSLSocket(MSNetTCPSocketImpl impl, MSNetTCPSocketFactory parentFactory, SSLEngine sslEngine) {
        this(impl, parentFactory, sslEngine, new SSLEncryptor(sslEngine));
    }
    MSNetSSLSocket(MSNetTCPSocketImpl impl, MSNetTCPSocketFactory parentFactory, SSLEngine sslEngine, SSLEncryptor sslEncryptor) {
        super(impl, parentFactory);
        this.sslEngine = sslEngine;
        this.sslEncryptor = sslEncryptor;
        this.sslHandshaker = new SSLHandshaker(sslEngine);
        this.certificateVerifier = new CertificateVerifier(sslEngine);
    }
    @Override
    
    public MSNetIOStatus read(MSNetTCPSocketBuffer destBuffer) {
        
        if (socketState != SSLSocketState.READY_FOR_ENCRYPTING) {
            return super.read(destBuffer);
        }
        MSNetIOStatus result = super.read(tmpReadBuffer);
        boolean readBytes = result.getNumBytesProcessed() > 0;
        
        if (readBytes) {
            decryptAndUpdateIOResult(tmpReadBuffer, destBuffer, result);
        }
        return result;
    }
    @Override
    public MSNetIOStatus write(MSNetTCPSocketBuffer buf) {
        return write(Collections.singletonList(buf));
    }
    @Override
    public MSNetIOStatus write(List<MSNetTCPSocketBuffer> bufs) {
        
        if (socketState != SSLSocketState.READY_FOR_ENCRYPTING) {
            MSNetIOStatus write = super.write(bufs);
            if (isLastHandshakeMessageSucessfullyWritten(write)) {
                setReadyForEncrypting();
            }
            return write;
        }
        int bytesWrittenSum = 0;
        MSNetIOStatus status = new MSNetIOStatus();
        for (MSNetTCPSocketBuffer unencryptedBuf : bufs) {
            copyAndEncrypt(unencryptedBuf, encryptedBufferToWrite);
            status = writeBuffer(encryptedBufferToWrite);
            boolean fullyWritten = encryptedBufferToWrite.size() == 0;
            if (fullyWritten) {
                
                
                bytesWrittenSum += unencryptedBuf.size();
            }
            
            
            
            if (!fullyWritten || status.inError()) {
                break;
            }
        }
        
        status.setNumBytesProcessed(bytesWrittenSum);
        return status;
    }
    private void copyAndEncrypt(MSNetTCPSocketBuffer rawBuffer, MSNetTCPSocketBuffer encryptedBuffer) {
        
        
        boolean noPreviousMessage = encryptedBuffer.size() == 0;
        if (noPreviousMessage && rawBuffer.size() != 0) {
            encryptedBuffer.store(rawBuffer.peek());
            encrypt(encryptedBuffer);
        }
    }
    private MSNetIOStatus writeBuffer(MSNetTCPSocketBuffer buf) {
        MSNetIOStatus write = super.write(buf);
        buf.processed(write.getNumBytesProcessed());
        return write;
    }
    private void encrypt(MSNetTCPSocketBuffer buf) {
        if (socketState == SSLSocketState.READY_FOR_ENCRYPTING && buf.size() != 0) {
            SSLEncryptorResult encrypt = sslEncryptor.encrypt(buf);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(
                        "Buffer initial size: " + encrypt.getBytesConsumed() + ", encrypted buffer size: " + encrypt.getBytesProduced());
            }
        }
    }
    
    private void decryptAndUpdateIOResult(MSNetTCPSocketBuffer originBuffer, MSNetTCPSocketBuffer destBuffer,
                                          MSNetIOStatus result) {
        int originBufferSize = originBuffer.size();
        SSLEncryptorResult decrypted = sslEncryptor.decrypt(originBuffer, destBuffer);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(
                    "Decrypted: " + decrypted.getBytesProduced() + ", buffer still contains: " + originBuffer.size() + " " +
                            "encrypted bytes");
        }
        
        result.setNumBytesInMessage(originBufferSize);
        result.setNumBytesProcessed(decrypted.getBytesProduced());
    }
    private boolean isLastHandshakeMessageSucessfullyWritten(MSNetIOStatus write) {
        return socketState == SSLSocketState.HANDSHAKE_COMPLETED && write.getNumBytesInMessage() == write.getNumBytesProcessed();
    }
    public MSNetTCPSocketBuffer getOutputBuffer() {
        return sslHandshaker.getOutputBuffer();
    }
    public boolean doHandshake(MSNetTCPSocketBuffer netData) throws Exception {
        return sslHandshaker.doHandshake(netData);
    }
    public boolean verifyCertificates()
            throws CertificateException, SSLException, java.security.cert.CertificateException {
        return certificateVerifier.verify(this.getAddress().getHost());
    }
    private String getUserIdFromPrincipalName(String pname) {
        return pname.split(",")[0].substring("CN=".length()).split("@")[0];
    }
    public MSNetAuthContext getAuthContext() throws SSLPeerUnverifiedException {
        String authMechanism = "SSL";
        String peerUserId = getUserIdFromPrincipalName(sslEngine.getSession().getPeerPrincipal().getName());
        MSNetAuthStatus status = new MSNetAuthStatus(MSNetAuthStatus.Authenticated);
        return new MSNetAuthContext(status, peerUserId, peerUserId, authMechanism);
    }
    public void setReadyForEncrypting() {
        socketState = SSLSocketState.READY_FOR_ENCRYPTING;
        
        sslHandshaker.cleanupBuffers();
    }
    public void setHandshakeCompleted() {
        socketState = SSLSocketState.HANDSHAKE_COMPLETED;
    }
    @Override
    public void close() throws IOException {
        super.close();
        sslEngine.closeOutbound();
    }
}
