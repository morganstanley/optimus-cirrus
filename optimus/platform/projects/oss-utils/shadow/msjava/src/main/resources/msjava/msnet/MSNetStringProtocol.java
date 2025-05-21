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
import msjava.msnet.auth.MSNetEncryptionException;
import msjava.msnet.auth.MSNetKerberosEncryptionMechanism;
import msjava.msnet.auth.MSNetEncryptor;
import msjava.msnet.internal.MSNetUtils;
import msjava.msnet.utils.MSNetConfiguration;
import msjava.tools.util.MSOctalEncoder;
import org.ietf.jgss.GSSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.annotations.VisibleForTesting;
public class MSNetStringProtocol implements MSNetProtocol, MSNetOutBoundProtocolWithMoveSupport {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(MSNetStringProtocol.class);
    
    public static final int DEFAULT_MAX_MESSAGE_SIZE = 209715200;
    
    private int maxMessageSize = MSNetConfiguration.getDefaultMaxMessageSize();
    static final int INTEGER_SIZE = 4; 
    private MSNetEncryptor encryptor;
    
    public boolean processBuffer(MSNetTCPSocketBuffer buffer, MSNetMessage message) throws MSNetProtocolException {
        
        
        MSNetStringMessage strmsg = (MSNetStringMessage) message;
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("EncryptorFlag={}, MsgDecryptedFlag={}, TCP buffer size={}, hex buf={}",
                    encryptor != null,
                    strmsg.isDecrypted(),
                    buffer.size(),
                    MSNetUtils.asHexString(buffer.peek()));
        }
        
        
        if (strmsg.isDecrypted() && strmsg.setToNextDecryptedMsg()) {
            return true;
        }
        
        int startbufsiz = buffer.size();
        while (!buffer.isEmpty()) {
            boolean ret = processFragment(buffer, strmsg);
            if (!ret) {
                
                return false;
            } else if (strmsg.isComplete()) {
                
                if (encryptor != null) {
                    processEncrypted(strmsg);
                }
                
                
                
                return true;
            }
            
            
            
            assert buffer.size() < startbufsiz : "processFragment didn't read any bytes!";
        }
        return false;
    }
    private void processEncrypted(MSNetStringMessage message) throws MSNetProtocolException {
        
        if (!message.isComplete()) {
            throw new MSNetProtocolException("encryption is not supported for fragments", false);
        }
        try {
            byte[] decryptedBytes = encryptor.decrypt(message.getBytes());
            message.setDecryptedBuffer(decryptedBytes);
            if (!message.setToNextDecryptedMsg()) {
                throw new MSNetProtocolException("Encrypted Message with size=" + decryptedBytes.length +
                        " malformed, String Header missing)", false);
            }
        } catch (MSNetEncryptionException e) {
            throw new MSNetProtocolException("Caught GSSException while decrypting msg: " + e.getMessage(), false);
        }
    }
    public boolean processBufferOnDisconnect(MSNetTCPSocketBuffer buffer, MSNetMessage message) {
        
        return false;
    }
    
    protected boolean processFragment(MSNetTCPSocketBuffer buffer, MSNetStringMessage message)
            throws MSNetProtocolException {
        int numleft = message.getBytesLeftInFragment();
        
        if (numleft == -1) {
            if (buffer.size < INTEGER_SIZE) {
                
                return false;
            }
            
            byte[] bytebuf = buffer.retrieve(INTEGER_SIZE);
            message.setComplete(bytebuf[0] >= 0);
            numleft = MSNetUtils.asInt((byte) (bytebuf[0] & ~0x80), bytebuf[1], bytebuf[2], bytebuf[3]);
            
            checkBadMessageSize(numleft);
            message.setBytesLeftInFragment(numleft);
            if (message.isComplete()) {
                message.setLengthFromHeader(message.getBytesLeftInFragment() + message.getLength());
            }
        } 
        if (buffer.size >= numleft){
            
            
            buffer.moveTo(message.getBuffer(), numleft, false); 
            message.setBytesLeftInFragment(-1);
            return true;
        } else {
            
            
            
            numleft -= buffer.size;
            int left = buffer.moveTo(message.getBuffer(), buffer.size, true);
            numleft += left;
            message.setBytesLeftInFragment(numleft);
            return false;
        }
    }
    
    @Deprecated
    public static void checkMessageSize(int size) throws MSNetProtocolException {
        checkProtocolErrors(size);
        String str = MSOctalEncoder.asMixedOctal(MSNetUtils.asBytes(size));
        throw new MSNetProtocolException("Message size too large. " + "Could not allocate " + size + " bytes ('" + str
                + "') for message.", true);
    }
    private static void checkProtocolErrors(int size) throws MSNetProtocolException {
        if (size == 1347375956) 
        {
            throw new MSNetProtocolException("An HTTP 'POST' message was received on a string protocol port.", true);
        } else if (size == 1195725856) 
        {
            throw new MSNetProtocolException("An HTTP 'GET' message was received on a string protocol port.", true);
        } else if (size == 1129337648) 
        {
            throw new MSNetProtocolException("A CPS message was received on a string protocol port.", true);
        } else if (size == 1010792557) 
        {
            throw new MSNetProtocolException(
                    "A raw XML message (possibly netAdmin) was received on a string protocol port.", true);
        } else if (size == 1296127045) 
        {
            throw new MSNetProtocolException("A kerberized request was received on a non-kerberos string protocol port.", true);
        }
    }
    @VisibleForTesting
    void checkBadMessageSize(int size) throws MSNetProtocolException {
        if (size > getMaxMessageSize()) {
            checkProtocolErrors(size);
            throw new MSNetProtocolException("Message size (" + size + " bytes) exceeds the predefined max size ("
                    + getMaxMessageSize() + " bytes.)", true);
        }
    }
    
    public MSNetTCPSocketBuffer[] createBuffers(MSNetMessage message) {
        return createBuffers(message, false);
    }
    @Override
    public MSNetTCPSocketBuffer[] createBuffers(MSNetMessage message, boolean consume) {
        MSNetStringMessage msg = (MSNetStringMessage) message;
        
        if (encryptor != null)
            return createEncryptedBuffer(msg);
        if (msg.getLengthFromHeader() != MSNetStringMessage.SIZE_OF_MSG_UNAVAILABLE) {
            return createBufferWithHeaderOnce(msg);
        }
        
        MSNetTCPSocketBuffer bufs[] = new MSNetTCPSocketBuffer[2];
        
        
        int i = msg.getLength();
        byte[] hdr = MSNetUtils.asBytes(i);
        if (!msg.isComplete()) {
            
            hdr[0] |= 0x80;
        }
        bufs[0] = new MSNetTCPSocketBuffer(hdr, 0, hdr.length, true);
        bufs[0].setIsLastBufferInMessage(false);
        if (msg.isComplete()) {
            
            
            
            
            bufs[1] = new MSNetTCPSocketBuffer(msg.getBuffer(), !consume);
        } else {
            
            bufs[1] = msg.getAvailableBytes(true);
        }
        bufs[1].setIsLastBufferInMessage(msg.isComplete());
        return bufs;
    }
    private MSNetTCPSocketBuffer[] createEncryptedBuffer(MSNetStringMessage message) {
        MSNetStringMessage msg = (MSNetStringMessage) message;
        
        
        if (msg.getLengthFromHeader() == MSNetStringMessage.SIZE_OF_MSG_UNAVAILABLE && !msg.isComplete()) {
            throw new IllegalArgumentException("encryption is not supported for fragments");
        }
        byte [] data = msg.getBytes();
        byte [] hdr = MSNetUtils.asBytes(data.length);
        byte [] fullRawBuffer = new byte[hdr.length + data.length];
        System.arraycopy(hdr, 0, fullRawBuffer, 0, hdr.length);
        System.arraycopy(data, 0, fullRawBuffer, hdr.length, data.length);
        try {
            byte [] encryptedBuffer = encryptor.encrypt(fullRawBuffer);
            MSNetTCPSocketBuffer[] outBuf = new MSNetTCPSocketBuffer[1];
            outBuf[0] = new MSNetTCPSocketBuffer();
            
            
            hdr = MSNetUtils.asBytes(encryptedBuffer.length);
            outBuf[0].store(hdr);
            outBuf[0].store(encryptedBuffer, 0, encryptedBuffer.length);
            outBuf[0].setIsLastBufferInMessage(true);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("created tcp buf len={}, encrypted buf len={}, hdr len={}, hex buf={}",
                        outBuf[0].size(), encryptedBuffer.length, hdr.length,
                        MSNetUtils.asHexString(outBuf[0].peek()));
            }
            return outBuf;
        } catch (MSNetEncryptionException e) {
            LOGGER.error("Encryption Failed", e);
            throw new IllegalArgumentException(e);
        }
    }
    
    private MSNetTCPSocketBuffer[] createBufferWithHeaderOnce(MSNetStringMessage message) {
        MSNetTCPSocketBuffer bufs[];
        MSNetTCPSocketBuffer headerBuffer = null;
        int i = message.getLengthFromHeader();
        
        if (i > 0) {
            bufs = new MSNetTCPSocketBuffer[2];
            byte[] hdr = MSNetUtils.asBytes(i);
            headerBuffer = new MSNetTCPSocketBuffer(hdr, 0, hdr.length, true);
            headerBuffer.setIsLastBufferInMessage(false);
            if (LOGGER.isTraceEnabled())
            {
                LOGGER.trace("Optimizing stream write for message of known STREAM_WRITE_SIZE={}", i);    
            }
            bufs[0] = headerBuffer;
        } else {
            bufs = new MSNetTCPSocketBuffer[1];
        }
        
        bufs[bufs.length - 1] = message.getAvailableBytes(true);
        bufs[bufs.length - 1].setIsLastBufferInMessage(message.isComplete());
        return bufs;
    }
    public MSNetMessage createMessage() {
        return new MSNetStringMessage();
    }
    
    public void cleanup() {
    }
    
    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }
    
    public int getMaxMessageSize() {
        return maxMessageSize;
    }
    public void setEncryptor(MSNetEncryptor encryptor) {
        this.encryptor = encryptor;
        if (encryptor == null)
            throw new NullPointerException("attempting to set encryptor to NULL");
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("set Encryptor={}",
                    encryptor.getClass().getCanonicalName());
        }
    }
    public boolean encryptionEnabled() { return this.encryptor != null; };
    public MSNetEncryptor getEncryptor() {
        return encryptor;
    }
}
