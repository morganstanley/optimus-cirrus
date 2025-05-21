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
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import msjava.msnet.internal.MSNetUtils;
import msjava.msnet.utils.MSNetConfiguration;
public class MSNetStringMessage implements MSNetStreamableMessage, InputStreamSupport {
    
    public static final int SIZE_OF_MSG_UNAVAILABLE = -1;
    public static final int SIZE_OF_MSG_SET_PREVIOUSLY = -2;
    
    private int bytesLeftInFragment = -1;
    
    private boolean complete = true;
    private int availablePtr = 0;
    protected MSNetTCPSocketBuffer buf = new MSNetTCPSocketBuffer();
    
    private int _lengthFromHeader = SIZE_OF_MSG_UNAVAILABLE;
    private byte[] decryptedBytes = null;
    private int decryptedPos = 0;
    
    public MSNetStringMessage() {
    }
    
    public MSNetStringMessage(String str) {
        this(str.getBytes(), true);
    }
    
    public MSNetStringMessage(byte[] bytes) {
        this(bytes, MSNetConfiguration.isAdoptBuffers());
    }
    
    public MSNetStringMessage(byte[] bytes, boolean adopt) {
        if (adopt) {
            adoptBytes(bytes);
        } else {
            setBytes(bytes, 0, bytes.length);
        }
    }
    
    public void setBytes(byte[] bytes, int offset, int length) {
        buf.clear();
        buf.store(bytes, offset, length);
        availablePtr = 0;
    }
    @Override
    public void storeByte(byte b) {
        buf.store(b);
    }
    
    public void storeBytes(ByteBuffer bbuf) {
        buf.store(bbuf);
    }
    
    public void storeBytes(byte[] bb, int offset, int len) {
        buf.store(bb, offset, len);
    }
    
    public void adoptBuffer(ByteBuffer bbuf) {
        buf = new MSNetTCPSocketBuffer(bbuf);
        availablePtr = 0;
    }
    public void adoptBytes(byte[] bytes) {
        adoptBytes(bytes, 0, bytes.length);
    }
    public void adoptBytes(byte[] bytes, int offset, int len) {
        buf.setBytes(bytes, offset, len, true);
        availablePtr = 0;
    }
    
    public byte[] getBytes() {
        return buf.peek();
    }
    
    public int getLength() {
        return buf.size();
    }
    
    MSNetTCPSocketBuffer getBuffer() {
        return buf;
    }
    
    public String toString() {
        return getString();
    }
    
    @SuppressWarnings("deprecation")
    public String getString() {
        synchronized (buf) {
            return new String(buf.getBytes(), buf.getHeadPosition(), buf.size());
        }
    }
    
    @SuppressWarnings("deprecation")
    public String getString(String encoding) throws UnsupportedEncodingException {
        synchronized (buf) {
            return new String(buf.getBytes(), buf.getHeadPosition(), buf.size(), encoding);
        }
    }
    
    int getBytesLeftInFragment() {
        return bytesLeftInFragment;
    }
    
    void setBytesLeftInFragment(int hdrlen) {
        bytesLeftInFragment = hdrlen;
    }
    
    public boolean isComplete() {
        return complete;
    }
    
    public void setComplete(boolean complete) {
        this.complete = complete;
    }
    
    public MSNetTCPSocketBuffer getAvailableBytes(boolean consume) {
        MSNetTCPSocketBuffer ret = null;
        if (consume) {
            
            ret = buf;
            buf = new MSNetTCPSocketBuffer();
        } else {
            
            ret = new MSNetTCPSocketBuffer(buf, true, availablePtr, buf.size() - availablePtr);
            availablePtr += ret.size();
        }
        return ret;
    }
    public int getLengthFromHeader() {
        return _lengthFromHeader;
    }
    protected void setLengthFromHeader(int headerLength) {
        _lengthFromHeader = headerLength;
    }
    @Override
    public InputStream getInputStream() {
        return buf.createReadStream();
    }
    
    final public boolean isDecrypted() {
        return decryptedBytes != null;
    }
    public boolean setToNextDecryptedMsg() throws MSNetProtocolException {
        int lenSize = decryptedBytes.length - decryptedPos;
        if (lenSize == 0)
            return false;
        if (lenSize < MSNetStringProtocol.INTEGER_SIZE)
            throw new MSNetProtocolException("Malformed Hdr: expected len bytes=" +
                    Integer.BYTES + ", actual length bytes=" + lenSize, false);
        int len = MSNetUtils.asInt(decryptedBytes, decryptedPos);
        int startOffset = decryptedPos + MSNetStringProtocol.INTEGER_SIZE;
        if (startOffset + len > decryptedBytes.length) {
            throw new MSNetProtocolException(String.format(
                    "Hdr Len exceeds Decrypted Data: HdrLen=%s, offset=%d, decryptBufLen=%d",
                    len, startOffset,decryptedBytes.length),  false);
        }
        setBytes(decryptedBytes, decryptedPos + MSNetStringProtocol.INTEGER_SIZE, len);
        decryptedPos += MSNetStringProtocol.INTEGER_SIZE + len;
        return true;
    }
    public void setDecryptedBuffer(byte[] decryptedBytes) {
        this.decryptedBytes = decryptedBytes;
        decryptedPos = 0;
    }
}
