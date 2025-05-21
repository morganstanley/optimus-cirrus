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
import java.io.OutputStream;
import msjava.msnet.utils.MSNetConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class MSNetBufferedMessageOutputStream<T extends MSNetProtocolTCPConnection> extends OutputStream {
    protected final T _conn;
    protected final MSNetStreamableMessage msg;
    protected final boolean isNonStreamingMessage;
    private boolean hasBeenWrittenTo = false;
    protected int _len;
    private volatile ConnectionState state = ConnectionState.OPEN;
    
    private boolean closeConnectionOnTimeout = false;
    public enum SendMode {
        
        MODE_SEND,
        
        MODE_ASYNC_SEND,
        
        MODE_SYNC_SEND
    }
    private SendMode _sendMode = SendMode.MODE_SEND;
    private long _timeout = MSNetConfiguration.DEFAULT_OUTPUTSTREAM_TIMEOUT;
    private static final Logger LOGGER = LoggerFactory.getLogger(MSNetBufferedMessageOutputStream.class);
    public MSNetBufferedMessageOutputStream(T conn) {
        _conn = conn;
        MSNetMessage message = _conn.getProtocol().createMessage();
        if (message instanceof MSNetStreamableMessage) {
            msg = (MSNetStreamableMessage) message;
            msg.setComplete(false);
            isNonStreamingMessage = false;
        } else {
            msg = new MSNetPlainTCPMessage();
            isNonStreamingMessage = true;
        }
    }
    @Override
    public void write(int b) throws IOException {
        checkForError();
        msg.storeByte((byte)b);
        _len += 1;
        hasBeenWrittenTo = true;
    }
    @Override
    public void write(byte[] b, int offset, int len) throws IOException {
        checkForError();
        msg.storeBytes(b, offset, len);
        _len += len;
        hasBeenWrittenTo = true;
    }
    @Override
    public void flush() throws IOException {
        
    }
    @Override
    public void close() throws IOException {
        switch (state) {
        case OPEN:
            if (hasBeenWrittenTo) {
                msg.setComplete(true);
                sendBufferedMessage();
            }
            state = ConnectionState.CLOSED;
            cleanupOnClose();
            break;
        case CLOSED:
            LOGGER.info("An attempt was made to close a stream ({}) that was already closed", _conn.getAddress());
            return;
        case CLOSED_WITH_EXCEPTION:
            throw new IOException("close(): Error writing to stream - underlying protocol connection "
                    + _conn.getAddress() + ". The connection was found to be closed while it was being written to.");
        }
    }
    
    public boolean isClosed() {
        return state != ConnectionState.OPEN;
    }
    
    protected void checkForError() throws IOException {
        if (isClosed()) {
            throw new IOException("Message stream already closed");
        } 
        if (!_conn.isConnected()) {
            
            state = ConnectionState.CLOSED_WITH_EXCEPTION;
            cleanupOnClose();
            throw new IOException("Error writing to stream - underlying protocol connection (" + _conn.getAddress()
                    + ") was closed. conn.close");
        }
    }
    
    @Deprecated
    protected boolean checkAndSend() throws IOException {
        return sendBufferedMessage();
    }
    
    protected boolean sendBufferedMessage() throws IOException {
        checkForError();
        
        
        
        if (_len == 0 && !msg.isComplete()) {
            return false;
        }
        MSNetMessage msgToSend;
        if (isNonStreamingMessage) {
            msgToSend = _conn.getProtocol().createMessage();
            msgToSend.adoptBytes(msg.getBytes()); 
        } else {
            msgToSend = msg;
        }
        switch (_sendMode) {
        case MODE_SEND:
            _conn._send(msgToSend, true);
            break;
        case MODE_ASYNC_SEND:
            _conn._asyncSend(msgToSend, true);
            break;
        case MODE_SYNC_SEND:
            MSNetIOStatus status = _conn._syncSend(msgToSend, _timeout, true);
            handleSyncSendError(msgToSend, status, _conn);
            break;
        }
        _len = 0;
        return true;
    }
    private void handleSyncSendError(MSNetMessage msg, MSNetIOStatus status, MSNetProtocolTCPConnection connection)
            throws IOException {
        if (status.inError() && status.getException() instanceof MSNetSocketEndPipeException
                && !connection.isInitFromSocket()) {
            if (!connection.syncConnect(_timeout)) {
                throw new IOException("Not connected:" + connection.getInfoMessage());
            }
            status = connection.syncSend(msg, _timeout);
        }
        if (status.inError()) {
            state = ConnectionState.CLOSED_WITH_EXCEPTION;
            LOGGER.error("Rethrowing exception encountered while sync sending over stream", status.getException());
            cleanupOnClose();
            if (closeConnectionOnTimeout) {
                connection.close();
            }
            throw new IOException(status.getException());
        }
    }
    
    public long getTimeout() {
        return _timeout;
    }
    
    public void setTimeout(long timeout_) {
        _timeout = timeout_;
    }
    
    public SendMode getSendMode() {
        return _sendMode;
    }
    
    public void setSendMode(SendMode sendMode_) {
        _sendMode = sendMode_;
    }
    
    protected void cleanupOnClose() {
        
    }
    private enum ConnectionState {
        OPEN, CLOSED, CLOSED_WITH_EXCEPTION,
    }
    
    public boolean isCloseConnectionOnTimeout() {
        return closeConnectionOnTimeout;
    }
    
    public void setCloseConnectionOnTimeout(boolean closeConnectionOnTimeout) {
        this.closeConnectionOnTimeout = closeConnectionOnTimeout;
    }
}
