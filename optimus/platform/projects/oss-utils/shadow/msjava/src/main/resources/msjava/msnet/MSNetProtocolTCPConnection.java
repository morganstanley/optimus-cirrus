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
import msjava.msnet.auth.MSNetEncryptor;
import msjava.msnet.internal.MSNetProtocolConnectionConfiguration;
import msjava.msnet.utils.IdleTimeoutScheduler;
import msjava.msnet.utils.MSNetConfiguration;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ExecutorConfigurationSupport;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;
public class MSNetProtocolTCPConnection extends MSNetTCPConnection {
    protected MSNetProtocol _protocol;
    
    protected MSNetMessage _currMsg;
    protected StreamListenerCallback _streamHandler;
    
    
    protected Executor _streamCallbackExecutor;
    
    protected boolean _shutdownStreamCallbackExecutor = false;
    protected MSNetMessageInputStream _inputStream;
    protected MSNetMessageFragmentHandler _fragmentHandler;
    protected MSNetMessageOutputStream<MSNetProtocolTCPConnection> _outputStream;
    private boolean _doMessageCallback = true;
    private boolean _protocolSupportsOutputStream;
    protected final ReentrantLock _sendLock = new ReentrantLock();
    private int _streamListenerQueuesize = MSNetConfiguration.DEFAULT_STREAM_LISTENER_QUEUESIZE;
    private int streamBufferSizeLimit = MSNetConfiguration.getDefaultStreamBufferSizeLimit();
    private static final Logger LOGGER = LoggerFactory.getLogger(MSNetProtocolTCPConnection.class);
    
    public MSNetProtocolTCPConnection() {
    }
    
    public MSNetProtocolTCPConnection(MSNetLoop loop, MSNetInetAddress address, MSNetID id, MSNetProtocol protocol) {
        this(loop, address, id, protocol, true);
    }
    
    public MSNetProtocolTCPConnection(MSNetLoop loop, MSNetInetAddress address, MSNetID id, MSNetProtocol protocol,
                                      boolean enableAsyncRead) {
        super(loop, address, id, enableAsyncRead);
        setProtocol(protocol);
    }
    
    public MSNetProtocolTCPConnection(MSNetLoop loop, MSNetTCPSocket socket, MSNetID id, MSNetProtocol protocol) {
        this(loop, socket, id, protocol, true);
    }
    
    public MSNetProtocolTCPConnection(MSNetLoop loop, MSNetTCPSocket socket, MSNetID id, MSNetProtocol protocol,
                                      boolean enableAsyncRead) {
        super(loop, socket, id, enableAsyncRead);
        setProtocol(protocol);
    }
    public MSNetProtocolTCPConnection(MSNetProtocolConnectionConfiguration configSup)
            throws UnknownHostException {
        super(configSup.getPlainConnectionConfiguration());
        if (!configSup.isInitialized()) {
            configSup.init();
        }
        resetProtocolConnectionProperties(configSup);
    }
    private void updateOutputStreamSupportInfo() {
        _protocolSupportsOutputStream = (_protocol.createMessage() instanceof MSNetStreamableMessage);
    }
    
    public void resetProperties(MSNetProtocolConnectionConfiguration configSup) {
        super.resetProperties(configSup.getPlainConnectionConfiguration());
        resetProtocolConnectionProperties(configSup);
    }
    private void resetProtocolConnectionProperties(MSNetProtocolConnectionConfiguration configSup) {
        setProtocol(configSup.getProtocol());
        setStreamBufferSizeLimit(configSup.getStreamBufferSizeLimit());
        _streamCallbackExecutor = configSup.getStreamCallbackExecutor();
    }
    @Override
    protected MSNetTCPConnectionMetrics createMetrics() {
        return new MSNetProtocolTCPConnectionMetrics();
    }
    
    protected MSNetTCPSocketBuffer[] doCreateBuffers(MSNetMessage msg, boolean consume) {
        MSNetTCPSocketBuffer bufs[] = createBuffers(msg, consume);
        if (null != bufs) {
            MSNetProtocolTCPConnectionMetrics metrics = (MSNetProtocolTCPConnectionMetrics) getMetrics();
            if (metrics != null) {
                metrics.bufferCreated();
            }
        }
        return bufs;
    }
    
    protected MSNetTCPSocketBuffer[] createBuffers(MSNetMessage msg, boolean consume) {
        MSNetProtocol protocol = getProtocol();
        if (consume && protocol instanceof MSNetOutBoundProtocolWithMoveSupport) {
            return ((MSNetOutBoundProtocolWithMoveSupport) protocol).createBuffers(msg, true);
        }
        return getProtocol().createBuffers(msg);
    }
    
    protected boolean doProcessBuffer(MSNetTCPSocketBuffer buf, MSNetMessage msg) throws MSNetProtocolException {
        return doProcessBuffer(buf, msg, false);
    }
    
    protected boolean doProcessBuffer(MSNetTCPSocketBuffer buf, MSNetMessage msg, boolean disconnect)
            throws MSNetProtocolException {
        try {
            boolean ret = disconnect ? processBufferOnDisconnect(buf, msg) : processBuffer(buf, msg);
            if (ret) {
                MSNetProtocolTCPConnectionMetrics metrics = (MSNetProtocolTCPConnectionMetrics) getMetrics();
                if (metrics != null) {
                    metrics.bufferProcessed();
                }
                return true;
            }
        } catch (MSNetProtocolException x) {
            if (disconnect) {
                
                
                LOGGER.error("Error trying to process buffer after disconnect: " + x.toString(), x);
                errorNotify(MSNetConnectionErrorEnum.PROTOCOL_ERROR);
                throw x;
            } else {
                handleProtocolException(x);
                throw x;
            }
        }
        return false;
    }
    
    protected boolean processBufferOnDisconnect(MSNetTCPSocketBuffer buf, MSNetMessage msg)
            throws MSNetProtocolException {
        return getProtocol().processBufferOnDisconnect(buf, msg);
    }
    
    protected void handleProtocolException(MSNetProtocolException x) {
        LOGGER.error("Error trying to process buffer: " + x.toString(), x);
        if (x.isFatal()) {
            
            if (!MSNetConfiguration.SHOULD_RETRY_ON_FATAL_PROTOCOL_ERRORS) {
                setRetryFlag(false);
            }
            resetForError(MSNetConnectionErrorEnum.PROTOCOL_ERROR);
        }
    }
    
    protected boolean processBuffer(MSNetTCPSocketBuffer buf, MSNetMessage msg) throws MSNetProtocolException {
        return getProtocol().processBuffer(buf, msg);
    }
    
    @Override
    public MSNetIOStatus syncRead(MSNetMessage msg, long ms) {
        long endTime = System.currentTimeMillis() + ms;
        int bytesRead = 0;
        MSNetException x = null;
        for (; ; ) {
            try {
                
                
                
                
                bytesRead += _readBuffer.size();
                if (doProcessBuffer(_readBuffer, msg)) {
                    break;
                }
            } catch (MSNetProtocolException pe) {
                LOGGER.error("Error trying to process buffer: " + pe.toString(), pe);
                x = pe;
                
                break;
            } finally {
                
                bytesRead -= _readBuffer.size();
            }
            MSNetIOStatus status = super.syncRead((MSNetTCPSocketBuffer) null, endTime - System.currentTimeMillis());
            if ((x = status.getException()) != null) {
                if (LOGGER.isInfoEnabled()) {
                    if (x instanceof MSNetSelectTimeoutException) {
                        LOGGER.info("Timeout in syncRead from '{}'", getName());
                    } else {
                        LOGGER.info("Caught exception trying to syncRead from '" + getName() + "': " +
                                x, x);
                    }
                }
                try {
                    if (processBufferOnDisconnect(_readBuffer, msg)) {
                        
                        x = null;
                    }
                } catch (MSNetProtocolException e) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Error processing buffer on disconnect", x);
                    }
                }
                break;
            }
        }
        return new MSNetIOStatus(bytesRead, bytesRead, x);
    }
    
    
    
    @Override
    protected MSNetMessage createMessageFromBuffer(MSNetTCPSocketBuffer buffer) {
        assert (buffer.isEmpty() || hasListeners() || (_streamHandler != null) || (_fragmentHandler != null)) : "Read callback, but no read, stream, or fragment listener installed!  Data will be lost. Connection name"
                + this.getName();
        while (!buffer.isEmpty()) {
            if (_currMsg == null) {
                _currMsg = _protocol.createMessage();
            }
            final boolean complete;
            try {
                
                complete = doProcessBuffer(buffer, _currMsg);
            } catch (MSNetProtocolException x) {
                
                break;
            }
            
            
            if (isStreaming()) {
                MSNetTCPSocketBuffer avail = null;
                if (_currMsg instanceof MSNetStreamableMessage) {
                    
                    avail = ((MSNetStreamableMessage) _currMsg).getAvailableBytes(!_doMessageCallback);
                } else if (complete) {
                    
                    
                    byte b[] = _currMsg.getBytes();
                    avail = new MSNetTCPSocketBuffer(b, 0, b.length, true);
                }
                
                if (null != avail && (!avail.isEmpty() || complete)) {
                    if (_streamHandler != null) {
                        if (_inputStream == null) {
                            
                            
                            
                            
                            MSNetMessageInputStream ins = null;
                            synchronized (getStateLock()) {
                                if (getState() == MSNetConnectionStateEnum.CONNECTED) {
                                    
                                    ins = getMessageInputStream(getName(), _currMsg);
                                    _inputStream = ins;
                                }
                            }
                            if (ins != null) {
                                _inputStream.storeBytes(avail.getReadViews(), complete);
                                _streamHandler.invoke(ins);
                            }
                        } else {
                            _inputStream.storeBytes(avail.getReadViews(), complete);
                        }
                    }
                    if (_fragmentHandler != null) {
                        
                        final MSNetTCPSocketBuffer av = avail;
                        _fragmentHandler.fragmentCallback(getName(), av, complete);
                    }
                }
            }
            if (!complete) {
                
                break;
            }
            
            MSNetMessage retmsg = _currMsg;
            _currMsg = null;
            _inputStream = null;
            if (_doMessageCallback) {
                return retmsg;
            }
        }
        return null;
    }
    
    protected MSNetMessageInputStream getMessageInputStream(MSNetID name, MSNetMessage msg) {
        return new MSNetMessageInputStream(this, streamBufferSizeLimit );
    }
    
    @Override
    public MSNetIOStatus syncSend(MSNetMessage msg, long millisec) {
        _sendLock.lock();
        try {
            checkSendInProgress();
            return _syncSend(msg, millisec, false);
        } finally {
            _sendLock.unlock();
        }
    }
    
    @Override
    public void asyncSend(MSNetMessage msg) {
        _sendLock.lock();
        try {
            checkSendInProgress();
            _asyncSend(msg, false);
        } finally {
            _sendLock.unlock();
        }
    }
    
    void _asyncSend(MSNetMessage msg, boolean consume) {
        super.asyncSend(doCreateBuffers(msg, consume));
    }
    
    protected int _send(MSNetMessage msg, boolean consume) {
        return super.send(doCreateBuffers(msg, consume));
    }
    
    MSNetIOStatus _syncSend(MSNetMessage msg, long ms, boolean consume) {
        return super.syncSend(doCreateBuffers(msg, consume), ms);
    }
    
    @Override
    public int send(MSNetMessage msg) {
        _sendLock.lock();
        try {
            checkSendInProgress();
            return _send(msg, false);
        } finally {
            _sendLock.unlock();
        }
    }
    @Override
    public int send(MSNetTCPSocketBuffer buf) {
        _sendLock.lock();
        try {
            checkSendInProgress();
            return super.send(buf);
        } finally {
            _sendLock.unlock();
        }
    }
    @Override
    public int send(MSNetTCPSocketBuffer buf[]) {
        _sendLock.lock();
        try {
            checkSendInProgress();
            return super.send(buf);
        } finally {
            _sendLock.unlock();
        }
    }
    @Override
    public int send(byte buf[]) {
        _sendLock.lock();
        try {
            checkSendInProgress();
            return super.send(buf);
        } finally {
            _sendLock.unlock();
        }
    }
    @Override
    public void asyncSend(MSNetTCPSocketBuffer buf) {
        _sendLock.lock();
        try {
            checkSendInProgress();
            super.asyncSend(buf);
        } finally {
            _sendLock.unlock();
        }
    }
    @Override
    public void asyncSend(MSNetTCPSocketBuffer buf[]) {
        _sendLock.lock();
        try {
            checkSendInProgress();
            super.asyncSend(buf);
        } finally {
            _sendLock.unlock();
        }
    }
    
    @Override
    public void asyncSend(byte buf[], int len) {
        _sendLock.lock();
        try {
            checkSendInProgress();
            super.asyncSend(buf, len);
        } finally {
            _sendLock.unlock();
        }
    }
    
    @Override
    public MSNetIOStatus syncSend(MSNetTCPSocketBuffer buf, long ms) {
        _sendLock.lock();
        try {
            checkSendInProgress();
            return super.syncSend(buf, ms);
        } finally {
            _sendLock.unlock();
        }
    }
    
    @Override
    public MSNetIOStatus syncSend(MSNetTCPSocketBuffer buf[], long ms) {
        _sendLock.lock();
        try {
            checkSendInProgress();
            return super.syncSend(buf, ms);
        } finally {
            _sendLock.unlock();
        }
    }
    
    @Override
    public MSNetIOStatus syncSend(byte buf[], int len, long ms) {
        _sendLock.lock();
        try {
            checkSendInProgress();
            return super.syncSend(buf, len, ms);
        } finally {
            _sendLock.unlock();
        }
    }
    
    public void setProtocol(MSNetProtocol protocol) {
        _protocol = protocol;
        updateOutputStreamSupportInfo();
    }
    
    public MSNetProtocol getProtocol() {
        return _protocol;
    }
    
    public void setMessageStreamHandler(MSNetMessageStreamHandler streamHandler) {
        if (streamHandler != null) {
            if (timeoutScheduler != null && timeoutScheduler.isTimeoutSet()) {
                throw new IllegalArgumentException("Cannot use idle timeouts with streaming!");
            }
            _streamHandler = new StreamListenerCallback(streamHandler);
            _doMessageCallback = false;
        } else {
            _streamHandler = null;
            _doMessageCallback = true;
        }
    }
    
    public void setMessageFragmentHandler(MSNetMessageFragmentHandler fragmentHandler) {
        if (fragmentHandler != null && timeoutScheduler != null && timeoutScheduler.isTimeoutSet()) {
            throw new IllegalArgumentException("Cannot use idle timeouts with streaming!");
        }
        _fragmentHandler = fragmentHandler;
        if (_fragmentHandler != null) {
            _doMessageCallback = true;
        }
    }
    private boolean isStreaming() {
        return _fragmentHandler != null || _streamHandler != null;
    }
    
    public void enableMessageCallback(boolean enableMessageCallback) {
        _doMessageCallback = enableMessageCallback;
    }
    protected class StreamListenerCallback {
        private ArrayBlockingQueue<InputStream> _queue = new ArrayBlockingQueue<InputStream>(_streamListenerQueuesize);
        private final Runnable _runnable;
        public StreamListenerCallback(final MSNetMessageStreamHandler listener) {
            Assert.notNull(listener, "Listener cannot be null");
            _runnable = new Runnable() {
                @Override
                public void run() {
                    MSNetMessageInputStream ins;
                    try {
                        ins = (MSNetMessageInputStream) _queue.take();
                        assert ins != null;
                        listener.streamCallback(ins.getConnectionID(), ins);
                        
                    } catch (InterruptedException e) {
                        LOGGER.error("Interrupted during stream callback, message lost");
                        Thread.currentThread().interrupt();
                    }
                }
            };
        }
        public void invoke(final InputStream in) {
            assert in != null;
            try {
                
                
                _queue.put(in);
                if (null != _streamCallbackExecutor) {
                    _streamCallbackExecutor.execute(_runnable);
                } else {
                    
                    getStreamCallbackExecutor().execute(_runnable);
                }
            } catch (InterruptedException e) {
                LOGGER.error("Interrupted during invocation message lost {}", in);
                Thread.currentThread().interrupt();
            }
        }
    }
    
    protected Executor getStreamCallbackExecutor() {
        if (null != _streamCallbackExecutor) {
            
            return _streamCallbackExecutor;
        }
        
        Executor exe = getCallbackExecutor();
        if (exe instanceof MSNetLoopExecutor && getNetLoop().equals(((MSNetLoopExecutor) exe).getLoop())) {
            _streamCallbackExecutor = createThreadPoolExecutor();
            _shutdownStreamCallbackExecutor = true;
        } else {
            _streamCallbackExecutor = exe;
            _shutdownStreamCallbackExecutor = false;
        }
        return _streamCallbackExecutor;
    }
    private ThreadPoolTaskExecutor createThreadPoolExecutor() {
        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
        threadPoolTaskExecutor.setCorePoolSize(1);
        threadPoolTaskExecutor.setMaxPoolSize(1);
        threadPoolTaskExecutor.setKeepAliveSeconds(0);
        threadPoolTaskExecutor.setQueueCapacity(Integer.MAX_VALUE);
        threadPoolTaskExecutor
                .setBeanName(this.getAddress() != null ? this.getAddress().toString() + "ConExec" : "ConExec");
        threadPoolTaskExecutor.afterPropertiesSet();
        return threadPoolTaskExecutor;
    }
    
    public MSNetMessageOutputStream<MSNetProtocolTCPConnection> getMessageOutputStream() {
        return getMessageOutputStream(MSNetMessageOutputStream.UNKNOWN_WRITE_SIZE);
    }
    
    public MSNetMessageOutputStream<MSNetProtocolTCPConnection> getMessageOutputStream(int knownLengthOfMessage) {
        if (!_protocolSupportsOutputStream) {
            throw new UnsupportedOperationException(getProtocol().getClass().getName() + " doesn't support streaming");
        }
        
        _sendLock.lock();
        try {
            checkSendInProgress();
            _outputStream = new MSNetOptimizedMessageOutputStream<MSNetProtocolTCPConnection>(this,
                    MSNetConfiguration.DEFAULT_OUTPUTSTREAM_FRAGMENT_SIZE, knownLengthOfMessage);
        } catch (RuntimeException e) {
            _sendLock.unlock();
            throw e;
        }
        return _outputStream;
    }
    
    public MSNetBufferedMessageOutputStream<MSNetProtocolTCPConnection> getBufferedMessageOutputStream() {
        
        return new MSNetBufferedMessageOutputStream<MSNetProtocolTCPConnection>(this);
    }
    
    void closeMessageOutputStream() {
        _outputStream = null;
        _sendLock.unlock();
    }
    
    protected void checkSendInProgress() {
        if (isSendInProgress()) {
            throw new IllegalStateException("Existing message stream already in progress");
        }
    }
    
    public boolean isSendInProgress() {
        return null != _outputStream;
    }
    
    @Override
    protected void beforeCleanup(boolean clearRetry) {
        
        try {
            if (_currMsg != null && doProcessBuffer(_readBuffer, _currMsg, true)) {
                readNotify(_currMsg);
            }
        } catch (MSNetProtocolException e) {
            
        }
        
        _currMsg = null;
        
        if (_shutdownStreamCallbackExecutor && _streamCallbackExecutor != null) {
            try {
                if (_streamCallbackExecutor instanceof ExecutorConfigurationSupport) {
                    ((ExecutorConfigurationSupport) _streamCallbackExecutor).shutdown();
                } else {
                    ((ExecutorService) _streamCallbackExecutor).shutdown();
                }
            } catch (ClassCastException e) {
                
                LOGGER.error("Error trying to shutdown executor: " + e.toString(), e);
            }
            _streamCallbackExecutor = null;
        }
    }
    @Override
    protected void afterCleanup(boolean clearRetry) {
        
        MSNetMessageInputStream ins = _inputStream;
        if (ins != null) {
            ins.setError(true);
        }
        _inputStream = null;
    }
    
    protected MSNetMessage getCurrentMessage() {
        return _currMsg;
    }
    
    public int getStreamListenerQueueSize() {
        return _streamListenerQueuesize;
    }
    
    public void setStreamListenerQueueSize(int size) {
        _streamListenerQueuesize = size;
    }
    
    public int getStreamBufferSizeLimit() {
        return streamBufferSizeLimit;
    }
    
    public void setStreamBufferSizeLimit(int streamBufferSizeLimit) {
        Validate.isTrue(streamBufferSizeLimit != 0, "0 buffer size limit is not allowed");
        this.streamBufferSizeLimit = streamBufferSizeLimit;
    }
    @Override
    void setIdleTimeout(long timeout) {
        if (_streamHandler != null && timeout != IdleTimeoutScheduler.TIMEOUT_VALUE_NONE) {
            throw new IllegalArgumentException("Cannot use idle timeouts with streaming!");
        }
        timeoutScheduler.setTimeout(timeout);
    }
    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        Validate.notNull(_protocol, "Protocol must be set! Use setProtocol()");
    }
    @Override
    void finishEstablish(boolean syncLoop, boolean enableAsyncRead) throws MSNetEstablishException {
        assert _inputStream == null;
        
        _currMsg = null;
        MSNetMessageInputStream ins = _inputStream;
        if (ins != null) {
            ins.setError(true);
        }
        _inputStream = null;
        super.finishEstablish(syncLoop, enableAsyncRead);
        if (getAuthContext() != null) {
            Object encr = getAuthContext().getProperty(MSNetEncryptor.class.getName());
            if (encr instanceof MSNetEncryptor) {
                
                if (_protocol instanceof MSNetStringProtocol) {
                    ((MSNetStringProtocol) _protocol).setEncryptor((MSNetEncryptor) encr);
                } else {
                    throw new MSNetEstablishException(
                            "Encryption is not supported for protocol" + _protocol.getClass().getCanonicalName());
                }
            }
        }
    }
    @Override
    protected boolean canHandleNotifications() {
        return hasListeners() || isStreaming();
    }
    
    @Override
    protected void fillString(StringBuilder builder) {
        super.fillString(builder);
        builder.append(", ");
        builder.append("protocol=").append(_protocol);
        builder.append(", ");
        builder.append("isStreaming=").append(isStreaming());
    }
    
    @Override
    protected void fillDiagnosticInformation(StringBuilder builder, int lod) {
        super.fillDiagnosticInformation(builder, lod);
        
        builder.append("\n");
        builder.append("    ").append("currentMessage=").append(getCurrentMessage());
        if (lod >= 2) {
            builder.append("\n");
            builder.append("    ").append("streamCallbackExecutor=").append(_streamCallbackExecutor);
            builder.append("\n");
            builder.append("    ").append("frameHandler=").append(_fragmentHandler);
            builder.append("\n");
            builder.append("    ").append("streamHandler=").append(_streamHandler);
        }
    }
}
