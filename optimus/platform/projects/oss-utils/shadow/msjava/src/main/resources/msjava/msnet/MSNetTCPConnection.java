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
import javax.annotation.CheckReturnValue;
import msjava.base.annotation.Experimental;
import msjava.base.annotation.Internal;
import msjava.base.kerberos.MSKerberosAuthority;
import msjava.msnet.auth.MSNetAnonymousAuthMechanism;
import msjava.msnet.auth.MSNetAuthContext;
import msjava.msnet.auth.MSNetAuthenticationException;
import msjava.msnet.auth.MSNetAuthenticator;
import msjava.msnet.auth.MSNetKerberosEstablisherFactory;
import msjava.msnet.auth.MSNetMateExtractor;
import msjava.msnet.AbstractEstablisherHandler.AsyncEstablishHandler;
import msjava.msnet.AbstractEstablisherHandler.SyncEstablishHandler;
import msjava.msnet.configurationmanager.ConnectionConfigurationManager;
import msjava.msnet.connectionevent.internal.ConnectionReporter;
import msjava.msnet.internal.LockSafeMSNetLoopExecutor;
import msjava.msnet.internal.MSNetAuditLogger;
import msjava.msnet.internal.MSNetConnectionConfigurationSupport;
import msjava.msnet.utils.IdleTimeoutScheduler;
import msjava.msnet.utils.MSNetConfiguration;
import msjava.slf4jutils.sanity.SanitizingLoggerFactory;
import msjava.threadmonitor.thread.MSThreadMonitor;
import msjava.tools.util.MSOctalEncoder;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.annotations.VisibleForTesting;
import com.ms.infra.net.establishers.auth.mechanism.MechanismDescription;
import com.ms.infra.net.establishers.auth.mechanism.kerberos.ChannelBindingMode;
import com.ms.infra.net.establishers.auth.report.MateWTPublisher;
import com.ms.infra.net.establishers.auth.report.Publisher;
import java.io.IOException;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static msjava.msnet.MSNetConnectionStateEnum.*;
public class MSNetTCPConnection extends MSNetAbstractConnection {
    
    @Internal
    interface MSNetInternalConnectionListener {
        
        void closeCallback(MSNetID id);
        
        void disconnectCallback(MSNetID id);
    }
    private static final Logger LOGGER = LoggerFactory.getLogger(MSNetTCPConnection.class);
    private static final Logger SANITIZING_LOGGER = SanitizingLoggerFactory.getLogger(MSNetTCPConnection.class);
    private static final MSNetInetAddress GLOBAL_BIND_ADDRESS = initGlobalBindAddress();
    private static final int TAP_READ = 1;
    private static final int TAP_WRITE = 2;
    
    
    
    
    
    
    
    
    
    
    
    
    
    protected final Object _writeLock = new Object();
    
    private final Object _socketLock = new Object();
    
    private final ReadWriteLock _syncReadWriteLock = new ReentrantReadWriteLock();
    
    private final Object _asyncSocketCloseCompleteLock = new Object();
    
    private MSNetTCPSocketFactory socketFactory = MSNetConfiguration.getDefaultMSNetTCPSocketFactory();
    protected MSNetTCPSocket _socket;
    
    private final MSNetInetAddressList addrs = new MSNetInetAddressList();
    
    private MSNetInetAddress _socksProxyAddress;
    private String _socksUserName;
    private MSNetSOCKS4Establisher _socksEstablisher;
    MSNetChannel _channel;
    
    protected MSNetTCPSocketBuffer _readBuffer = new MSNetTCPSocketBuffer();
    
    protected LinkedList<MSNetTCPSocketBuffer> _writeList = new LinkedList<>(); 
    
    
    private volatile long _writeListThreshold = 300000;
    
    protected volatile long _writeBufferSize = 0;
    
    protected int _currentWritten = 0;
    
    private boolean _tapping = MSNetConnectionConfigurationSupport.getTapFile() != null;
    
    private final AtomicBoolean _didConnectCallback = new AtomicBoolean(false);
    private boolean _initFromSocket = false;
    
    protected volatile boolean _readingEnabled = true;
    
    private boolean _readingEnabledSettingAtConfig;
    private final SortedMap<MSNetEstablisher,MSNetEstablisher> establishers = new TreeMap<>();
    private MSNetAuthContext _authContext;
    
    private MSNetMultiplexer _syncWriteMultiplexer = null;
    
    private MSNetMultiplexer _syncReadMultiplexer = null;
    private final Object _syncReadMultiplexerLock = new Object();
    private boolean _keepAlive = MSNetConfiguration.DEFAULT_TCP_KEEPALIVE;
    private boolean _tcpNoDelay = MSNetConfiguration.DEFAULT_TCP_NODELAY;
    private int _receiveBufferSize = MSNetConfiguration.DEFAULT_SOCKET_RCV_BUFFER_SIZE;
    private int _sendBufferSize = MSNetConfiguration.DEFAULT_SOCKET_SEND_BUFFER_SIZE;
    
    private volatile Object _retryKey;
    protected MSNetTCPConnectionMetrics _metrics;
    private boolean _sendDoneCallbackDisabled = MSNetConfiguration.DEFAULT_DISABLE_SEND_DONE_CALLBACK;
    private volatile boolean _isAsyncSocketCloseInProgress = false;
    
    private volatile boolean resetCalled = false;
    private MSNetConnectionStateEnum _stateBeforeConnectionReset;
    private MSNetInetAddress _bindAddress;
    private boolean kerberos;
    private boolean kerberosEncryption;
    private ChannelBindingMode channelBindingMode;
    private boolean anonymousAuthentication;
    
    
    private boolean authenticatorInitialized = false;
    private List<String> backupAddresses;
    private boolean immutable = false;
    private volatile boolean connectNotificationOnSyncConnect = false;
    private volatile long asyncConnectTimeout = 0;
    private final AtomicReference<Object> asyncConnectTimeoutCallbackId = new AtomicReference<>();
    private MasterEstablisherMode masterEstablisherMode = MasterEstablisherMode.OFF;
    private final CopyOnWriteArrayList<MSNetInternalConnectionListener> internalConnectionListeners = new CopyOnWriteArrayList<>();
    private static MSNetInetAddress initGlobalBindAddress() {
        MSNetInetAddress address = null;
        if (MSNetConfiguration.DEFAULT_MSNET_BIND_ADDRESS != null) {
            try {
                address = new MSNetInetAddress(MSNetConfiguration.DEFAULT_MSNET_BIND_ADDRESS);
            } catch (Exception e) {
                LOGGER.error("Error resolving global bind address [{}]: {}", MSNetConfiguration.DEFAULT_MSNET_BIND_ADDRESS, e.toString());
            }
        }
        return address;
    }
    
    public MSNetTCPConnection() {
    }
    
    public MSNetTCPConnection(MSNetLoop loop, MSNetInetAddress address, String id) {
        this(loop, address, new MSNetID(id), true);
    }
    
    public MSNetTCPConnection(MSNetLoop loop, MSNetInetAddress address, MSNetID id, boolean enableAsyncRead) {
        super(loop, id);
        if ((loop == null || loop.getImpl() instanceof MSNetLoopNullImpl) && enableAsyncRead) {
            throw new IllegalStateException("Cannot use asynchronous reading with a null loop");
        }
        _doRetry = true;
        _readingEnabled = enableAsyncRead;
        _readingEnabledSettingAtConfig = enableAsyncRead;
        setPrimaryAddress(new MSNetInetAddress(address));
        timeoutScheduler = new IdleTimeoutScheduler(this);
    }
    
    public MSNetTCPConnection(MSNetLoop loop, MSNetInetAddress address, MSNetInetAddress socksProxyAddress, MSNetID id) {
        this(loop, address, id, true);
        setSOCKSProxyAddress(socksProxyAddress);
    }
    
    public MSNetTCPConnection(MSNetLoop loop, MSNetInetAddress address, MSNetID id) {
        this(loop, address, id, true);
    }
    
    public MSNetTCPConnection(MSNetLoop loop, List<MSNetInetAddress> addressList, MSNetID id) {
        this(loop, addressList, id, true);
    }
    
    public MSNetTCPConnection(MSNetLoop loop, MSNetInetAddress address, String id, boolean enableAsyncRead) {
        this(loop, address, new MSNetID(id), enableAsyncRead);
    }
    
    public MSNetTCPConnection(MSNetLoop loop, List<MSNetInetAddress> addressList, MSNetID id, boolean enableAsyncRead) {
        super(loop, id);
        if ((loop == null || loop.getImpl() instanceof MSNetLoopNullImpl) && enableAsyncRead) {
            throw new IllegalStateException("Cannot use asynchronous reading with a null loop");
        }
        _doRetry = true;
        _readingEnabled = enableAsyncRead;
        _readingEnabledSettingAtConfig = enableAsyncRead;
        setAddressList(addressList);
        timeoutScheduler = new IdleTimeoutScheduler(this);
    }
    
    
    public MSNetTCPConnection(MSNetLoop loop, MSNetTCPSocket socket, MSNetID id) {
        this(loop, socket, id, true);
    }
    
    public MSNetTCPConnection(MSNetLoop loop, MSNetTCPSocket socket, MSNetID id, boolean enableAsyncRead) {
        super(loop, id);
        if ((loop == null || loop.getImpl() instanceof MSNetLoopNullImpl) && enableAsyncRead) {
            throw new IllegalStateException("Cannot use asynchronous reading with a null loop");
        }
        _doRetry = false;
        _socket = socket;
        _readingEnabled = enableAsyncRead;
        _readingEnabledSettingAtConfig = enableAsyncRead;
        _initFromSocket = true;
        timeoutScheduler = new IdleTimeoutScheduler(this);
    }
    public MSNetTCPConnection(MSNetConnectionConfigurationSupport configSup) throws UnknownHostException {
        super(configSup);
        if ((configSup.getLoop() == null || configSup.getLoop().getImpl() instanceof MSNetLoopNullImpl)
                && configSup.getAsyncReading()) {
            throw new IllegalStateException("Cannot use asynchronous reading with a null loop");
        }
        setPrimaryAddress(new MSNetInetAddress(configSup.getHostPort()));
        List<String> backupAddresses = configSup.getBackupAddresses();
        if (backupAddresses != null) {
            for (Iterator<String> i = backupAddresses.iterator(); i.hasNext(); ) {
                setAddress(getAddressList().size(), new MSNetInetAddress(i.next()));
            }
        }
        String bindAddress = configSup.getBindAddress();
        if (bindAddress != null && configSup.isSkipHostPortValidation()) {
            
            new MSNetInetAddress(bindAddress);
        }
        resetProperties(configSup);
        kerberos = configSup.getKerberos();
        kerberosEncryption = configSup.getKerberosEncryption();
        
        if (kerberosEncryption && !kerberos)
            throw new IllegalArgumentException("Attempt to use Kerberos encryption without Kerberos authentication");
        this.channelBindingMode = configSup.getChannelBindingMode();
        this.anonymousAuthentication = configSup.getAnonymousAuthentication();
        if (kerberos || anonymousAuthentication) {
            addAuthenticator();
            setAuthenticatorInitialized();
        }
        List<MSNetEstablisher> establishers = configSup.getEstablishers();
        if (establishers != null) {
            for (MSNetEstablisher e : establishers) {
                addEstablisher(e);
            }
        }
        timeoutScheduler = new IdleTimeoutScheduler(this);
    }
    private void addAuthenticator() {
        MSNetAuthenticator auth;
        if (kerberos) {
            try {
                auth = (MSNetAuthenticator) MSNetKerberosEstablisherFactory.builder().client()
                        .channelBinding(channelBindingMode).encryption(kerberosEncryption).build().createEstablisher();
            } catch (MSNetAuthenticationException e) {
                throw new RuntimeException(e);
            }
            if (kerberosEncryption) {
                LOGGER.info("Client initialized with setKerberosEncryption(true) - kerberos enabled WITHOUT any authorization.");
            } else {
                LOGGER.info("Client initialized with setKerberos(true) - kerberos enabled WITHOUT any authorization.");
            }
        } else {
            auth = new MSNetAuthenticator();
            auth.addAuthMechanism(new MSNetAnonymousAuthMechanism());
        }
        addEstablisher(auth);
    }
    
    @SuppressWarnings("deprecation")
    void resetProperties(MSNetConnectionConfigurationSupport configSup) {
        _authContext = null;
        _doRetry = configSup.getRetryFlag();
        _readingEnabled = configSup.getAsyncReading();
        _readingEnabledSettingAtConfig = configSup.getAsyncReading();
        _maxRetryTime = configSup.getMaxRetryTime();
        _retryTime = configSup.getRetryTime();
        if(configSup.getInitialRetryTime() > 0){ 
            setInitialRetryTime(configSup.getInitialRetryTime());
        }
        _maxRetries = configSup.getMaxRetries();
        _maxDisconnects = configSup.getMaxDisconnects();
        _tapping = configSup.getTapping();
        _keepAlive = configSup.getKeepAlive();
        _tcpNoDelay = configSup.getTcpNoDelay();
        _receiveBufferSize = configSup.getReceiveBufferSize();
        _sendBufferSize = configSup.getSendBufferSize();
        connectNotificationOnSyncConnect = configSup.isConnectNotificationOnSyncConnect();
        String bindAddress = configSup.getBindAddress();
        if (bindAddress != null) {
            try {
                _bindAddress = new MSNetInetAddress(bindAddress);
            } catch (UnknownHostException e) {
                
                LOGGER.error("Error resolving bind address: {}", e.toString());
            }
        }
        setCallbackExecutor(configSup.getCallbackExecutor());
        setSocketFactory(configSup.getSocketFactory());
    }
    
    @Override
    public MSNetAuthContext getAuthContext() {
        return _authContext;
    }
    
    public void setAuthContext(MSNetAuthContext authContext) {
        _authContext = authContext;
    }
    
    public void addEstablisher(MSNetEstablisher establisher) {
        establisher.init(isInitFromSocket(), this);
        MSNetEstablisher existing = establishers.put(establisher, establisher);
        if (existing != null) {
            throw new MSNetFatalException("Unable to add establisher " + establisher.getEstablisherName() +
                    " to MSNetTCPConnection [" + getName() +
                    "] - there is already an establisher of priority " + establisher.getPriority() +
                    " (" + establisher.getEstablisherName() + ")");
        }
        LOGGER.info("DEBUG: adding establisher name={}, encryption flag={}",
            establisher.getEstablisherName(), kerberosEncryption);
    }
    
    public boolean isInitFromSocket() {
        return _initFromSocket;
    }
    
    @Override
    public boolean getTapping() {
        return _tapping;
    }
    
    @Override
    public void setTapping(boolean tapping) {
        this._tapping = tapping;
    }
    
    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (_socket != null) {
            LOGGER.error("Connection with name " + getName()
                    + " was not closed. The file descriptor is being cleaned up in the finalizer");
            doCloseSocket(_socket);
        }
    }
    
    protected MSNetTCPConnectionMetrics createMetrics() {
        return new MSNetTCPConnectionMetrics();
    }
    public MSNetTCPConnectionMetrics getMetrics() {
        return _metrics;
    }
    
    synchronized void initFromSocket() {
        assert isLockStateValid(this);
        
        
        MSNetTCPSocket socket = _socket;
        
        if (socket == null || !socket.isValid()) {
            LOGGER.error("Attempt to init with invalid socket!");
            return;
        }
        if (socket.getParentFactory() != null) {
            
            
            
            setSocketFactory(socket.getParentFactory());
        }
        setAddress(socket.getAddress());
        socket.setKeepAlive(_keepAlive);
        socket.setTcpNoDelay(_tcpNoDelay);
        socket.setReceiveBufferSize(_receiveBufferSize);
        socket.setSendBufferSize(_sendBufferSize);
        _channel = new MSNetChannel(getName().toString(), socket);
        socket.setBlocking(false);
        assert getState() == INVALID;
        try {
            connectingLock.lock();
            addAsyncConnectTimeoutCallback();
            setState(CONNECTING);
            
            
            
            
            establish(false, false);
        } catch (MSNetEstablishException e) {
            logConnectError("Connection[" + getName() + "] from " + getAddress() + " failed ", e);
            resetConnection();
        } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
        } finally {
            connectingLock.unlock();
            assert connectingLock.notHeldByCurrentThread();
        }
    }
    
    @Override
    public void reparent(MSNetLoop newLoop) {
        if (_loop != newLoop) {
            boolean read = isReadingEnabled();
            boolean write = isWritingEnabled();
            MSNetLoop oldLoop = _loop;
            _loop = newLoop;
            LOGGER.debug("Reparenting connection {} from loop {} to {}", this, oldLoop, newLoop);
            if (read) {
                setupReadHandler(oldLoop);
            }
            if (write) {
                setupWriteHandler();
            }
            
            
            reparentNotify();
            
            
            _loop.wakeup();
        } else {
            LOGGER.debug("Reparenting connection {} skipped, already on the specified loop", this);
        }
    }
    protected void clearRetryHandler() {
        Object retryKey = _retryKey;
        _retryKey = null;
        if (retryKey != null) {
            getNetLoop().cancelCallback(retryKey);
        }
    }
    protected void setupRetryHandler(final long retryTime) {
        clearRetryHandler();
        _retryKey = _loop.callbackAfterDelay(retryTime, new MSNetEventListener() {
            @Override
            public void eventOccurred(MSNetEvent e) {
                retryHandler();
            }
        });
        if (LOGGER.isDebugEnabled()) {
            Thread threadForLoop = MSNetLoop.getThreadForLoop(_loop);
            String threadDetail;
            if (threadForLoop != null) {
                threadDetail = "on the netloop thread:" + threadForLoop.getName();
            } else {
                threadDetail = "on thread:" + Thread.currentThread().getName();
            }
            LOGGER.debug("Setting up retry handler {} ", threadDetail);
        }
    }
    
    private void setupReadHandler(MSNetLoop disableLoop) {
        setupReadHandler(disableLoop, _readingEnabled);
    }
    
    private void setupReadHandler(MSNetLoop disableLoop, boolean enabled) {
        MSNetChannel c = _channel;
        if (null != c) {
            c.setReadListener(new ReadHandler());
        }
        if (enabled) {
            enableReading();
        }
    }
    private class ReadHandler implements MSNetChannelListener {
        @Override
        public void channelReady(MSNetChannelEvent e) {
            doRead();
        }
    }
    
    private void updateReadingEnabled() {
        if (_readingEnabled) {
            enableChannel(MSNetChannelOpertaion.READ);
        } else {
            disableChannel(MSNetChannelOpertaion.READ);
        }
    }
    
    private void setupWriteHandler() {
        MSNetChannel c = _channel;
        if (null != c) {
            c.setWriteListener(new WriteHandler());
        }
        assert isLockStateValid(_writeLock);
        if (bufferSize() > 0) {
            enableChannel(MSNetChannelOpertaion.WRITE);
        }
    }
    private class WriteHandler implements MSNetChannelListener {
        @Override
        public void channelReady(MSNetChannelEvent e) {
            writeHandler();
        }
    }
    
    @VisibleForTesting
    void setupEstablishHandler(MSNetLoop disableLoop) throws MSNetEstablishException {
        final MSNetChannel c = _channel;
        if (c != null) {
            try {
                connectingLock.whileHolding(() -> {
                    c.setConnectListener(new ConnectEventHandler(connectingLock.scheduleAsyncEvent()));
                    enableChannel(MSNetChannelOpertaion.CONNECT);
                });
            } catch (IllegalStateException e) {
                throw new MSNetEstablishException(
                    "Failed to set up establish handler, the connection has probably been reset in the meantime -- "
                        + "connection state: " + getState(),
                    e);
            }
        }
    }
    
    private class ConnectEventHandler implements MSNetChannelListener {
        private final int eventId;
        private final AtomicInteger counter = new AtomicInteger();
        private ConnectEventHandler(int eventId) {
            this.eventId = eventId;
        }
        @Override
        public void channelReady(MSNetChannelEvent event) {
            
            
            if (!connectingLock.lockAsync(eventId)) {
                LOGGER.info("Event id invalid, skipping connect event. Expected {}, current {}.", eventId, connectingLock.getAsyncEventId());
                if (counter.incrementAndGet() > 5) {
                    MSNetChannel c = _channel;
                    SANITIZING_LOGGER.warn("Disabling connect. ConnectListener {}. Channel {}", connectListenerCurrent(c),
                            channelState(c));
                    disableConnect();
                }
                return;
            }
            assert connectingLock.assertsLockedByCurrentThread();
            try {
                
                disableConnect();
                finishConnect();
                establish(false, _readingEnabled);
                LOGGER.debug("Connection[{}] from {} to {} established. (MSNetTCPConnection:establishHandler)",
                        new Object[]{getName(), getLocalAddress(), getAddress()});
            } catch (MSNetException e) {
                connectFailed("establish() failed in EstablishHandler ( " + e.getMessage() + " )", e);
            } finally {
                connectingLock.unlockAsync();
                assert connectingLock.notHeldByCurrentThread();
            }
        }
        private void disableConnect() {
            final MSNetChannel c = _channel;
            if (null != c) {
                connectingLock.whileHolding(() -> {
                    disableChannel(MSNetChannelOpertaion.CONNECT);
                    c.setConnectListener(null);
                });
            }
        }
        private String connectListenerCurrent(MSNetChannel c) {
            return c == null ? "NA" : this == c.connectListener ? "not changed" : c.connectListener == null ? "removed" : "changed";
        }
        private String channelState(MSNetChannel c) {
            if (c == null) return "null";
            return String.valueOf(c.getSocket().getSelectableChannel());
        }
    }
    
    
    @Override
    public void connect(MSNetAddress addr) {
        setOverrideAddress(new MSNetInetAddress((MSNetInetAddress) addr));
        connect();
    }
    
    @Override
    public void connect() {
        initializeIfNotInitialized();
        if(!ensureNoConcurrentConnect(true)) {
            LOGGER.info("Skipping connect() - already done or underway");
            return;
        }
        try {
            assert isLockStateValid(connectingLock);
            connectingLock.lock();
            tryToConnect(false);
        } catch (InterruptedException e) {
            LOGGER.info("Interrupted waiting for connect lock.");
            Thread.currentThread().interrupt();
            return;
        } catch (MSNetConnectException e) {
            connectFailed(e.getMessage(), e);
        } finally {
            connectingLock.unlock();
            assert connectingLock.notHeldByCurrentThread();
        }
    }
    private void tryToConnect(boolean syncLoop) throws MSNetConnectException {
        assert isLockStateValid(this);
        assert connectingLock.assertsLockedByCurrentThread();
        MSNetInetAddress connectAddress = (MSNetInetAddress) getAddress();
        if (_socksProxyAddress != null) {
            connectAddress = _socksProxyAddress;
        }
        if (!syncLoop && _loop.getImpl() instanceof MSNetLoopNullImpl) {
            throw new MSNetConnectException("Attempted to connect to " + connectAddress
                    + " asynchronously via a NULL_IMPL netloop!");
        }
        if (!ensureNoConcurrentConnect(!syncLoop || connectNotificationOnSyncConnect)) {
            LOGGER.info("Already {} to {}. (sync={})", new Object[] { getState(), connectAddress, syncLoop });
            return;
        }
        try {
            connectToAddress(connectAddress, syncLoop);
        } catch (MSNetException e) {
            throw new MSNetConnectException("Attempted to connect to " + connectAddress + " failed (" + e.getMessage()
                    + ")", e);
        }
    }
    
    private boolean ensureNoConcurrentConnect(boolean notifyIfConnected) {
        MSNetConnectionStateEnum connState = getState();
        if (connState == CONNECTED) {
            if (notifyIfConnected) {
                
                connectNotify();
            }
            return false;
        } else if (connState == CONNECTING || connState == ESTABLISHING
                
                || connectingLock.hasScheduledEvent()) {
            
            
            if (_doRetry) {
                startRetry(connState);
            }
            return false;
        }
        return true;
    }
    
    private void connectToAddress(MSNetInetAddress addr, boolean syncLoop) throws MSNetConnectException,
            MSNetEstablishException {
        assert isLockStateValid(this);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Attempting to connect to {}. (sync={})", addr, syncLoop);
        }
        if ((addr == null) || (!addr.isValid())) {
            throw new MSNetConnectException("Invalid address '" + addr + "'");
        }
        addAsyncConnectTimeoutCallback();
        assert connectingLock.assertsLockedByCurrentThread();
        setState(CONNECTING);
        resetCalled = false;
        
        
        MSNetTCPSocket socket;
        assert isLockStateValid(_socketLock);
        synchronized (_socketLock) {
            if ((null == _socket) || (_socket.isValid() == false)) {
                try {
                    _socket = getSocketFactory().createMSNetTCPSocket(false);
                    _socket.setBlocking(false);
                    _socket.setKeepAlive(_keepAlive);
                    _socket.setTcpNoDelay(_tcpNoDelay);
                    _socket.setReceiveBufferSize(_receiveBufferSize);
                    _socket.setSendBufferSize(_sendBufferSize);
                } catch (MSNetIOException x) {
                    throw new MSNetConnectException("Could not construct a new socket.", x);
                }
                MSNetInetAddress bindAddress = null;
                
                if (_bindAddress != null) {
                    bindAddress = new MSNetInetAddress(_bindAddress);
                }
                
                else if (GLOBAL_BIND_ADDRESS != null && GLOBAL_BIND_ADDRESS.isValid()
                        && !addr.getInetAddress().isLoopbackAddress()) {
                    bindAddress = GLOBAL_BIND_ADDRESS;
                }
                if (bindAddress != null) {
                    try {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("Attempting to bind to {}", bindAddress);
                        }
                        _socket.bind(bindAddress);
                    } catch (IOException x) {
                        throw new MSNetConnectException("Could not bind socket to address (" + bindAddress + ")", x);
                    }
                }
            }
            socket = _socket;
            _channel = new MSNetChannel(getName().toString(), socket);
        }
        
        boolean connectionInProgress = false;
        try {
            MSNetConnectStatus conStatus = socket.connect(addr);
            if (conStatus.inError()) {
                MSNetException x = conStatus.getException();
                if ((x instanceof MSNetInProgressException)) {
                    
                    connectionInProgress = true;
                } else {
                    
                    throw new MSNetConnectException("Socket.connect() failed (" + x.getMessage() + ")", x);
                }
            }
        } catch (IOException x) {
            throw new MSNetConnectException(x);
        }
        if (connectionInProgress) {
            if (syncLoop == false) {
                
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Connection[{}] to {} in progress. Scheduling establish handler via [{}].",
                            new Object[]{getName(), addr, _loop});
                }
                setupEstablishHandler(_loop);
            }
            
            return;
        }
        
        establish(syncLoop, _readingEnabled);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Connection[{}] to {} established.", getName(), addr);
        }
    }
    private void finishConnect() throws MSNetIOException {
        
        
        MSNetTCPSocket socket = _socket;
        if (null == socket) {
            return;
        }
        boolean connected = socket.finishConnect();
        assert connected;
    }
    
    @VisibleForTesting
    void establish(boolean syncLoop, boolean enableAsyncRead) throws MSNetEstablishException {
        
        
        MSNetTCPSocket socket = _socket;
        if (null == socket) {
            throw new MSNetEstablishException("Socket already closed");
        }
        if (!socket.isConnected()) {
            throw new MSNetEstablishException("Socket not connected");
        }
        if (!socket.isValid()) {
            throw new MSNetEstablishException("Invalid socket");
        }
        if (getState() != CONNECTING) {
            throw new MSNetEstablishException("Invalid connection state (" + getState() + ")");
        }
        
        
        
        if (socket.getAddress().equals(socket.getLocalAddress())) {
            throw new MSNetEstablishException("Remote address equals to local address");
        }
        clearWriteList();
        
        _readBuffer.clear();
        for (ConnectionConfigurationManager man : MSNetConfiguration.getConfigurationManagers()) {
            man.beforeEstablish(this, establishers.keySet());
        }
        assert connectingLock.assertsLockedByCurrentThread() : connectingLock.getState();
        setState(ESTABLISHING);
        setupWriteHandler();
        boolean hasEstablisher = masterEstablisherMode != MasterEstablisherMode.OFF || !establishers.isEmpty();
        if (syncLoop) {
            
            if (hasEstablisher) {
                SyncEstablishHandler syncEstablishHandler = new SyncEstablishHandler(this, masterEstablisherMode, establishers.keySet());
                syncEstablishHandler.syncEstablisherLoop();
            }
            finishEstablish(true, enableAsyncRead);
        } else {
            
            if (!hasEstablisher) {
                
                finishEstablish(false, enableAsyncRead);
                return;
            }
            try {
                
                AsyncEstablishHandler establisherReadHandler =
                    new AsyncEstablishHandler(this, masterEstablisherMode, establishers.keySet(), enableAsyncRead,
                        connectingLock.scheduleAsyncEvent());
                
                _loop.callbackAfterDelay(0, establisherReadHandler);
                MSNetChannel c = _channel;
                if (null != c) {
                    c.setReadListener(establisherReadHandler);
                }
                enableChannel(MSNetChannelOpertaion.READ);
            } catch (IllegalStateException e) {
                throw new MSNetEstablishException(
                    "Failed to set up establish handler, the connection has probably been reset in the meantime -- "
                        + "connection state: " + getState(),
                    e);
            }
        }
    }
    private void asyncConnectTimeoutStateCallback(MSNetConnectionStateEnum state) {
        if (state == CONNECTED || state == DISCONNECTED || state == ABANDONED) {
            replaceAsyncTimeoutCallback(null);
        }
    }
    private void asyncConnectTimeoutRetryCallback() {
        replaceAsyncTimeoutCallback(null);
    }
    @Override
    protected void internalOnStateChanged(MSNetConnectionStateEnum state) {
        asyncConnectTimeoutStateCallback(state);
        super.internalOnStateChanged(state);
    }
    private void addAsyncConnectTimeoutCallback() {
        final long timeout = asyncConnectTimeout;
        if (timeout > 0) {
            final Object callbackId = _loop.callbackAfterDelay(timeout, new MSNetEventListener() {
                @Override
                public void eventOccurred(MSNetEvent e) {
                    if (!isConnected()) {
                        LOGGER.info("Async connect timed out after {} ms...", timeout);
                        MSNetChannel c = _channel;
                        if (c != null) {
                            c.setReadListener(null);
                        }
                        connectFailed();
                        connectTimeoutNotify();
                    }
                }
            });
            replaceAsyncTimeoutCallback(callbackId);
        }
    }
    private void replaceAsyncTimeoutCallback(final Object callbackId) {
        Object oldId = asyncConnectTimeoutCallbackId.getAndSet(callbackId);
        if (oldId!= null)
            _loop.cancelCallback(oldId);
    }
    void finishEstablish(boolean syncLoop, boolean enableAsyncRead) throws MSNetEstablishException {
        
        MSNetTCPSocket socket = _socket;
        if (socket == null) {
            throw new MSNetEstablishException("Socket was closed before the connection was established.");
        }
        _connectTime = new Date(); 
        _established = true;
        
        
        
        
        if (_stateBeforeConnectionReset != CONNECTED) {
            _retryTime = 0;
            _numRetries = 0;
        }
        assert connectingLock.assertsLockedByCurrentThread();
        setState(CONNECTED);
        
        clearRetryHandler();
        setupReadHandler(_loop, enableAsyncRead);
        
        
        
        if (_authContext != null) {
            MSNetAuditLogger.LOGGER.info("User = {}; Auth Mechanism = {}; Status = {}",
                    new Object[]{_authContext.getAuthID(),
                            _authContext.getAuthMechanism(),
                            _authContext.getAuthStatus().toString()});
        }
        ConnectionReporter.reportConnection(this);
        MSNetMateExtractor.INSTANCE.onConnect(this, authenticationPublisher());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Connection[{}] from {} to {} established. (MSNetTCPConnection:finishEstablish)",
                    new Object[]{getName(), socket.getLocalAddress(), socket.getAddress()});
        }
        
        updateReadingEnabled();
        
        if (timeoutScheduler != null && timeoutScheduler.isTimeoutSet()) {
            timeoutScheduler.start();
        }
        
        if (!syncLoop || connectNotificationOnSyncConnect) {
            connectNotify();
        }
        if (_metrics != null) {
            _metrics.gotConnected();
        }
    }
    @VisibleForTesting
    Publisher authenticationPublisher() {
        return MateWTPublisher.INSTANCE;
    }
    
    protected void clearWriteList() {
        assert isLockStateValid(_writeLock);
        synchronized (_writeLock) {
            _writeList.clear();
            _writeBufferSize = 0;
            _currentWritten = 0;
        }
    }
    
    @VisibleForTesting
    void retryHandler() {
        try {
            try {
                int i = 0;
                do {
                    if (!shouldRetry()) {
                        return;
                    }
                    if (i == 10) {
                        LOGGER.warn("Couldn't acquire connecting lock in 100 ms in retry handler. Scheduling another retry");
                        
                        setupRetryHandler(0);
                        return;
                    }
                    ++i;
                } while (!connectingLock.tryLock(10, MILLISECONDS));
            } catch (InterruptedException e) {
                LOGGER.info("Interrupted waiting for connect lock (retryHandler).");
                Thread.currentThread().interrupt();
                return;
            }
            if (!shouldRetry()) {
                return;
            }
            try {
                assert isLockStateValid(connectingLock);
                assert connectingLock.assertsLockedByCurrentThread();
                retryNotify();
                resolveAddr();
                tryToConnect(false);
            } catch (MSNetException | MSNetRuntimeException e) {
                connectFailed(e.getMessage(), e);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Connect failed in retryHandler", e);
                }
            }
        } finally {
            if (connectingLock.isHeldByCurrentThread()) {
                connectingLock.unlock();
                assert connectingLock.notHeldByCurrentThread();
            }
        }
        if (_metrics != null) {
            _metrics.didRetry();
        }
    }
    private boolean shouldRetry() {
        MSNetConnectionStateEnum state = getState();
        if (state == CONNECTED || state == CONNECTING || state == ESTABLISHING) {
            LOGGER.debug("Already {}, won't retry", state);
            return false;
        }
        if (!_doRetry) {
            LOGGER.debug("The retry flag has been unset since the retry was scheduled, "
                    + "skipping retry and signalling connection close.");
            safeCloseNotifyInternal(); 
            return false;
        }
        
        if (_retryKey == null) {
            LOGGER.debug("The retry task has been cancelled since the retry was scheduled, "
                    + "skipping retry and signalling connection close.");
            safeCloseNotifyInternal(); 
            return false;
        }
        
        if (connectingLock.hasScheduledEvent()) {
            
            
            
            LOGGER.debug("Ignoring request for retry since an establish has already been scheduled. Let that fail or succeed and produce retry");
            return false;
        }
        return true;
    }
    
    protected void resolveAddr() {
        ((MSNetInetAddress) getAddress()).resolve();
    }
    private void connectFailed(String errorMsg, Exception e) {
        logConnectError("Connection[" + getName() + "] to " + getAddress() + " failed: " + errorMsg, e);
        connectFailed();
    }
    private void connectFailed() {
        if (_numRetries == _maxRetries) {
            LOGGER.error("Max retries ({}) reached. Abandoning retry.", _maxRetries);
            resetForError(MSNetConnectionErrorEnum.CONNECT_FAILED);
            setState(ABANDONED);
        } else {
            resetConnection();
        }
    }
    protected void resetForError(MSNetConnectionErrorEnum e) {
        MSNetConnectionStateEnum s = getState();
        if (s == DISCONNECTED || s== ABANDONED) {
            return;
        }
        
        assert connectingLock.assertsLockedByCurrentThread() || isLockStateValid(connectingLock);
        boolean release = connectingLock.lockForReset();
        try {
            assert connectingLock.assertsLockedByCurrentThread();
            s = getState();
            if (s == DISCONNECTED || s== ABANDONED) {
                return;
            }
            if (_metrics != null) {
                _metrics.gotDisconnected();
            }
            
            
            resetConnection(true, true, null);
        } finally {
            if (release) {
                connectingLock.releaseResetLock();
                assert connectingLock.notHeldByCurrentThread();
            }
        }
        errorNotify(e);
        
        
        disconnectNotify();
    }
    @VisibleForTesting
    void resetForDisconnect() {
        if (getState() == DISCONNECTED) {
            return;
        }
        
        assert connectingLock.assertsLockedByCurrentThread() || isLockStateValid(connectingLock);
        boolean release = connectingLock.lockForReset();
        try {
            assert connectingLock.assertsLockedByCurrentThread();
            if (getState() == DISCONNECTED) {
                return;
            }
            ++_numDisconnects;
            if (_metrics != null) {
                _metrics.gotDisconnected();
            }
            boolean abandon = false;
            
            if (_maxDisconnects != InfiniteDisconnects && _numDisconnects >= _maxDisconnects) {
                LOGGER.error("Maximum disconnects ({}) reached; abandoning retry", _maxDisconnects);
                _doRetry = false;
                abandon = true;
            }
            
            
            resetConnection(true, true, null);
            if (abandon) {
                setState(ABANDONED);
            }
        } finally {
            if (release) {
                connectingLock.releaseResetLock();
            }
            assert connectingLock.notHeldByCurrentThread();
        }
        disconnectNotify();
    }
    
    public final void resetConnection() {
        resetConnection(true, false, null);
    }
    
    protected void beforeReset(){
        
    }
    
    protected void afterReset(){
        
    }
    
    private void resetConnection(boolean clearRetry, boolean disconnect, MSNetConnectionStateEnum connState) {
        boolean didRetry = false;
        try {
            resetCalled = true;
            beforeReset();
            MSNetConnectionStateEnum savedState = connState == null ? getState() : connState;
            _stateBeforeConnectionReset = savedState;
            cleanup(clearRetry);
            if (_doRetry) {
                startRetry(savedState);
                didRetry = true;
            }
            afterReset();
        } finally {
            if (!didRetry) {
                if (disconnect) {
                    safeDisconnectNotifyInternal();
                } else {
                    safeCloseNotifyInternal();
                }
            }
        }
    }
    private void startRetry(MSNetConnectionStateEnum previousState) {
        if ((getOverrideAddress() == null) && (getPrimaryAddress() == null) && (getBackupAddress() == null)) {
            LOGGER.error("All addresses are invalid");
            resetForError(MSNetConnectionErrorEnum.ADDRESS_ERROR);
            
            
        }
        
        
        
        
        
        
        
        
        
        if (previousState != CONNECTED) {
            addrs.failOver();
        }
        if ((_maxRetries == InfiniteRetries) || (_numRetries < _maxRetries)) {
            long nextRetryTime = increaseRetryTime();
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Will retry '{}' in {} milliseconds.", getAddress(), nextRetryTime);
            }
            setupRetryHandler(nextRetryTime);
        } else {
            safeCloseNotifyInternal(); 
        }
    }
    
    @SuppressWarnings("deprecation")
    protected long increaseRetryTime() {
        ++_numRetries;
        if (_retryTime == 0) {
            setRetryTime(Math.min(getInitialRetryTime(), _maxRetryTime));
        } else {
            setRetryTime(Math.min(_retryTime * 2, _maxRetryTime));
        }
        return getRetryTime();
    }
    
    @Override
    synchronized public void close() {
        assert isLockStateValid(this);
        
        
        
        try {
            cleanup(true);
        } finally {
            safeCloseNotifyInternal();
        }
    }
    
    public void syncClose(final boolean doDisconnectNotify, final long timeoutMs) throws InterruptedException, TimeoutException {
        final long startTime = System.currentTimeMillis();
        if (!_writeList.isEmpty()) {
            
            final MSNetIOStatus status;
            assert isLockStateValid(_writeLock);
            synchronized (_writeLock) {
                status = syncWriteSelectLoop(timeoutMs - (System.currentTimeMillis() - startTime));
            }
            try {
                status.raise();
            } catch (MSNetInterruptedException e) {
                throw new InterruptedException();
            } catch (MSNetSelectTimeoutException e) {
                TimeoutException te = new TimeoutException("Timed out in syncWriteSelectLoop");
                te.initCause(e);
                throw te;
            } catch (MSNetException e) {
                LOGGER.error("Error writing leftover data from NetLoop thread before SyncClose"
                        + " - continuing with the close, rest of the data will be lost", e);
            }
        }
        if (timeoutMs - (System.currentTimeMillis() - startTime) <= 0) {
            throw new TimeoutException("Timed out after sending was completed but before cleanup");
        }
        
        assert isLockStateValid(this);
        MSNetTCPSocket oldSock;
        synchronized (this) {
            try {
                oldSock = cleanup(true);
            } finally {
                safeCloseNotifyInternal();
            }
        }
        
        if (oldSock != null) {
            while (!oldSock.isCloseFinished()) {
                if (timeoutMs - (System.currentTimeMillis() - startTime) <= 0) {
                    
                    if (doDisconnectNotify) {
                        Thread closer = new Thread(() -> {
                            MSThreadMonitor.instance().register(10000);
                            try {
                                while (!oldSock.isCloseFinished()) {
                                    Thread.yield();
                                }
                                if (doDisconnectNotify) {
                                    disconnectNotify();
                                }
                            } finally {
                                MSThreadMonitor.instance().unregister();
                            }
                        }, "MSNetTCPConnection-CloseFinishedWaiter-" + getName());
                        closer.setDaemon(true);
                        closer.start();
                    }
                    throw new TimeoutException(
                            "Timed out waiting for socket to close, the remaining cleanup will be done asynchronously. Socket state: "
                                    + oldSock);
                }
                Thread.yield();
            }
        }
        if (doDisconnectNotify) {
            disconnectNotify();
        }
    }
    
    synchronized public void disconnect() {
        assert isLockStateValid(this);
        try {
            cleanup(true);
        } finally {
            safeDisconnectNotifyInternal();
        }
        disconnectNotify();
    }
    private void closeSocketInLoopThread(final MSNetTCPSocket socket) {
        try {
            _syncReadWriteLock.writeLock().lock();
            if (inLoopThread() || (loopIsLooping() == false)) {
                doCloseSocket(socket);
            } else {
                
                
                
                
                
                
                
                _isAsyncSocketCloseInProgress = true;
                getNetLoop().callbackAfterDelay(0, new MSNetEventListener() {
                    @Override
                    public void eventOccurred(MSNetEvent e_) {
                        doCloseSocket(socket);
                        synchronized (_asyncSocketCloseCompleteLock) {
                            _isAsyncSocketCloseInProgress = false;
                            _asyncSocketCloseCompleteLock.notifyAll();
                        }
                    }
                });
            }
        } finally {
            _syncReadWriteLock.writeLock().unlock();
        }
    }
    @VisibleForTesting
    void doCloseSocket(MSNetTCPSocket socketToClose) {
        if (socketToClose == null) {
            return;
        }
        
        
        
        
        
        
        
        
        
        
        
        MSNetTCPSocket socket = _socket;
        if (socket == null || socket.equals(socketToClose)) {
            if (null != _syncWriteMultiplexer) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Closing syncWriteMultiplexer");
                }
                _syncWriteMultiplexer.close();
                _syncWriteMultiplexer = null;
            }
            if (null != _syncReadMultiplexer) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Closing syncReadMultiplexer");
                }
                _syncReadMultiplexer.close();
                _syncReadMultiplexer = null;
            }
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Closing socket {}.", socketToClose.getAddress());
        }
        try {
            socketToClose.close();
        } catch (IOException e) {
            LOGGER.warn("IOException occurred while closing socketChannel", e);
        }
        
        
        
        
        
        
        
        
        
        
        
    }
    private MSNetTCPSocket closeSocket() {
        assert isLockStateValid(_socketLock);
        synchronized (_socketLock) {
            if (null != _socket) {
                MSNetTCPSocket oldSock = _socket;
                _socket = null;
                _channel = null;
                closeSocketInLoopThread(oldSock);
                return oldSock;
            } else {
                return null;
            }
        }
    }
    @Override
    public MSNetAddress getLocalAddress() {
        MSNetTCPSocket socket = getSocket();
        if (socket == null)
            return null;
        return socket.getLocalAddress();
    }
    
    private final MSNetTCPSocket cleanup(boolean clearRetry) {
        beforeCleanup(clearRetry);
        MSNetTCPSocket oldSock = doCleanup(clearRetry);
        afterCleanup(clearRetry);
        
        connectingLock.clear();
        return oldSock;
    }
    
    protected void beforeCleanup(boolean clearRetry) {
        
    }
    
    protected void afterCleanup(boolean clearRetry) {
        
    }
    
    @SuppressWarnings("deprecation")
    private MSNetTCPSocket doCleanup(boolean clearRetry) {
        disableChannel(MSNetChannelOpertaion.ALL_OPS);
        MSNetTCPSocket oldSock = closeSocket();
        _established = false;
        _didConnectCallback.set(false);
        _authContext = null;
        if (clearRetry) {
            
            
            
            clearRetryHandler();
        }
        if (MSNetConfiguration.USE_NEW_CALLBACK_STRATEGY) {
            setState(DISCONNECTED);
        } else {
            setStateWithoutNotification(DISCONNECTED);
            
            
            connectingLock.clear(); 
            notifyStateChanged(DISCONNECTED);
        }
        
        for (MSNetEstablisher e : establishers.keySet()) {
            try {
                e.cleanup();
            } catch (Exception ex) {
                LOGGER.error("establisher.cleanup() threw an exception", ex);
            }
        }
        return oldSock;
    }
    
    protected void appendToWriteList(MSNetTCPSocketBuffer buffer) {
        assert Thread.holdsLock(_writeLock);
        _writeList.add(buffer);
        _writeBufferSize += buffer.size();
        if (_writeBufferSize > _writeListThreshold) {
            writeListThresholdReachedNotify(_writeBufferSize);
        }
        
        
    }
    
    boolean isLockStateValid(Object lock) {
        boolean ret = false;
        String name = "UNKNOWN LOCK: " + lock;
        if (lock == this) {
            ret = !Thread.holdsLock(_socketLock) && !Thread.holdsLock(_writeLock);
            name = "this";
        } else if (lock == connectingLock) {
            ret = !Thread.holdsLock(_writeLock) && !Thread.holdsLock(_socketLock);
            name = "_connectingLock";
        } else if (lock == _writeLock) {
            ret = !Thread.holdsLock(_socketLock);
            name = "_writeLock";
        } else if (lock == _socketLock) {
            
            
            ret = true;
            name = "_socketLock";
        }
        String msg = null;
        if (!ret) {
            msg = "Invalid lock state for " + name + ": " + "this=" + Thread.holdsLock(this)
                    + "  _writeLock=" + Thread.holdsLock(_writeLock)
                    + "  _socketLock=" + Thread.holdsLock(_socketLock);
        }
        if (Thread.holdsLock(connectingLock)) {
            String connectingLockMsg = "MSNetConnectingLock should not be used as a monitor.";
            msg = (msg == null) ? connectingLockMsg : (msg + ". " + connectingLockMsg);
        }
        if (msg != null) {
            LOGGER.error(msg);
            assert false : msg;
            return false;
        } else {
            return true;
        }
    }
    private void writeHandler() {
        MSNetIOStatus ioStatus = null;
        assert isLockStateValid(_writeLock);
        synchronized (_writeLock) {
            ioStatus = _doWriteWithoutErrorHandling();
        }
        handleWriteErrorIfAny(ioStatus);
    }
    
    MSNetIOStatus _doWriteWithoutErrorHandling() {
        assert Thread.holdsLock(_writeLock);
        long totalToWrite = bufferSize();
        MSNetIOStatus ioStatus = doRawWriteWithoutErrorHandling(_writeList, getTapping());
        int total = ioStatus.getNumBytesProcessed();
        if (total == -1 || total <= 0 && totalToWrite > 0) {
            return ioStatus;
        }
        _currentWritten += total;
        _writeBufferSize -= total;
        int numFinished = 0;
        for (MSNetTCPSocketBuffer buf : _writeList) {
            
            
            
            if (buf.size() == 0) {
                if (buf.isLastBufferInMessage()) {
                    if (_metrics != null) {
                        _metrics.sentBytes(_currentWritten);
                    }
                    _currentWritten = 0;
                    sendDoneNotify();
                }
                numFinished++;
            } else {
                break;
            }
        }
        
        if (numFinished == _writeList.size()) {
            _writeList.clear();
        } else {
            for (int i = 0; i < numFinished; i++) {
                _writeList.remove(0);
            }
        }
        
        if (bufferSize() == 0) {
            disableChannel(MSNetChannelOpertaion.WRITE);
        }
        return ioStatus;
    }
    private void disableChannel(MSNetChannelOpertaion op) {
        MSNetChannel c = _channel;
        MSNetLoop loop = _loop;
        if (c != null && c.isEnabled(op)) {
            c.disable(op);
            if (null != loop) {
                if (c.getOps() != 0) {
                    
                    loop.registerChannel(c);
                } else {
                    loop.unregisterChannel(c);
                }
            }
        }
    }
    void enableChannel(MSNetChannelOpertaion op) {
        MSNetChannel c = _channel;
        MSNetLoop loop = _loop;
        if (c != null && !c.isEnabled(op) && loop != null) {
            c.enable(op);
            
            loop.registerChannel(c);
        }
    }
    
    int doRawWrite(MSNetTCPSocketBuffer writeBuffer, boolean doTap) {
        List<MSNetTCPSocketBuffer> l = new LinkedList<MSNetTCPSocketBuffer>();
        l.add(writeBuffer);
        return doRawWrite(l, doTap);
    }
    
    private int doRawWrite(List<MSNetTCPSocketBuffer> writeBuffers, boolean doTap) {
        MSNetIOStatus ioStatus = doRawWriteWithoutErrorHandling(writeBuffers, doTap);
        handleWriteErrorIfAny(ioStatus);
        return ioStatus.getNumBytesProcessed();
    }
    
    
    
    
    private MSNetIOStatus doRawWriteWithoutErrorHandling(List<MSNetTCPSocketBuffer> writeBuffers, boolean doTap) {
        if (writeBuffers.isEmpty()) {
            return new MSNetIOStatus();
        }
        MSNetIOStatus ioStatus = null;
        
        MSNetTCPSocket socket = _socket;
        if (null == socket) {
            ioStatus = new MSNetIOStatus(new MSNetIOException("Not connected (detected while writing)"));
        } else {
            try {
                ioStatus = socket.write(writeBuffers);
                
            } catch (Exception x) {
                ioStatus = new MSNetIOStatus(new MSNetIOException("Error writing", x));
            }
        }
        if (ioStatus.inError()) {
            if (!(ioStatus.getException() instanceof MSNetResourceBusyException)) {
                ioStatus.setNumBytesProcessed(-1);
                return ioStatus;
            }
        } else if (getState() != ESTABLISHING) {
            resetRetryTimeAndNumOfRetries();
        }
        int numWritten = ioStatus.getNumBytesProcessed();
        
        
        int x = numWritten;
        for (MSNetTCPSocketBuffer buf : writeBuffers) {
            if (x == 0) {
                break;
            }
            int thisSize = buf.size();
            if (x < thisSize) {
                thisSize = x;
            }
            if (doTap && (thisSize > 0)) {
                tapNotify(buf.peek(thisSize), TAP_WRITE, getAddress());
            }
            buf.processed(thisSize, false);
            x -= thisSize;
        }
        return ioStatus;
    }
    
    private boolean handleWriteError(MSNetException x) {
        if (x instanceof MSNetResourceBusyException) {
            
            
            return true;
        } else if (x instanceof MSNetSocketEndPipeException) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Socket disconnected from address: {}.", getAddress());
            }
            resetForDisconnect();
        } else if (x instanceof MSNetIOException) { 
            LOGGER.error("Write error for address " + getAddress() + ": " + x, x);
            resetForError(MSNetConnectionErrorEnum.WRITE_ERROR);
        } else if (x instanceof MSNetInvalidFDException) {
            LOGGER.error("Invalid fd, resetting...");
            resetForError(MSNetConnectionErrorEnum.WRITE_ERROR);
        } else {
            LOGGER.error("Cannot write to " + getAddress() + ": " + x, x);
            resetForError(MSNetConnectionErrorEnum.WRITE_ERROR);
        }
        return false;
    }
    
    protected void logWriteError(String msg, Exception e) {
        
        if (LOGGER.isDebugEnabled()) {
            LOGGER.error(msg, e);
        } else {
            LOGGER.error(msg);
        }
    }
    
    
    @Override
    @CheckReturnValue
    public boolean syncConnect(MSNetAddress addr, long millisec) {
        setOverrideAddress(addr);
        return syncConnect(millisec);
    }
    
    @Override
    @CheckReturnValue
    public boolean syncConnect(long millisec) {
        initializeIfNotInitialized();
        long expiration = System.currentTimeMillis() + millisec;
        MSNetConnectionStateEnum connState = getState();
        if (connState == CONNECTING || connState == CONNECTED || connState == ESTABLISHING) {
            LOGGER.info("Already {}.", connState);
        }
        
        try {
            if (!connectingLock.tryLock(millisec, MILLISECONDS)) {
                
                
                return false;
            }
        } catch (InterruptedException e) {
            LOGGER.info("Cannot syncConnect to ({}), interrupted waiting for connect lock.", getAddress());
            Thread.currentThread().interrupt();
            return false;
        }
        clearRetryHandler();
        
        boolean success = false;
        boolean reconnect = false;
        try {
            
            if (getState() == CONNECTED) {
                return (success = true);
            }
            
            
            
            tryToConnect(true);
            if (getState() == CONNECTING) {
                
                assert isLockStateValid(_writeLock);
                long timeLeft = expiration - System.currentTimeMillis();
                synchronized (_writeLock) {
                    
                    
                    
                    
                    
                    syncConnectSelectLoop(timeLeft);
                    
                    establish(true, _readingEnabled);
                    updateReadingEnabled();
                }
            }
            return (success = (getState() == CONNECTED));
        } catch (MSNetConnectException e) {
            
            logSyncConnectError("connect failed", e);
        } catch (MSNetReconnectException e) {
            
            reconnect = true;
            logSyncConnectError("establish failed", e);
        } catch (MSNetEstablishException e) {
            
            logSyncConnectError("establish failed", e);
        } catch (MSNetSelectTimeoutException e) {
            
            logSyncConnectError(millisec + "ms timeout expired. This could be due to prodperim. More info here: http://optimusguide/OptimusCoreDAL/DalEntityFAQ#why-couldnt-my-app-connect-to-the-dal", e);
        } catch (MSNetException e) {
            
            logSyncConnectError("connect failed", e);
        } finally {
            connectingLock.unlock();
            assert connectingLock.notHeldByCurrentThread();
            if (!success) {
                
                
                if (!resetCalled) {
                    
                    
                    resetConnection(false, false, connState);
                }
            }
        }
        if (reconnect) {
            return syncConnect(millisec);
        }
        return false;
    }
    private void logSyncConnectError(String msg, Exception t) {
        logConnectError("Cannot syncConnect to (" + getAddress() + "), " + msg +  ": " + t.getMessage(), t);
    }
    protected void logConnectError(String msg, Exception t) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.error(msg, t);
        } else {
            LOGGER.error(msg);
        }
    }
    private void initializeIfNotInitialized() {
        try {
            afterPropertiesSet();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }
    
    private void syncConnectSelectLoop(long timeout) throws MSNetException {
        assert Thread.holdsLock(_writeLock);
        assert connectingLock.assertsLockedByCurrentThread();
        long startTime = System.currentTimeMillis();
        long endTime = startTime + timeout;
        
        
        MSNetTCPSocket socket = _socket;
        if (socket == null) {
            throw new MSNetSocketEndPipeException("Error in syncConnect(); socket closed");
        }
        
        
        
        
        MSNetMultiplexer mxr = new MSNetMultiplexer();
        try 
        {
            while (true) {
                long curTime = System.currentTimeMillis();
                long mxrTimeout = curTime > endTime ? 0 : endTime - curTime;
                mxr.register(socket, MSNetChannelOpertaion.CONNECT.selectionKey);
                MSNetMultiplexStatus status = mxr.multiplex(mxrTimeout);
                if (status.timedOut()) {
                    throw new MSNetSelectTimeoutException(timeout);
                }
                if (status.isOk() == false) {
                    MSNetException x = status.getException();
                    if (x instanceof MSNetResourceBusyException) {
                        
                    } else if (x instanceof MSNetSelectInterruptException) {
                        interruptNotify();
                    } else {
                        throw x;
                    }
                }
                if (mxr.isEventPending(socket, MSNetChannelOpertaion.CONNECT.selectionKey)) {
                    boolean connected = socket.finishConnect();
                    assert connected;
                    break;
                }
                if (curTime > endTime) {
                    throw new MSNetSelectTimeoutException(timeout);
                }
            }
        } finally {
            mxr.close();
        }
    }
    
    public MSNetIOStatus syncSend(MSNetTCPSocketBuffer buffer, long millisec) {
        timeoutScheduler.logWorkStart();
        try {
            return syncSendBuffer(buffer, millisec);
        } finally {
            timeoutScheduler.logWorkEnd();
        }
    }
    
    public MSNetIOStatus syncSend(MSNetTCPSocketBuffer buffers[], long millisec) {
        timeoutScheduler.logWorkStart();
        try {
            return syncSendBuffers(buffers, millisec);
        } finally {
            timeoutScheduler.logWorkEnd();
        }
    }
    
    @Override
    public MSNetIOStatus syncSend(byte[] buffer, int size, long ms) {
        timeoutScheduler.logWorkStart();
        try {
            return syncSendBuffer(new MSNetTCPSocketBuffer(buffer, 0, size), ms);
        } finally {
            timeoutScheduler.logWorkEnd();
        }
    }
    
    @Override
    public MSNetIOStatus syncSend(MSNetMessage msg, long millisec) {
        timeoutScheduler.logWorkStart();
        try {
            byte b[] = msg.getBytes();
            MSNetTCPSocketBuffer buf = new MSNetTCPSocketBuffer(b, 0, b.length);
            MSNetIOStatus buffer = syncSendBuffer(buf, millisec);
            return buffer;
        } finally {
            timeoutScheduler.logWorkEnd();
        }
    }
    
    protected MSNetIOStatus syncSendBuffer(MSNetTCPSocketBuffer buffer, long millisec) {
        return _syncSendBuffer(buffer, millisec);
    }
    
    protected MSNetIOStatus syncSendBuffers(MSNetTCPSocketBuffer buffers[], long millisec) {
        return _syncSendBuffers(buffers, millisec);
    }
    private MSNetIOStatus _syncSendBuffers(MSNetTCPSocketBuffer buffers[], long millisec) {
        int totalBytes = 0;
        boolean isLastBufferInMessage = false;
        for (int i = 0; i < buffers.length; i++) {
            totalBytes += buffers[i].size();
            if (buffers[i].isLastBufferInMessage()) {
                isLastBufferInMessage = true;
            }
        }
        if (totalBytes <= 0 && !isLastBufferInMessage) {
            return new MSNetIOStatus(totalBytes, 0, new MSNetException(
                    "MSNetTCPConnection.syncSendBuffer(): syncSend failed: Attempt to send invalid buffer"));
        }
        
        MSNetTCPSocket socket = _socket;
        if ((null == socket) || (!socket.isValid())) {
            return new MSNetIOStatus(0, 0, new MSNetException("Invalid socket"));
        }
        MSNetIOStatus ioStatus = null;
        assert isLockStateValid(_writeLock);
        synchronized (_writeLock) {
            for (int i = 0; i < buffers.length; i++) {
                appendToWriteList(buffers[i]);
            }
            long numBytesQueued = _writeBufferSize;
            ioStatus = syncWriteSelectLoop(millisec);
            ioStatus.setNumBytesInMessage(totalBytes);
            int numBytesProcessed = ioStatus.getNumBytesProcessed();
            if (numBytesProcessed > numBytesQueued) {
                
                
                
                ioStatus.setNumBytesProcessed((int) (numBytesProcessed - numBytesQueued));
            }
        }
        handleReadErrorIfAny(ioStatus);
        handleWriteErrorIfAny(ioStatus);
        return ioStatus;
    }
    MSNetIOStatus _syncSendBuffer(MSNetTCPSocketBuffer buffer, long millisec) {
        if (buffer.size() <= 0) {
            return new MSNetIOStatus(buffer.size(), 0, new MSNetException(
                    "MSNetTCPConnection.syncSendBuffer(): syncSend failed: Attempt to send invalid buffer"));
        }
        
        MSNetTCPSocket socket = _socket;
        if ((null == socket) || (!socket.isValid())) {
            return new MSNetIOStatus(0, 0, new MSNetException("Invalid socket"));
        }
        MSNetIOStatus ioStatus = null;
        assert isLockStateValid(_writeLock);
        synchronized (_writeLock) {
            long numBytesQueued = _writeBufferSize;
            int bufferSize = buffer.size();
            appendToWriteList(buffer);
            ioStatus = syncWriteSelectLoop(millisec);
            ioStatus.setNumBytesInMessage(bufferSize);
            int numBytesProcessed = ioStatus.getNumBytesProcessed();
            if (numBytesProcessed > numBytesQueued) {
                
                
                
                ioStatus.setNumBytesProcessed((int) (numBytesProcessed - numBytesQueued));
            }
        }
        handleReadErrorIfAny(ioStatus);
        handleWriteErrorIfAny(ioStatus);
        return ioStatus;
    }
    
    private MSNetIOStatus syncWriteSelectLoop(long timeout) {
        assert Thread.holdsLock(_writeLock);
        int wrote = 0;
        final long endTime = System.currentTimeMillis() + timeout;
        
        
        
        
        MSNetIOStatus endStatus = null;
        try { 
            _syncReadWriteLock.readLock().lock();
            long currentTime = 0;
            
            synchronized (_asyncSocketCloseCompleteLock) {
                while (_isAsyncSocketCloseInProgress && (currentTime = System.currentTimeMillis()) < endTime) {
                    try {
                        _asyncSocketCloseCompleteLock.wait(endTime - currentTime);
                    } catch (InterruptedException e) {
                        LOGGER.info("Interrupted waiting for async socket close complete lock.");
                        Thread.currentThread().interrupt();
                        return new MSNetIOStatus(new MSNetInterruptedException());
                    }
                }
                if (_isAsyncSocketCloseInProgress) {
                    return new MSNetIOStatus(new MSNetSelectTimeoutException(timeout));
                }
            }
            if (_syncWriteMultiplexer == null) {
                _syncWriteMultiplexer = new MSNetMultiplexer();
            }
            MSNetMultiplexer mxr = _syncWriteMultiplexer;
            
            MSNetMultiplexStatus status;
            long timeleft;
            try { 
                for (; ; ) {
                    timeleft = endTime - System.currentTimeMillis();
                    if (timeleft <= 0) {
                        
                        
                        
                        
                        
                        if (!_writeList.isEmpty()) {
                            enableChannel(MSNetChannelOpertaion.WRITE);
                        }
                        return new MSNetIOStatus(0, wrote, new MSNetSelectTimeoutException(timeout,
                                "Timeout went to 0."));
                    }
                    
                    MSNetTCPSocket socket = _socket;
                    if (socket == null) {
                        return new MSNetIOStatus(new MSNetSocketEndPipeException(
                                "Error in syncSend(); socket closed"));
                    }
                    
                    
                    
                    
                    
                    
                    
                    
                    mxr.clearAllPendingEvents();
                    try {
                        mxr.register(socket, MSNetChannelOpertaion.WRITE.selectionKey);
                        
                        if (!isReadingEnabled()) {
                            mxr.register(socket, MSNetChannelOpertaion.READ.selectionKey);
                        }
                    } catch(MSNetException e) {
                        LOGGER.error("Failed to register multiplexer", e);
                        MSNetIOStatus resultStatus = new MSNetIOStatus(0, wrote, e);
                        resultStatus.setWriteErrorInSyncWriteSelectLoop(true);
                        return resultStatus;
                    }
                    status = mxr.multiplex(timeleft);
                    if (status.timedOut() && endTime < System.currentTimeMillis()) {
                        
                        
                        if (!_writeList.isEmpty()) {
                            enableChannel(MSNetChannelOpertaion.WRITE);
                        }
                        return new MSNetIOStatus(0, wrote, new MSNetSelectTimeoutException(timeout,
                                "Multiplexing timed out."));
                    }
                    
                    
                    
                    
                    
                    if (!status.isOk()) {
                        if (status.getException() instanceof MSNetResourceBusyException) {
                            continue;
                        }
                        try {
                            status.raise();
                        } catch (MSNetSelectInterruptException e) {
                            interruptNotify();
                        } catch (MSNetInvalidFDException e) {
                            return new MSNetIOStatus(0, wrote, e);
                        } catch (MSNetException e) {
                            
                            return new MSNetIOStatus(0, wrote, e);
                        }
                    }
                    
                    
                    if (!isReadingEnabled() && mxr.isEventPending(socket, MSNetChannelOpertaion.READ.selectionKey)) {
                        
                        MSNetIOStatus ioStatus = doRawReadWithoutErrorHandling();
                        if (ioStatus.inError()) {
                            endStatus = ioStatus;
                            endStatus.setReadErrorInSyncWriteSelectLoop(true);
                            break;
                        }
                        int read = ioStatus.getNumBytesProcessed();
                        if (_metrics != null) {
                            _metrics.receivedBytes(read);
                        }
                        
                        
                        
                        
                        if (-1 != read) {
                            ioStatus = doRawReadWithoutErrorHandling();
                            if (ioStatus.inError()) {
                                endStatus = ioStatus;
                                endStatus.setReadErrorInSyncWriteSelectLoop(true);
                                break;
                            }
                            read = ioStatus.getNumBytesProcessed();
                            if (_metrics != null) {
                                _metrics.receivedBytes(read);
                            }
                        }
                    }
                    
                    if (mxr.isEventPending(socket, MSNetChannelOpertaion.WRITE.selectionKey)) {
                        MSNetIOStatus ioStatus = _doWriteWithoutErrorHandling();
                        if (ioStatus.getNumBytesProcessed() == -1) {
                            endStatus = ioStatus;
                            endStatus.setWriteErrorInSyncWriteSelectLoop(true);
                            endStatus.setNumBytesProcessed(wrote);
                            break;
                        }
                        wrote += ioStatus.getNumBytesProcessed();
                    }
                    MSNetConnectionStateEnum connectionState = getState();
                    if (connectionState != CONNECTED && connectionState != CONNECTING && connectionState != ESTABLISHING) {
                        return new MSNetIOStatus(0, wrote, new MSNetException("Error in syncSend; broken connection"));
                    }
                    
                    if (_writeList.isEmpty()) {
                        return new MSNetIOStatus(0, wrote);
                    }
                }
            } finally {
                
                
                mxr = null;
            }
        } finally {
            
            _syncReadWriteLock.readLock().unlock();
        }
        return endStatus;
    }
    
    
    @Override
    public int send(byte[] buffer) {
        
        timeoutScheduler.logWorkStart();
        try {
            return sendBuffer(buffer);
        } finally {
            timeoutScheduler.logWorkEnd();
        }
    }
    
    public int send(MSNetTCPSocketBuffer buffer) {
        timeoutScheduler.logWorkStart();
        try {
            return sendBuffer(buffer);
        } finally {
            timeoutScheduler.logWorkEnd();
        }
    }
    
    public int send(MSNetTCPSocketBuffer buffers[]) {
        timeoutScheduler.logWorkStart();
        try {
            return sendBuffers(buffers, false);
        } finally {
            timeoutScheduler.logWorkEnd();
        }
    }
    
    @Override
    public int send(MSNetMessage msg) {
        byte b[] = msg.getBytes();
        MSNetTCPSocketBuffer buf = new MSNetTCPSocketBuffer(b, 0, b.length);
        return send(buf);
    }
    private int sendBuffer(byte[] buffer_) {
        int retValue = 0;
        if (buffer_.length > 0) {
            MSNetTCPSocketBuffer buffer = new MSNetTCPSocketBuffer(buffer_, 0, buffer_.length, true);
            retValue = sendBuffer(buffer);
        }
        return retValue;
    }
    protected int sendBuffer(MSNetTCPSocketBuffer buffer) {
        return sendBuffer(buffer, false);
    }
    private int sendBuffers(MSNetTCPSocketBuffer[] buffers, boolean isEstablisher) {
        MSNetConnectionStateEnum state = getState();
        if (!isEstablisher && state != CONNECTED) {
            
            LOGGER.warn("Tried to send before connection is fully established, discarding data");
            
            if (state != CONNECTING && state != ESTABLISHING) {
                MSNetIOStatus ioStatus = new MSNetIOStatus(new MSNetIOException(
                        "Not connected (detected in pre-send state check)"));
                ioStatus.setNumBytesProcessed(-1);
                handleWriteErrorIfAny(ioStatus);
                return ioStatus.getNumBytesProcessed();
            }
            return 0;
        }
        MSNetIOStatus ioStatus = null;
        assert isLockStateValid(_writeLock);
        synchronized (_writeLock) {
            boolean writeListWasEmpty = _writeList.isEmpty();
            for (MSNetTCPSocketBuffer buffer : buffers) {
                appendToWriteList(buffer);
            }
            if (!writeListWasEmpty) {
                
                
                return 0;
            }
            ioStatus = _doWriteWithoutErrorHandling();
            
            if (bufferSize() > 0) {
                enableChannel(MSNetChannelOpertaion.WRITE);
            }
        }
        handleWriteErrorIfAny(ioStatus);
        return ioStatus.getNumBytesProcessed();
    }
    int sendBuffer(MSNetTCPSocketBuffer buffer, boolean isEstablisher) {
        MSNetConnectionStateEnum state = getState();
        if (!isEstablisher && state != CONNECTED) {
            
            LOGGER.warn("Tried to send before connection is fully established, discarding data");
            
            if (state != CONNECTING && state != ESTABLISHING) {
                MSNetIOStatus ioStatus = new MSNetIOStatus(new MSNetIOException(
                        "Not connected (detected in pre-send state check)"));
                ioStatus.setNumBytesProcessed(-1);
                handleWriteErrorIfAny(ioStatus);
                return ioStatus.getNumBytesProcessed();
            }
            return 0;
        }
        MSNetIOStatus ioStatus;
        assert isLockStateValid(_writeLock);
        synchronized (_writeLock) {
            boolean writeListWasEmpty = _writeList.isEmpty();
            appendToWriteList(buffer);
            if (!writeListWasEmpty) {
                
                
                return 0;
            }
            ioStatus = _doWriteWithoutErrorHandling();
            
            if (bufferSize() > 0) {
                enableChannel(MSNetChannelOpertaion.WRITE);
            }
        }
        handleWriteErrorIfAny(ioStatus);
        return ioStatus.getNumBytesProcessed();
    }
    
    
    
    
    @Override
    public void asyncSend(MSNetMessage msg) {
        timeoutScheduler.logWorkStart();
        byte b[] = msg.getBytes();
        MSNetTCPSocketBuffer buf = new MSNetTCPSocketBuffer(b, 0, b.length);
        asyncSend(buf);
        timeoutScheduler.logWorkEnd();
    }
    
    @Override
    public void asyncSend(byte[] buffer, int nb) {
        if ((buffer.length > 0) && (nb > 0)) {
            
            timeoutScheduler.logWorkStart();
            MSNetTCPSocketBuffer socketBuffer = new MSNetTCPSocketBuffer(buffer, 0, nb);
            asyncSend(socketBuffer);
            timeoutScheduler.logWorkEnd();
        } else {
            LOGGER.warn("Attempt to asyncSend invalid message");
        }
    }
    
    public void asyncSend(MSNetTCPSocketBuffer buffer) {
        timeoutScheduler.logWorkStart();
        asyncSendBuffer(buffer);
        timeoutScheduler.logWorkEnd();
    }
    
    public void asyncSend(MSNetTCPSocketBuffer buffers[]) {
        timeoutScheduler.logWorkStart();
        asyncSendBuffers(buffers);
        timeoutScheduler.logWorkEnd();
    }
    private void asyncSendBuffers(MSNetTCPSocketBuffer buffers[]) {
        int totalBytes = 0;
        assert isLockStateValid(_writeLock);
        synchronized (_writeLock) {
            for (int i = 0; i < buffers.length; i++) {
                appendToWriteList(buffers[i]);
                totalBytes += buffers[i].size();
            }
            
            
            if (_writeBufferSize == totalBytes) {
                enableChannel(MSNetChannelOpertaion.WRITE);
            }
        }
    }
    protected void asyncSendBuffer(MSNetTCPSocketBuffer buffer_) {
        assert isLockStateValid(_writeLock);
        synchronized (_writeLock) {
            appendToWriteList(buffer_);
            
            if (_writeBufferSize == buffer_.size()) {
                enableChannel(MSNetChannelOpertaion.WRITE);
            }
        }
    }
    
    protected int doRead() {
        MSNetIOStatus s = doRawRead();
        int numBytesRead = s.getNumBytesProcessed();
        if (numBytesRead > 0) {
            readNotify(_readBuffer);
        }
        return numBytesRead;
    }
    
    
    
    
    protected MSNetIOStatus doRawRead() {
        MSNetIOStatus ioStatus = doRawReadWithoutErrorHandling();
        if (ioStatus.inError()) {
            handleReadError(ioStatus.getException());
        }
        return ioStatus;
    }
    
    
    
    
    protected final MSNetIOStatus doRawReadWithoutErrorHandling() {
        int oldSize = _readBuffer.size();
        MSNetIOStatus ioStatus = tryReadBuffer(_readBuffer);
        if (ioStatus.inError()) {
            return ioStatus;
        }
        int read = ioStatus.getNumBytesProcessed();
        if (_metrics != null) {
            _metrics.receivedBytes(read);
        }
        if (read < 0) {
            
            ioStatus.setException(new MSNetSocketEndPipeException("Disconnected (read returned -1)"));
            return ioStatus;
        } else if (getState() != ESTABLISHING) {
            resetRetryTimeAndNumOfRetries();
        }
        if (getTapping() && (read > 0)) {
            int newSize = _readBuffer.size();
            if ((newSize - oldSize) > 0) {
                byte[] readBytes = _readBuffer.tail(read);
                tapNotify(readBytes, TAP_READ, getAddress());
            }
        }
        return ioStatus;
    }
    void handleReadError(MSNetException x) {
        if (x instanceof MSNetResourceBusyException) {
            
            
            
        } else if (x instanceof MSNetSocketEndPipeException) {
            
            if (getState() == CONNECTED && null != _readBuffer && _readBuffer.size() > 0) {
                if (canHandleNotifications()) {
                    readNotify(_readBuffer);
                } else {
                    LOGGER.error("Read buffer has {} unprocessed bytes, which will be discarded in disconnecting", _readBuffer.size());
                }
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Connection[{}] to {}: disconnected with an end pipe: {}", new Object[]{getName(),
                        getAddress(), x.getMessage()});
            }
            resetForDisconnect();
        } else if (x instanceof MSNetIOException) {
            
            
            String errmsg = x.toString();
            if (errmsg.endsWith("Connection reset by peer")
                    || errmsg.endsWith("An existing connection was forcibly closed by the remote host")) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Connection[{}] to {} reset by peer", getName(), getAddress());
                }
                resetForDisconnect();
            } else {
                LOGGER.error("Connection[{}] to {} resulted in IO Error: {}",
                        new Object[]{getName(), getAddress(), x});
                resetForError(MSNetConnectionErrorEnum.READ_ERROR);
            }
        } else { 
            LOGGER.error("Connection[{}] to {}: {} occurred.", new Object[]{getName(), getAddress(), x});
            resetForError(MSNetConnectionErrorEnum.READ_ERROR);
        }
    }
    private static final MSNetIOStatus DISCONNECT = new MSNetIOStatus(new MSNetIOException("Disconnecting due to OOM (msjava.msnet.MSNetTCPConnection.tryReadBuffer(MSNetTCPSocketBuffer))"));
    private MSNetIOStatus tryReadBuffer(MSNetTCPSocketBuffer buffer) {
        
        
        MSNetTCPSocket socket = _socket;
        if (null == socket) {
            return new MSNetIOStatus(new MSNetIOException("Not connected (detected while reading)."));
        }
        try {
            return socket.read(buffer);
        } catch (OutOfMemoryError e) {
            try {
                LOGGER.error("Disconnecting due to OutOfMemoryError on read", e);
            } catch (OutOfMemoryError ee) {
                
            }
            return DISCONNECT;
        }
    }
    
    @Override
    public MSNetIOStatus syncRead(MSNetMessage msg, long millisec) {
        timeoutScheduler.logWorkStart();
        MSNetTCPSocketBuffer buffer = new MSNetTCPSocketBuffer();
        MSNetIOStatus iostatus = syncRead(buffer, millisec);
        
        
        
        byte b[] = buffer.retrieve();
        msg.setBytes(b, 0, b.length);
        timeoutScheduler.logWorkEnd();
        return iostatus;
    }
    
    public MSNetIOStatus syncRead(MSNetTCPSocketBuffer buffer, long millisec) {
        timeoutScheduler.logWorkStart();
        MSNetIOStatus status = syncReadBuffer(buffer, millisec, false);
        timeoutScheduler.logWorkEnd();
        return status;
    }
    MSNetIOStatus syncReadBuffer(MSNetTCPSocketBuffer buffer, long millisec, boolean isEstablisher) {
        final long endTime = System.currentTimeMillis() + millisec;
        
        
        
        
        
        
        if (!isEstablisher) {
            try {
                waitForConnectedState(millisec);
            } catch (MSNetException e) {
                return new MSNetIOStatus(e);
            }
        }
        assert !isReadingEnabled() : "Sync reading while reading is enabled!";
        
        
        int size = _readBuffer.size();
        if (size > 0) {
            MSNetIOStatus s = doRawRead();
            if (buffer != null) {
                buffer.store(_readBuffer);
            }
            return new MSNetIOStatus(size, size, s.getException());
        }
        
        
        
        MSNetException readError = null;
        try {
            _syncReadWriteLock.readLock().lock();
            long currentTime;
            
            synchronized (_asyncSocketCloseCompleteLock) {
                while (_isAsyncSocketCloseInProgress && (currentTime = System.currentTimeMillis()) < endTime) {
                    try {
                        _asyncSocketCloseCompleteLock.wait(endTime - currentTime);
                    } catch (InterruptedException e) {
                        LOGGER.info("Interrupted waiting for async socket close complete lock.");
                        Thread.currentThread().interrupt();
                        return new MSNetIOStatus(0, 0, new MSNetInterruptedException());
                    }
                }
                if (_isAsyncSocketCloseInProgress) {
                    return new MSNetIOStatus(0, 0, new MSNetSelectTimeoutException(millisec));
                }
            }
            
            
            synchronized (_syncReadMultiplexerLock) {
                if (_syncReadMultiplexer == null) {
                    _syncReadMultiplexer = new MSNetMultiplexer();
                }
                MSNetMultiplexer mxr = _syncReadMultiplexer;
                currentTime = System.currentTimeMillis();
                if (endTime - currentTime < 0) {
                    return new MSNetIOStatus(0, 0, new MSNetSelectTimeoutException(millisec));
                }
                for (; ; ) {
                    
                    
                    MSNetTCPSocket socket = _socket;
                    if (null == socket) {
                        MSNetIOStatus ioStatus = new MSNetIOStatus();
                        ioStatus.setException(new MSNetSocketEndPipeException("Error in syncRead(); socket closed"));
                        return ioStatus;
                    }
                    try {
                        
                        mxr.register(socket, MSNetChannelOpertaion.READ.selectionKey);
                    } catch (MSNetException e) {
                        LOGGER.error("Failed to register multiplexer", e);
                        return new MSNetIOStatus(0, 0, e);
                    }
                    MSNetMultiplexStatus mxStatus = mxr.multiplex(endTime - currentTime);
                    
                    if (mxStatus.inError()) {
                        if (mxStatus.getException() instanceof MSNetSelectInterruptException) {
                            interruptNotify();
                            
                            continue;
                        } else {
                            return new MSNetIOStatus(0, 0, mxStatus.getException());
                        }
                    }
                    if (mxr.isEventPending(socket, MSNetChannelOpertaion.READ.selectionKey)) {
                        MSNetIOStatus ioStatus = doRawReadWithoutErrorHandling();
                        if (ioStatus.inError()) {
                            readError = ioStatus.getException();
                            return ioStatus;
                        }
                        if (buffer != null) {
                            buffer.store(_readBuffer);
                        }
                        return ioStatus;
                    }
                    currentTime = System.currentTimeMillis();
                    if (currentTime >= endTime) {
                        return new MSNetIOStatus(0, 0, new MSNetSelectTimeoutException(millisec));
                    }
                }
            } 
        } finally {
            _syncReadWriteLock.readLock().unlock();
            if (readError != null) {
                handleReadError(readError);
            }
        } 
    }
    
    @Override
    public String getInfoMessage() {
        MSNetTCPSocket socket = _socket;
        MSNetInetAddress local = (socket == null) ? null : socket.getLocalAddress();
        MSNetAuthContext auth = getAuthContext();
        StringBuilder sb = new StringBuilder("MSNetTCPConnection info:\n" +
                "  name             = " + _id.toString() + "\n" +
                "  address          = " + getAddress() + "\n" +
                "  primary          = " + getPrimaryAddress() + "\n" +
                "  backup           = " + getBackupAddress() + "\n" +
                "  local            = " + local + "\n" +
                "  state            = " + getState());
        if (auth != null) {
            sb.append("\n  auth mechanism   = ").append(auth.getAuthMechanism()).append("\n  auth status      = ")
                    .append(auth.getAuthStatus()).append("\n  auth id          = ").append(auth.getAuthID()).append(
                    "\n  authz id         = ").append(auth.getAuthzID());
        }
        sb.append("\n  reading          = ").append((isReadingEnabled() ? "enabled" : "disabled")).append(
                "\n  writing          = ").append((isWritingEnabled() ? "enabled" : "disabled")).append(
                "\n  socket           = ").append(socket).append("\n  buffer           = ").append(_writeBufferSize);
        return sb.toString();
    }
    
    public synchronized void setTcpNoDelay(boolean tcpNoDelay) {
        assert isLockStateValid(this);
        this._tcpNoDelay = tcpNoDelay;
        MSNetTCPSocket socket = _socket;
        if (null != socket) {
            socket.setTcpNoDelay(tcpNoDelay);
        }
    }
    
    public synchronized boolean getTcpNoDelay() {
        assert isLockStateValid(this);
        MSNetTCPSocket socket = _socket;
        if (null != socket) {
            return socket.getTcpNoDelay();
        }
        return _tcpNoDelay;
    }
    
    public synchronized void setKeepAlive(boolean keepAlive) {
        assert isLockStateValid(this);
        this._keepAlive = keepAlive;
        MSNetTCPSocket socket = _socket;
        if (null != socket) {
            socket.setKeepAlive(keepAlive);
        }
    }
    
    public synchronized boolean getKeepAlive() {
        assert isLockStateValid(this);
        MSNetTCPSocket socket = _socket;
        if (null != socket) {
            return socket.getKeepAlive();
        }
        return _keepAlive;
    }
    
    public synchronized int getReceiveBufferSize() {
        assert isLockStateValid(this);
        MSNetTCPSocket socket = _socket;
        if (null != socket) {
            return socket.getReceiveBufferSize();
        }
        return _receiveBufferSize;
    }
    
    public synchronized int getSendBufferSize() {
        assert isLockStateValid(this);
        MSNetTCPSocket socket = _socket;
        if (null != socket) {
            return socket.getSendBufferSize();
        }
        return _sendBufferSize;
    }
    
    public synchronized void setReceiveBufferSize(int size) {
        assert isLockStateValid(this);
        _receiveBufferSize = size;
        MSNetTCPSocket socket = _socket;
        if (null != socket) {
            socket.setReceiveBufferSize(size);
        }
    }
    
    public synchronized void setSendBufferSize(int size) {
        assert isLockStateValid(this);
        _sendBufferSize = size;
        MSNetTCPSocket socket = _socket;
        if (null != socket) {
            socket.setSendBufferSize(size);
        }
    }
    
    @Override
    public void enableReading() {
        _readingEnabled = true;
        if (null != _readBuffer && _readBuffer.size() > 0) {
            _loop.callbackAfterDelay(0, new MSNetEventListener() {
                @Override
                public void eventOccurred(MSNetEvent e_) {
                    readNotify(_readBuffer);
                }
            });
        }
        enableChannel(MSNetChannelOpertaion.READ);
    }
    
    @Override
    public void disableReading() {
        _readingEnabled = false;
        disableChannel(MSNetChannelOpertaion.READ);
    }
    
    @Override
    public boolean isReadingEnabled() {
        MSNetChannel c = _channel;
        if (c != null) {
            return c.isEnabled(MSNetChannelOpertaion.READ);
        } else {
            return false;
        }
    }
    
    public boolean isWritingEnabled() {
        MSNetChannel c = _channel;
        if (c != null) {
            return c.isEnabled(MSNetChannelOpertaion.WRITE);
        } else {
            return false;
        }
    }
    
    public int clearBuffers() {
        assert isLockStateValid(_writeLock);
        synchronized (_writeLock) {
            int numErased = 0;
            Iterator<MSNetTCPSocketBuffer> iter = _writeList.iterator();
            
            if (_currentWritten > 0) {
                while (iter.hasNext()) {
                    MSNetTCPSocketBuffer buffer = iter.next();
                    if (buffer.isLastBufferInMessage()) {
                        break;
                    }
                }
            }
            while (iter.hasNext()) {
                iter.next();
                
                iter.remove();
                ++numErased;
            }
            return numErased;
        }
    }
    
    public int bufferCount() {
        synchronized (_writeLock) {
            return _writeList.size();
        }
    }
    
    public long bufferSize() {
        synchronized (_writeLock) {
            return _writeBufferSize;
        }
    }
    
    public long bufferSizeLockless() {
        return _writeBufferSize;
    }
    
    
    
    
    @Internal
    void addInternalConnectionListener(MSNetInternalConnectionListener listener) {
        internalConnectionListeners.add(listener);
    }
    
    @Internal
    void removeInternalConnectionListener(MSNetInternalConnectionListener listener) {
        internalConnectionListeners.remove(listener);
    }
    
    protected MSNetMessage createMessageFromBuffer(MSNetTCPSocketBuffer buffer) {
        if (buffer.size() <= 0)
            return null;
        MSNetMessage msg = new MSNetByteMessage(buffer.retrieve());
        return msg;
    }
    protected boolean canHandleNotifications() {
        return hasListeners();
    }
    
    protected void readNotify(MSNetTCPSocketBuffer buffer) {
        assert hasListeners() || (this instanceof MSNetProtocolTCPConnection) : "Read callback, but no read listener installed!  Data will be lost.";
        MSNetMessage msg;
        while ((msg = createMessageFromBuffer(buffer)) != null) {
            readNotify(msg);
        }
    }
    protected void readNotify(final MSNetMessage message) {
        assert hasListeners() || (this instanceof MSNetProtocolTCPConnection) : "Read callback, but no read listener installed!  Data will be lost.";
        Runnable callback = new Runnable() {
            @Override
            public void run() {
                for (MSNetConnectionListener listener : getListeners()) {
                    timeoutScheduler.logWorkStart();
                    try {
                        listener.readCallback(_id, message);
                    } finally {
                        timeoutScheduler.logWorkEnd();
                    }
                }
            }
        };
        Executor executor = getCallbackExecutor();
        if (executor instanceof LockSafeMSNetLoopExecutor) {
            ((LockSafeMSNetLoopExecutor) executor).execute(callback, true);
        } else {
            executor.execute(callback);
        }
    }
    
    protected void establishFailedNotify(final String msg) {
        getCallbackExecutor().execute(new Runnable() {
            @Override
            public void run() {
                for(MSNetConnectionListener listener : getListeners()) {
                    listener.establishFailedCallback(_id, msg);
                }
            }
        });
    }
    
    protected void sendDoneNotify() {
        if (_sendDoneCallbackDisabled) {
            return;
        }
        getCallbackExecutor().execute(new Runnable() {
            @Override
            public void run() {
                for(MSNetConnectionListener listener : getListeners()) {
                    listener.sendDoneCallback(_id);
                }
            }
        });
    }
    
    protected void writeListThresholdReachedNotify(final long writeBufferSize) {
        getCallbackExecutor().execute(new Runnable() {
            @Override
            public void run() {
                for(MSNetConnectionListener listener : getListeners()) {
                    listener.writeListThresholdCallback(_id, writeBufferSize);
                }
            }
        });
    }
    private boolean loopIsLooping() {
        return getNetLoop().isLooping();
    }
    
    protected boolean inLoopThread() {
        
        return getNetLoop().equals(MSNetLoop.getLoopForThread(Thread.currentThread()));
    }
    
    protected void connectNotify() {
        if (_didConnectCallback.getAndSet(true)) {
            return;
        }
        getCallbackExecutor().execute(new Runnable() {
            @Override
            public void run() {
                for(MSNetConnectionListener listener : getListeners()) {
                    listener.connectCallback(_id);
                }
            }
        });
    }
    
    @Internal
    void safeCloseNotifyInternal() {
        for (final MSNetInternalConnectionListener l : internalConnectionListeners) {
            try {
                getNetLoop().callbackAfterDelay(0, new MSNetEventListener() {
                    @Override
                    public void eventOccurred(MSNetEvent e) {
                        try {
                            assert connectingLock.notHeldByCurrentThread();
                            l.closeCallback(_id);
                        } catch (Throwable t) {
                            LOGGER.warn("Uncaught throwable when running an internal close event", t);
                        }
                    }
                });
            } catch (Throwable t) {
                LOGGER.warn("Uncaught throwable when scheduling an internal close event", t);
            }
        }
    }
    
    @Internal
    void safeDisconnectNotifyInternal() {
        for (final MSNetInternalConnectionListener l : internalConnectionListeners) {
            try {
                getNetLoop().callbackAfterDelay(0, new MSNetEventListener() {
                    @Override
                    public void eventOccurred(MSNetEvent e) {
                        try {
                            assert connectingLock.notHeldByCurrentThread();
                            l.disconnectCallback(_id);
                        } catch (Throwable t) {
                            LOGGER.warn("Uncaught throwable when running an internal disconnect event", t);
                        }
                    }
                });
            } catch (Throwable t) {
                LOGGER.warn("Uncaught throwable when scheduling an internal disconnect event", t);
            }
        }
    }
    
    protected void disconnectNotify() {
        getCallbackExecutor().execute(new Runnable() {
            @Override
            public void run() {
                for(MSNetConnectionListener listener : getListeners()) {
                    listener.disconnectCallback(_id);
                }
            }
        });
    }
    
    protected void connectTimeoutNotify() {
        getCallbackExecutor().execute(new Runnable() {
            @Override
            public void run() {
                for(MSNetConnectionListener listener : getListeners()) {
                    listener.errorCallback(_id, MSNetConnectionErrorEnum.ESTABILISH_TIMEOUT);
                }
            }
        });
    }
    
    protected void retryNotify() {
        
        asyncConnectTimeoutRetryCallback();
        getCallbackExecutor().execute(new Runnable() {
            @Override
            public void run() {
                for(MSNetConnectionListener listener : getListeners()) {
                    listener.retryCallback(_id);
                }
            }
        });
    }
    
    protected void errorNotify(final MSNetConnectionErrorEnum error) {
        getCallbackExecutor().execute(new Runnable() {
            @Override
            public void run() {
                for(MSNetConnectionListener listener : getListeners()) {
                    listener.errorCallback(_id, error);
                }
            }
        });
    }
    
    protected void reparentNotify() {
        getCallbackExecutor().execute(new Runnable() {
            @Override
            public void run() {
                for(MSNetConnectionListener listener : getListeners()) {
                    listener.reparentCallback(_id);
                }
            }
        });
    }
    
    protected void interruptNotify() {
        getCallbackExecutor().execute(new Runnable() {
            @Override
            public void run() {
                for(MSNetConnectionListener listener : getListeners()) {
                    listener.interruptCallback(_id);
                }
            }
        });
    }
    
    protected void tapNotify(final byte[] buffer, final int tapSource, final MSNetAddress peerAddress) {
        getCallbackExecutor().execute(new Runnable() {
            @Override
            public void run() {
                for(MSNetConnectionListener listener : getListeners()) {
                    listener.tapCallback(_id, peerAddress, buffer, (tapSource == TAP_READ));
                }
            }
        });
        if (MSNetConnectionConfigurationSupport.getTapFile() != null) {
            String msg = getName().toString() + "  " + peerAddress.toString()
                    + (tapSource == TAP_READ ? " (rx) " : " (tx) ") + " size=" + buffer.length + " ["
                    + MSOctalEncoder.asMixedOctal(buffer) + "]\n";
            try {
                MSNetConnectionConfigurationSupport.getTapFile().write(msg.getBytes());
                MSNetConnectionConfigurationSupport.getTapFile().flush();
            } catch (IOException e) {
                LOGGER.error("Error writing to tap log", e);
            }
        }
    }
    
    public MSNetTCPSocketFactory getSocketFactory() {
        return socketFactory;
    }
    
    public void setSocketFactory(MSNetTCPSocketFactory socketFactory) {
        Objects.requireNonNull(socketFactory, "The socket factory cannot be null");
        this.socketFactory = socketFactory;
    }
    
    public MSNetTCPSocket getSocket() {
        return _socket;
    }
    
    public MSNetTCPSocketBuffer getReadBuffer() {
        return _readBuffer;
    }
    
    public void setSOCKSUserName(String userName) {
        if (_socksEstablisher != null) {
            _socksEstablisher.setUserName(userName);
        }
        _socksUserName = userName;
    }
    
    public String getSOCKSUserName() {
        if (_socksEstablisher == null) {
            return MSNetConfiguration.USER_NAME;
        } else {
            return _socksEstablisher.getUserName();
        }
    }
    
    public boolean setSOCKSProxyAddress(MSNetInetAddress addr) {
        MSNetConnectionStateEnum state = getState();
        if (state == CONNECTED || state == CONNECTING || state == ESTABLISHING) {
            return false;
        }
        _socksProxyAddress = addr;
        if (addr == null) {
            if (_socksEstablisher != null) {
                
                establishers.remove(_socksEstablisher);
                _socksEstablisher = null;
            }
        } else {
            _socksEstablisher = new MSNetSOCKS4Establisher();
            _socksEstablisher.setUserName(_socksUserName);
            addEstablisher(_socksEstablisher);
        }
        return true;
    }
    
    public MSNetInetAddress getSOCKSProxyAddress() {
        return _socksProxyAddress;
    }
    
    @Override
    public boolean setAddress(MSNetAddress addr) {
        throwIfImmutable();
        MSNetConnectionStateEnum state = getState();
        if (state == CONNECTED || state == CONNECTING || state == ESTABLISHING) {
            return false;
        }
        setOverrideAddress(addr);
        return true;
    }
    
    public List<MSNetInetAddress> getAddressList() {
        return addrs.getAddressList();
    }
    
    public void setAddressList(List<MSNetInetAddress> l) {
        addrs.setAddressList(l);
    }
    
    public MSNetInetAddress getAddress(int index) {
        return addrs.getAddress(index);
    }
    
    public void setAddress(int index, MSNetInetAddress addr) {
        addrs.setAddress(index, addr);
    }
    
    public void failback(boolean useOverride) {
        close();
        addrs.failback(useOverride);
        connect();
    }
    
    public void failback(MSNetInetAddress addr) {
        close();
        connect(addr);
    }
    
    public void setAsyncConnectTimeout(long asyncConnectTimeout) {
        this.asyncConnectTimeout = asyncConnectTimeout;
    }
    
    @Override
    public MSNetAddress getAddress() {
        return addrs.getAddress();
    }
    
    @Override
    public MSNetAddress getBackupAddress() {
        return addrs.getBackupAddress();
    }
    
    @Override
    public void setBackupAddress(MSNetAddress addr) {
        addrs.setBackupAddress((MSNetInetAddress) addr);
    }
    
    @Override
    public MSNetAddress getPrimaryAddress() {
        return addrs.getPrimaryAddress();
    }
    
    @Override
    public void setPrimaryAddress(MSNetAddress addr) {
        addrs.setPrimaryAddress((MSNetInetAddress) addr);
    }
    
    public MSNetAddress getOverrideAddress() {
        return addrs.getOverrideAddress();
    }
    
    public void setOverrideAddress(MSNetAddress addr) {
        addrs.setOverrideAddress((MSNetInetAddress) addr);
    }
    private void resetRetryTimeAndNumOfRetries() {
        _retryTime = 0;
        _numRetries = 0;
        _stateBeforeConnectionReset = null;
    }
    
    public boolean isDisableSendDoneCallback() {
        return _sendDoneCallbackDisabled;
    }
    
    public void setDisableSendDoneCallback(boolean sendDoneCallbackDisabled) {
        this._sendDoneCallbackDisabled = sendDoneCallbackDisabled;
    }
    
    public void setBindAddress(MSNetInetAddress addr) {
        _bindAddress = addr;
    }
    
    public MSNetInetAddress getBindAddress() {
        return _bindAddress;
    }
    
    public void setWriteListThreshold(long size) {
        _writeListThreshold = size;
    }
    
    public long getWriteListThreshold() {
        return _writeListThreshold;
    }
    void handleWriteErrorIfAny(MSNetIOStatus ioStatus) {
        if (ioStatus.isWriteErrorInSyncWriteSelectLoop()
            
            || (ioStatus.getNumBytesProcessed() == -1 && !(_readingEnabled && MSNetConfiguration.IGNORE_WRITE_FAILURE))) {
            handleWriteError(ioStatus.getException());
        }
    }
    private void handleReadErrorIfAny(MSNetIOStatus ioStatus) {
        if (ioStatus.isReadErrorInSyncWriteSelectLoop()) {
            handleReadError(ioStatus.getException());
        }
    }
    
    void setIdleTimeout(long timeout) {
        timeoutScheduler.setTimeout(timeout);
    }
    public long getIdleTimeout() {
        return timeoutScheduler.getTimeout();
    }
    public void setAsyncReading(boolean readingEnabled) {
        _readingEnabled = readingEnabled;
    }
    public final void setKerberos(boolean kerberos) {
        this.kerberos = kerberos;
    }
    
    @Experimental
    public final void setKerberosEncryption(boolean kerberosEnryption) {
        this.kerberosEncryption = kerberosEnryption;
        LOGGER.info("set KerberosEncryption={}", this.kerberosEncryption);
    }
    public void setChannelBindingMode(ChannelBindingMode channelBindingMode) {
        this.channelBindingMode = channelBindingMode;
    }
    public ChannelBindingMode getChannelBindingMode() {
        return channelBindingMode;
    }
    
    @Deprecated
    public void setAnonymousAuthentication(boolean anonymous) {
        this.anonymousAuthentication = anonymous;
    }
    
    protected final void setAuthenticatorInitialized() {
        this.authenticatorInitialized = true;
    }
    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        _readingEnabledSettingAtConfig = _readingEnabled;
        if (kerberosEncryption && !kerberos) {
            throw new IllegalArgumentException("Attempt to use Kerberos encryption without Kerberos authentication");
        }
        if (_readingEnabled && _loop.getImpl() instanceof MSNetLoopNullImpl) {
            throw new IllegalStateException("Cannot use asynchronous reading with a null loop");
        }
        if (!authenticatorInitialized && (kerberos || anonymousAuthentication)) {
            addAuthenticator();
        }
        boolean krbRequired = false;
        MSNetAuthenticator auth = null;
        for (MSNetEstablisher establisher : establishers.keySet()) {
            if (establisher instanceof MSNetAuthenticator) {
                auth = (MSNetAuthenticator) establisher;
                if (auth.containsAuthMechanismFeature(MechanismDescription.Feature.Kerberos)) {
                    krbRequired = !auth.isOptional();
                    break;
                }
            }
        }
        if (!krbRequired) {
            MSKerberosAuthority.assertKerberizedClientNotEnforced("msjava.msnet.client");
        }
        if (backupAddresses != null) {
            int index = 1;
            for (String backup : backupAddresses) {
                setAddress(index++, new MSNetInetAddress(backup));
            }
        }
        if (timeoutScheduler == null) {
            timeoutScheduler = new IdleTimeoutScheduler(MSNetTCPConnection.this);
        }
    }
    @Override
    void validateAddresses() throws UnknownHostException {
        if (getAddress() == null) {
            Validate.notNull(hostPort, "You must either specify a valid address or the hostPort property!");
            setAddress(new MSNetInetAddress(hostPort));
        }
    }
    public List<String> getBackupAddresses() {
        return backupAddresses;
    }
    public void setBackupAddresses(List<String> backupAddresses) throws UnknownHostException {
        this.backupAddresses = backupAddresses;
    }
    public void setBindAddress(String bindAddress) throws UnknownHostException {
        setBindAddress(new MSNetInetAddress(bindAddress));
    }
    public void setImmutable(boolean immutable) {
        this.immutable = immutable;
    }
    private void throwIfImmutable() {
        if (immutable) {
            throw new IllegalStateException("Cannot change property on an immutable connection");
        }
    }
    
    public final void resetReadingEnabled() {
        this._readingEnabled = _readingEnabledSettingAtConfig;
    }
    
    public boolean getKerberos() {
        return kerberos;
    }
    
    public boolean getKerberosEncryption() {
        return kerberosEncryption;
    }
    
    public boolean isConnectNotificationOnSyncConnect() {
        return connectNotificationOnSyncConnect;
    }
    
    public void setConnectNotificationOnSyncConnect(boolean connectNotificationOnSyncConnect) {
        this.connectNotificationOnSyncConnect = connectNotificationOnSyncConnect;
    }
    
    public MasterEstablisherMode getMasterEstablisherMode() {
        return masterEstablisherMode;
    }
    
    public void setMasterEstablisherMode(MasterEstablisherMode masterEstablisherMode) {
        this.masterEstablisherMode = masterEstablisherMode;
    }
    
    public Iterator<MSNetEstablisher> establisherIterator() {
        return establishers.keySet().iterator();
    }
    
    @Override
    protected void fillString(StringBuilder builder) {
        super.fillString(builder);
        builder.append(", ");
        builder.append("address=").append(getAddress());
        builder.append(", ");
        builder.append("kerberos=").append(kerberos);
    }
    @Override
    protected void fillDiagnosticInformation(StringBuilder builder, int lod) {
        super.fillDiagnosticInformation(builder, lod);
        builder.append("\n");
        builder.append("    ").append("socket=").append(_socket);
        builder.append("\n");
        builder.append("    ").append("writeBufferCount=").append(bufferCount());
        builder.append("\n");
        builder.append("    ").append("writeBufferSize=").append(bufferSize());
        builder.append("\n");
        builder.append("    ").append("readBufferSize=").append(_readBuffer.size);
        
        if (lod >= 2) {
            builder.append("\n");
            builder.append("    ").append("connectingLock=").append(connectingLock);
            builder.append("\n");
            builder.append("    ").append("callbackExecutor=").append(getCallbackExecutor());
        }
    }
}
