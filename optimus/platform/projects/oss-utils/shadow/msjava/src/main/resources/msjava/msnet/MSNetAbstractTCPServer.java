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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.concurrent.GuardedBy;
import msjava.base.annotation.Experimental;
import msjava.base.kerberos.MSKerberosAuthority;
import msjava.base.service.internal.AuthMethod;
import msjava.base.slr.internal.ServiceDescriptionBuilder;
import msjava.base.slr.internal.ServiceName;
import msjava.base.slr.internal.ServicePublicationSupport;
import msjava.base.slr.internal.ServicePublisher;
import msjava.base.spring.lifecycle.BeanState;
import msjava.base.spring.lifecycle.MSComponentLifecyclePhases;
import msjava.base.sr.MapServiceDescription;
import msjava.base.sr.ServiceAttributes;
import msjava.base.sr.ServiceDescription;
import msjava.base.sr.internal.PublicationAttributesSupport;
import msjava.kerberos.auth.ChannelBindingConfiguration;
import msjava.msnet.EstablishersValidator.ValidationResult;
import msjava.msnet.admin.MSNetAdminManager;
import msjava.msnet.admin.MSNetAdminableConnection;
import msjava.msnet.admin.MSNetAdminableTCPAcceptor;
import msjava.msnet.auth.MSNetAnonymousEstablisherFactory;
import msjava.msnet.auth.MSNetAuthenticationException;
import msjava.msnet.auth.MSNetKerberosEstablisherFactory;
import msjava.msnet.jmx.MSNetJMXControls;
import msjava.msnet.spring.ApplicationNetLoopAware;
import msjava.msnet.tcp.server.TCPServerConnectionLimitListener;
import msjava.msnet.tcp.server.TCPServerMessageHandler;
import msjava.msnet.tcp.server.TCPServerStateListener;
import msjava.msnet.tcp.server.TCPServerStreamHandler;
import msjava.msnet.utils.IdleTimeoutScheduler;
import msjava.msnet.utils.MSNetConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.util.Assert;
import static java.util.stream.Collectors.joining;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AtomicLongMap;
import com.ms.infra.net.establishers.auth.mechanism.kerberos.ChannelBindingMode;
import com.ms.infra.net.establishers.auth.report.MateWTPublisher;
import com.ms.infra.net.establishers.auth.report.Publisher;
@SuppressWarnings("deprecation")
public abstract class MSNetAbstractTCPServer implements MSNetTCPServer, InitializingBean, ApplicationNetLoopAware,
        BeanNameAware, SmartLifecycle, ServiceDescriptionBuilder {
    public static final int DEFAULT_ORDER = MSComponentLifecyclePhases.SERVER_TRANSPORT_PHASE;
    private static final boolean DEFAULT_FORWARD_EVENTS = true;
    private static final Logger LOGGER = LoggerFactory.getLogger(MSNetAbstractTCPServer.class);
    private static final String AUTO_MASTER_EST_URL = "http:
    private final BeanState state = new BeanState();
    
    
    private MasterEstablisherMode masterEstablisherMode = MasterEstablisherMode.OFF;
    long idleTimeout = IdleTimeoutScheduler.TIMEOUT_VALUE_NONE;
    protected long connectTimeout = 0;
    private int order = DEFAULT_ORDER;
    private ServicePublicationSupport publicationSupport;
    private final Set<TCPServerConnectionLimitListener> connectionLimitListeners = new HashSet<>();
    
    protected class ConnectListener extends MSNetConnectionListenerAdapter {
        @Override
        public void connectCallback(MSNetID connectionName) {
            openConnectionHandler(connectionName);
        }
    }
    
    protected class EstablishListener extends MSNetConnectionListenerAdapter {
        @Override
        public void connectCallback(MSNetID connectionName) {
            promoteConnection(connectionName);
        }
        @Override
        public void disconnectCallback(MSNetID connectionName) {
            guardedRemoveEstablishingContext(connectionName);  
        }
        @Override
        public void establishFailedCallback(MSNetID connectionName, String msg) {
            guardedRemoveEstablishingContext(connectionName);
        }
    }
    
    MSNetConnectionContext guardedRemoveEstablishingContext(MSNetID connectionName) {
        if (doAcquireIfRunning(writeLock())) {
            try {
                return _removeEstablishingContext(connectionName);
            } finally {
                doUnlock(writeLock());
            }
        } else {
            LOGGER.debug("remove establishing context called after the server was shutdown - skipping...");
            return null;
        }
    }
    
    
    protected class CloseListener implements MSNetTCPConnection.MSNetInternalConnectionListener {
        @Override
        public void closeCallback(MSNetID id) {
            cleanupAndRemoveConnection(id, false);
            getMetrics().connectionDropped();
        }
        @Override
        public void disconnectCallback(MSNetID id) {
            cleanupAndRemoveConnection(id, true);
            getMetrics().connectionDropped();            
        }
    }
    
    protected class ConnectionEventForwarder extends MSNetConnectionListenerAdapter implements
            MSNetMessageStreamHandler {
        @Override
        public void errorCallback(MSNetID connectionName, MSNetConnectionErrorEnum error) {
            connectionErrorHandler(connectionName, error);
        }
        @Override
        public void readCallback(MSNetID connectionName, MSNetMessage message) {
            connectionReadHandler(connectionName, message);
        }
        @Override
        public void streamCallback(MSNetID connectionName, MSNetMessageInputStream in) {
            connectionStreamHandler(connectionName, in);
        }
    }
    
    protected class AcceptorListener implements MSNetTCPAcceptorListener {
        @Override
        public void acceptCallback(MSNetTCPSocket socket) {
            acceptHandler(socket);
        }
        @Override
        public void stateCallback(MSNetTCPAcceptorStateEnum state) {
        }
    }
    
    protected class DeadConnectionCleaner implements MSNetEventListener {
        @Override
        public void eventOccurred(MSNetEvent e) {
            removeDeadConnections();
        }
    }
    
    @VisibleForTesting
    protected class ConnectionLimitingServerStateListener {
        
        private static final String WARNING_MESSAGE = "TCP Server ({} on {}) connection count on key '{}' from {} "
                + " reached warning threshold ({} >= {}), using policy '{}'. New connections will be"
                + " rejected if connection count from this source exceeds maximum allowed"
                + " number of connections ({}). These limits can be changed by setting"
                + " connectionPerHostLimit or connectionPerHostWarningThreshold property"
                + " on your server, or globally by seting '"
                + MSNetConfiguration.DEFAULT_CONNECTION_PER_HOST_LIMIT_PROPERTY + "' or '"
                + MSNetConfiguration.DEFAULT_CONNECTION_PER_HOST_WARN_THRESHOLD_PROPERTY + "' system " + "property.";
        private static final String ERROR_MESSAGE = "TCP server ({} on {}) rejected"
                + " incoming connection with key '{}' from {}, using policy '{}'. Maximum number ({}) of allowed"
                + " connections from a single source reached. You can change this limit"
                + " on a TCP server by setting connectionPerHostLimit property on your"
                + " server, or globally by setting '" + MSNetConfiguration.DEFAULT_CONNECTION_PER_HOST_LIMIT_PROPERTY
                + "' system property.";
        
        private final AtomicLongMap<Object> connCountPerGroup = AtomicLongMap.create();
        
        public boolean newConnectionCallback(MSNetTCPServerUserContext context) {
            Object groupKey = connectionGroup(context);
            if (groupKey == null)
                return true;
            if (LOGGER.isDebugEnabled())
                LOGGER.debug("Going to increment connection count for {}", groupKey);
            
            long currentConnectionCount = connCountPerGroup.incrementAndGet(groupKey);
            
            int connectionLimit = configurationSettings.getConnectionPerHostLimit();
            if ((connectionLimit > 0) && (currentConnectionCount > connectionLimit)) {
                newConnectionHandledCallback(context, groupKey, currentConnectionCount, false, true);
                
                LOGGER.error(ERROR_MESSAGE, new Object[] { getName(), getAddress(), groupKey,
                        context.getConnection().getAddress(), getConnectionLimitPolicy(), connectionLimit });
                context.getConnection().disconnect();
                notifyConnectionLimited(context, groupKey, currentConnectionCount);
                return false;
            } else {
                
                int connectionWarnThreshold = configurationSettings.getConnectionPerHostWarnThreshold();
                if ((connectionWarnThreshold > 0) && (currentConnectionCount >= connectionWarnThreshold)) {
                    newConnectionHandledCallback(context, groupKey, currentConnectionCount, true, false);
                    LOGGER.warn(WARNING_MESSAGE, new Object[] { getName(), getAddress(), groupKey,
                            context.getConnection().getAddress(), currentConnectionCount, connectionWarnThreshold,
                            getConnectionLimitPolicy(), connectionLimit });
                    notifyConnectionLimitWarning(context, groupKey, currentConnectionCount);
                } else {
                    newConnectionHandledCallback(context, groupKey, currentConnectionCount, false, false);
                }
                return true;
            }
        }
        
        public void removeConnectionCallback(MSNetTCPServerUserContext context) {
            Object groupKey = connectionGroup(context);
            if (groupKey == null)
                return;
            if (LOGGER.isDebugEnabled())
                LOGGER.debug("Going to decrement connection count for {}", groupKey);
            long remainingConnCount = connCountPerGroup.decrementAndGet(groupKey);
            removeConnectionHandledCallback(context, groupKey, remainingConnCount);
            assert remainingConnCount >= 0 : "groupKey=" + groupKey + ", remaining=" + remainingConnCount;
        }
        
        @VisibleForTesting
        void newConnectionHandledCallback(MSNetTCPServerUserContext context, Object groupKey, long newCount, boolean warn, boolean limit) {
            
        }
        
        @VisibleForTesting
        void removeConnectionHandledCallback(MSNetTCPServerUserContext context, Object groupKey, long newCount) {
            
        }
        private Object connectionGroup(MSNetTCPServerUserContext context) {
            return getConnectionLimitPolicy().connectionGroup(context);
        }
    }
    
    @Deprecated
    protected MSNetLoop _loop;
    
    @Deprecated
    protected MSNetID _serverName;
    
    @Deprecated
    protected msjava.msnet.admin.MSNetAdminManager _adminManager;
    
    @Deprecated
    protected MSNetTCPConnectionFactory _connectionFactory;
    
    @Deprecated
    protected List<MSNetEstablisherFactory> _establishers = new ArrayList<>();
    protected MSNetAbstractTCPServerConfiguration configurationSettings;
    protected MSNetTCPAcceptor _acceptor;
    protected MSNetID _adminableAcceptorName;
    @GuardedBy("_rwLock")
    protected HashMap<MSNetID, MSNetConnectionContext> _connectionContextMap = new HashMap<>();
    protected AtomicInteger _connectionCount = new AtomicInteger(0);
    protected List<TCPServerStateListener> stateListeners = new CopyOnWriteArrayList<>();
    protected List<MSNetConnectionContext> _deadConnectionList = new LinkedList<>();
    @GuardedBy("_rwLock")
    protected final HashMap<MSNetID, MSNetConnectionContext> _establishingContextMap = new HashMap<>();
    protected ConnectListener _connectListener = new ConnectListener();
    protected CloseListener connectionListener = new CloseListener();
    
    protected ConnectionEventForwarder _connectionEventForwarder = new ConnectionEventForwarder();
    protected DeadConnectionCleaner _deadConnectionCleaner = new DeadConnectionCleaner();
    protected final ReadWriteLock _rwLock = new ReentrantReadWriteLock();
    private MSNetTCPServerMetrics _metrics;
    
    @Deprecated
    protected boolean _forwardAcceptedEvents;
    private TCPServerStreamHandler streamHandler;
    private TCPServerMessageHandler messageHandler;
    
    private volatile boolean isServerLoopInternal;
    
    
    private volatile boolean isStarted;
    private final ConnectionLimitingServerStateListener connectionLimitingServerStateListener = createLimiter();
    
    private MasterEstablisherMode originalMEMode;
    
    private MSNetKerberosEstablisherFactory implicitKerberosFactory;
    @VisibleForTesting
    ConnectionLimitingServerStateListener createLimiter() {
        return new ConnectionLimitingServerStateListener();
    }
    protected MSNetAbstractTCPServer() {
        this(new Configuration());
    }
    public MSNetAbstractTCPServer(MSNetAbstractTCPServerConfiguration configuration) {
        this(configuration, DEFAULT_FORWARD_EVENTS);
    }
    private MSNetAbstractTCPServer(MSNetAbstractTCPServerConfiguration configuration, boolean forwardAcceptedEvents) {
        this.configurationSettings = configuration;
        init(forwardAcceptedEvents);
    }
    
    public MSNetAbstractTCPServer(MSNetLoop loop, MSNetInetAddress address, MSNetID serverName,
                                  MSNetTCPConnectionFactory connectionFactory, boolean forwardAcceptedEvents) {
        
        
        this.configurationSettings = new DefaultConfiguration(loop, address, serverName, null, connectionFactory, null,
                null);
        
        init(forwardAcceptedEvents);
    }
    private void init(boolean forwardAcceptedEvents) {
        setForwardAcceptedEvents(forwardAcceptedEvents);
    }
    
    private void doCreatePublicationSupport() {
        assert publicationSupport == null : "never overwriting publicationSupport";
        ServicePublisher publisher = configurationSettings.getServicePublisher();
        if (publisher != null) {
            publicationSupport = new ServicePublicationSupport(this , publisher);
        }
    }
    
    public void setServicePublisher(ServicePublisher servicePublisher) {
            configurationSettings.setServicePublisher(servicePublisher);
    }
    
    public void setServiceName(String serviceName) {
        configurationSettings.setServiceName(serviceName);
    }
    
    public void setPublicationAttributes(Map<String, Object> attributes) {
        configurationSettings.setPublicationAttributes(attributes);
        
    }
    
    @Deprecated
    public void setPublicationDetails(ServicePublisher servicePublisher, String serviceName) {
            configurationSettings.setServicePublisher(servicePublisher);
            configurationSettings.setServiceName(serviceName);
    }
    public MSNetTCPAcceptor getAcceptor() {
        return _acceptor;
    }
    
    protected MSNetTCPServerMetrics createMetrics() {
        return new MSNetTCPServerMetrics();
    }
    public MSNetTCPServerMetrics getMetrics() {
        return _metrics;
    }
    protected Lock readLock() {
        return _rwLock.readLock();
    }
    protected Lock writeLock() {
        return _rwLock.writeLock();
    }
    
    
    boolean doAcquireIfRunning(Lock sync) {
        long timeout = 1;
        while (isRunning()) {
            try {
                if (sync.tryLock(timeout, TimeUnit.MILLISECONDS)) {
                    if (isRunning()) {
                        return true;
                    } else {
                        sync.unlock();
                        return false;
                    }
                } else if (timeout < 10000L) {
                    timeout *= 2;
                }
            } catch (InterruptedException e) {
                return false;
            }
        }
        return false;
    }
    
    void doAcquire(Lock sync) {
        sync.lock();
    }
    
    void doUnlock(Lock sync) {
        try {
            sync.unlock();
        } catch (IllegalMonitorStateException e) {
            assert false : e.toString();
        }
    }
    
    @Override
    @Deprecated
    public void addListener(TCPServerStateListener listener) {
        addStateListener(listener);
    }
    
    public void addStateListener(TCPServerStateListener listener) {
        setMessageHandlerIfOldListener(listener);
        stateListeners.add(listener);
    }
    
    
    @Deprecated
    private void setMessageHandlerIfOldListener(TCPServerStateListener listener) {
        if (listener instanceof MSNetTCPServerListener) {
            MSNetTCPServerListener backCompatListener = (MSNetTCPServerListener) listener;
            try {
                setMessageHandler(backCompatListener);
            } catch (Exception e) {
                throw new RuntimeException(
                        "You are making use an older MSNetTCPServerListener interface allowed in versions "
                                + "before msnet 7.x."
                                + " This interface has been broken into two separate interfaces TCPServerStateListener "
                                + "& TCPServerMessageHandler in newer versions. Please use them instead with new "
                                + "methods \n 1.server.addListener(TCPServerStateListener listener) \n "
                                + "2.server.setMessageHandler(TCPServerMessageHandler messageHandler. \n "
                                + "If you want to continue to use MSNetTCPServerListener interface, you cannot "
                                + "add more than one of these to your server. ");
            }
        }
    }
    
    @Deprecated
    public void removeListener(TCPServerStateListener listener) {
        removeStateListener(listener);
    }
    
    @Override
    public void removeStateListener(TCPServerStateListener listener) {
        stateListeners.remove(listener);
    }
    
    protected void notifyMessage(final MSNetTCPServerUserContext context, final MSNetMessage message) {
        if (configurationSettings.getExecutor() != null) {
            configurationSettings.getExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    doNotifyMessage(context, message);
                }
            });
        } else {
            doNotifyMessage(context, message);
        }
    }
    
    private void doNotifyMessage(MSNetTCPServerUserContext context, MSNetMessage message) {
        messageHandler.messageCallback(context, message);
    }
    
    
    protected void notifyStream(MSNetTCPServerUserContext context, MSNetMessageInputStream in) {
        streamHandler.streamCallback(context, in);
    }
    
    protected void notifyNewConnection(final MSNetTCPServerUserContext context) {
        
        
        MSNetLoop loop = context.getConnection().getNetLoop();
        loop.callbackAfterDelay(0, (new MSNetEventListener() {
            @Override
            public void eventOccurred(MSNetEvent e_) {
                for (TCPServerStateListener l : stateListeners) {
                    l.newConnectionCallback(context);
                }
            }
        }));
    }
    
    protected void notifyRemoveConnection(MSNetTCPServerUserContext context) {
        for (TCPServerStateListener l : stateListeners) {
            l.removeConnectionCallback(context);
        }
    }
    protected MSNetTCPConnectionFactory getConnectionFactory() {
        return configurationSettings.getConnectionFactory();
    }
    public void setConnectionFactory(MSNetTCPConnectionFactory factory) {
        configurationSettings.setConnectionFactory(factory);
    }
    
    protected void notifyServerError(Exception exception) {
        for (TCPServerStateListener l : stateListeners) {
            l.serverErrorCallback(exception);
        }
    }
    
    private void notifyConnectionLimited(MSNetTCPServerUserContext context, Object groupKey, long connectionCount) {
        for (TCPServerConnectionLimitListener l : connectionLimitListeners) {
            l.limitReached(context, groupKey, connectionCount, getConnectionPerHostWarnThreshold(),
                    getConnectionPerHostLimit());
        }
    }
    private void notifyConnectionLimitWarning(MSNetTCPServerUserContext context, Object groupKey, long connectionCount) {
        for (TCPServerConnectionLimitListener l : connectionLimitListeners) {
            l.warningThresholdReached(context, groupKey, connectionCount, getConnectionPerHostWarnThreshold(),
                    getConnectionPerHostLimit());
        }
    }
    
    @Override
    public MSNetID getName() {
        return new MSNetID(configurationSettings.getServerName());
    }
    
    @Override
    public void setName(MSNetID serverName) {
        configurationSettings.setBeanName(serverName.getString());
    }
    
    @Override
    public MSNetInetAddress getAddress() {
        MSNetTCPAcceptor a = _acceptor;
        if (a == null || !a.isListening()) {
            
              return configurationSettings.getConfiguredAddress();
        }
        return a.getAddress();
    }
    
    public MSNetInetAddress getConfiguredAddress() {
        return configurationSettings.getConfiguredAddress();
    }
    
    @Override
    public void setAddress(MSNetInetAddress address) {
        
        if (!state.isRunning()) {
            configurationSettings.setConfiguredAddress(address);
        } else {
            _acceptor.setAddress(address);
        }
    }
    
    public MSNetLoop getNetLoop() {
        return configurationSettings.getLoop();
    }
    
    @Override
    public MSNetTCPServerUserContext getUserContext(MSNetID connectionName) {
        doAcquire(readLock());
        try {
            MSNetConnectionContext context = _getConnectionContext(connectionName);
            if (context == null) {
                return null;
            }
            return context.getUserContext();
        } finally {
            doUnlock(readLock());
        }
    }
    
    @Override
    public List<MSNetTCPServerUserContext> getUserContexts() {
        doAcquire(readLock());
        try {
            List<MSNetConnectionContext> ccl = _getConnectionContexts();
            ArrayList<MSNetTCPServerUserContext> contexts = new ArrayList<>(ccl.size());
            for (MSNetConnectionContext cc : ccl) {
                contexts.add(cc.getUserContext());
            }
            return contexts;
        } finally {
            doUnlock(readLock());
        }
    }
    
    protected List<MSNetConnectionContext> getConnectionContexts() {
        doAcquire(readLock());
        try {
            return _getConnectionContexts();
        } finally {
            doUnlock(readLock());
        }
    }
    
    protected List<MSNetConnectionContext> _getConnectionContexts() {
        return new ArrayList<>(_connectionContextMap.values());
    }
    
    @Override
    public boolean hasConnection(MSNetID connectionName) {
        return getConnectionContext(connectionName) != null;
    }
    
    public void startListening() {
        doAcquire(writeLock());
        try {
            _startListening();
        } finally {
            doUnlock(writeLock());
        }
    }
    
    protected void _startListening() {
        _acceptor.listen(configurationSettings.getStartupFailurePolicy());
        if (_acceptor.getState() != MSNetTCPAcceptorStateEnum.LISTEN) {
            LOGGER.info("Server failed to start but no exception thrown (startup failure policy is "
                    + configurationSettings.getStartupFailurePolicy()
                    + "), skipping publication and acceptor registration. Error listeners will be notified, "
                    + "but startup sequence will continue.");
            notifyServerError(new MSNetServerException(_acceptor.getState().name()));
        } else {
            registerAcceptor();
            configurationSettings.setAcceptor(_acceptor);
            LOGGER.info("Started {}@{}", getName(), getAddress());
            publish();
            reportMateStartup();
        }
    }
    @VisibleForTesting
    Publisher authenticationPublisher() {
        return MateWTPublisher.INSTANCE;
    }
    
    private void reportMateStartup() {
        Map<String, String> startEvent = new HashMap<>();
        startEvent.put("server_user", System.getProperty("user.name"));
        startEvent.put("server_host", getAddress().getInetAddress().getHostAddress());
        startEvent.put("server_port", String.valueOf(getAddress().getPort()));
        MSNetKerberosEstablisherFactory est = findKerberosEstablisher();
        startEvent.put("kerberized", String.valueOf(est != null && !est.isOptional()));
        if (est != null) {
            startEvent.put("cb_enforcement_mode",
                    String.valueOf(est.getChannelBindingMode() == ChannelBindingMode.REQUIRED));
            startEvent.put("cb_vip_enabled",
                    ChannelBindingConfiguration.vipCBAllowedAddresses().stream().collect(joining(",")));
        }
        authenticationPublisher().accept(Publisher.EVENT_TYPE_START, startEvent);
    }
    protected final void publish() {
        if (publicationSupport != null) {
            publicationSupport.publish();
        }
    }
    protected final void unpublish() {
        if (publicationSupport != null) {
            publicationSupport.unpublish();
        }
    }
    private String prefix() {
        return requiresKerberos() ? "ktcp:
    }
    
    public void stopListening() {
        doAcquire(writeLock());
        try {
            _stopListening();
        } finally {
            doUnlock(writeLock());
        }
    }
    
    protected void _stopListening() {
        unpublish();
        unregisterAcceptor();
        MSNetTCPAcceptor acceptor = _acceptor;
        if (acceptor != null) {
            acceptor.close();
        }
    }
    
    @Override
    public void start() {
        afterPropertiesSet();
        state.startIfNotRunning(new Runnable() {
            @Override
            public void run() {
                doAcquire(writeLock());
                try {
                    validate();
                    oneTimeInitialization();
                    if (isStarted) {
                        return;
                    }
                    _start();
                    isStarted = true;
                } catch (Throwable t) {
                    LOGGER.error("Caught throwable in server startup, will be rethrown after cleanup", t);
                    isStarted = false;
                    _stop(true);
                    throw t;
                } finally {
                    doUnlock(writeLock());
                }
            }
        });
    }
    
    protected void _start() {
        _startListening();
    }
    
    protected void validate() {
        configurationSettings.validate();
        if (_forwardAcceptedEvents && (getMessageHandler() == null && getStreamHandler() == null)) {
            throw new IllegalArgumentException(
                    "\n You have decided to forward the individual connection events to a common server handler, \n however neither a message handler or a stream service handler has been set on the TCPServer "
                            + getName().getString()
                            + " being started.\n Resolutions \n 1. setMessageHandler(..) or setStreamHandler(...) on TCPServer. \n 2. You could also resolve this by setting _forwardAcceptedEvents to false while constructing the TCPServer");
        }
        state.throwIfInactive();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Starting server {} asynchronously", getName());
        }
    }
    
    @Override
    public void start(MSNetInetAddress address_) {
        doAcquire(writeLock());
        try {
            setAddress(address_);
            start();
        } finally {
            doUnlock(writeLock());
        }
    }
    protected final void stop(Runnable completionListener, final boolean closeAllConnections) {
        state.stopIfRunningCompletionListener(new Runnable() {
            @Override
            public void run() {
                isStarted = false;
                doAcquire(writeLock());
                try {
                    _stop(closeAllConnections);
                } finally {
                    doUnlock(writeLock());
                }
            }
        }, completionListener);
    }
    @Override
    public final void stop(Runnable completionListener) {
        stop(completionListener, true);
    }
    
    @Override
    public final void stop() {
        stop(null);
    }
    
    protected void _stop(boolean closeAllConnections) {
        _stopListening();
        if (closeAllConnections) {
            _closeAllConnections();
        }
        
        if (isServerLoopInternal) {
            try {
                isServerLoopInternal = false;
                MSNetLoop loop = getNetLoop();
                setLoop(null);
                if (loop.isLooping()) {
                    loop.quit();
                }
            } catch (Exception e) {
                LOGGER.warn("Exception occured while closing internal server loop", e);
            }
        }
    }
    
    @Override
    public boolean isListening() {
        return _acceptor.isListening();
    }
    
    @Override
    public boolean manageConnection(MSNetTCPConnection connection, boolean forwardEvents) {
        doAcquire(writeLock());
        try {
            if (_getConnectionContext(connection.getName()) != null) {
                
                return false;
            }
            
            if(!initContext(createManagedConnectionContext(connection, getNetLoopForConnection()), forwardEvents)) {
                throw new ConnectionLimitExceededException(
                        "Failed to manage connection "
                                + connection.getName()
                                + " by server "
                                + getName()
                                + ": connection limit exceeded for the group of the passed connection, it has been disconnected.");
            }
            
            return true;
        } finally {
            doUnlock(writeLock());
        }
    }
    
    @Override
    public void openConnection(MSNetInetAddress address, MSNetID connectionName, boolean forwardEvents) {
        MSNetTCPConnection connection = configurationSettings.getConnectionFactory().createConnection(
                configurationSettings.getLoop(), address, connectionName, false);
        
        doAcquire(writeLock());
        try {
            if(!initContext(createOpenedConnectionContext(connection, getNetLoopForConnection()), forwardEvents)) {
                throw new ConnectionLimitExceededException("Failed to open connection " + connectionName + " to "
                        + address + " by server " + getName()
                        + ": connection limit exceeded for the group of the new connection, it has been disconnected.");
            }
        } finally {
            doUnlock(writeLock());
        }
        for (MSNetEstablisherFactory factory : configurationSettings.getEstablishers()) {
            connection.addEstablisher(factory.createEstablisher());
        }
        connection.addListener(_connectListener);
        connection.enableReading();
        connection.connect();
    }
    
    public int getEstablisherFactoryCount() {
        return configurationSettings.getEstablishers().size();
    }
    private void checkStateBeforeModifyEstablisherList() {
        state.throwIfInitialized();
    }
    
    @Override
    public void addEstablisherFactory(MSNetEstablisherFactory newFactory) {
        checkStateBeforeModifyEstablisherList();
        List<MSNetEstablisherFactory> establisherFactories = configurationSettings.getEstablishers();
        
        if (establisherFactories.contains(newFactory)) {
			throw new IllegalArgumentException("MSNetTCPServer is already configured with an MSNetEstablisherFactory of type ["
					+ newFactory.getClass() + "], and multiple instances of this type cannot be configured.");
        }
        
        for (MSNetEstablisherFactory factory : establisherFactories) {
			if (factory.getClass().equals(newFactory.getClass())) {
				LOGGER.warn("MSNetTCPServer is already configured with an MSNetEstablisherFactory of type [{}]. "
					+ "Ensure that it does not always create establishers of the same priority.",  newFactory.getClass());
				break;
			}
		}
        
        establisherFactories.add(newFactory);
        if (newFactory instanceof ServerNameAware) {
            String serverName = configurationSettings.getServerName();
            if (serverName == null || serverName.isEmpty()) {
                LOGGER.warn(
                        "Establisher {} is serverName-aware, but the server does not have an ID set. Name will not be registered with the establsiher.",
                        newFactory);
            } else {
                ((ServerNameAware) newFactory).setServerName(serverName);
            }
        }
    }
    
    @Override
    public void removeEstablisherFactory(MSNetEstablisherFactory establisher) {
        checkStateBeforeModifyEstablisherList();
        configurationSettings.getEstablishers().remove(establisher);
    }
    
    @Override
    public Iterator<MSNetEstablisherFactory> establisherFactoryIterator() {
        return configurationSettings.getEstablishers().iterator();
    }
    
    protected void _insertEstablishingContext(MSNetConnectionContext context) {
        _establishingContextMap.put(context.getName(), context);
    }
    
    protected boolean _insertConnectionContext(MSNetConnectionContext context) {
        if (_connectionContextMap.put(context.getName(), context) == null) {
            return connectionLimitingServerStateListener.newConnectionCallback(context.getUserContext());
        }
        
        return true;
    }
    
    protected void openConnectionHandler(MSNetID connectionName) {
        MSNetConnectionContext context = getConnectionContext(connectionName);
        if (context != null) {
            notifyNewConnection(context.getUserContext());
            
            
            
            context.getConnection().removeListener(_connectListener);
        }
    }
    
    public void closeAllConnections() {
        if (doAcquireIfRunning(writeLock())) {
            try {
                _closeAllConnections();
            } finally {
                doUnlock(writeLock());
            }
        } else {
            LOGGER.debug("closeConnections called after the server was shutdown - skipping...");
        }
    }
    
    @GuardedBy("#writeLock()")
    protected void _closeAllConnections() {
        closeConnectionsInList(new ArrayList<>(_establishingContextMap.values()));
        closeConnectionsInList(_getConnectionContexts());
    }
    
    protected void closeConnectionsInList(List<MSNetConnectionContext> connectionContexts) {
        for (MSNetConnectionContext cCtx : connectionContexts) {
            MSNetConnection conn = cCtx.getConnection();
            if (conn.getState() != MSNetConnectionStateEnum.DISCONNECTED) {
                conn.close();
            }
        }
    }
    
    protected boolean cleanupConnection(MSNetID connectionName) {
        MSNetConnectionContext context = getConnectionContext(connectionName);
        if (context != null) {
            return cleanupConnection(context);
        }
        return false;
    }
    
    protected boolean cleanupConnection(MSNetConnectionContext context) {
        clearListeners(context);
        MSNetConnectionContextTypeEnum type = context.getType();
        if (type == MSNetConnectionContextTypeEnum.ACCEPTED_CONNECTION
                || type == MSNetConnectionContextTypeEnum.OPENED_CONNECTION) {
            markForDeath(context);
        }
        unregisterConnection(context);
        return true;
    }
    
    protected void markForDeath(MSNetConnectionContext context) {
        synchronized (_deadConnectionList) {
            _deadConnectionList.add(context);
            configurationSettings.getLoop().callbackAfterDelay(0, _deadConnectionCleaner);
        }
    }
    
    protected void removeDeadConnections() {
        synchronized (_deadConnectionList) {
            closeConnectionsInList(_deadConnectionList);
            _deadConnectionList.clear();
        }
    }
    
    protected boolean cleanupAndRemoveConnection(MSNetID connectionName, boolean notifyRemoveListeners) {
        MSNetConnectionContext context = null;
        if (doAcquireIfRunning(writeLock())) {
            try {
                context = _removeConnectionContext(connectionName);
                if (context == null) {
                    return false;
                }
                MSNetTCPConnection conn = context.getConnection();
                
                conn.setRetryFlag(false);
                conn.clearRetryHandler();
                if (conn.getState() != MSNetConnectionStateEnum.DISCONNECTED) {
                    conn.close();
                }
                if (!cleanupConnection(context)) {
                    return false;
                }
            } finally {
                doUnlock(writeLock());
            }
        } else {
            LOGGER.debug("cleanupAndRemoveConnection called after the server was shutdown - skipping...");
            return false;
        }
        if (notifyRemoveListeners) {
            notifyRemoveConnection(context.getUserContext());
        }
        return true;
    }
    
    protected void clearListeners(MSNetConnectionContext context) {
        MSNetTCPConnection connection = context.getConnection();
        connection.removeListener(_connectListener);
        connection.removeInternalConnectionListener(connectionListener);
        connection.removeListener(_connectionEventForwarder);
    }
    
    protected void connectionErrorHandler(MSNetID connectionName, MSNetConnectionErrorEnum error) {
        cleanupAndRemoveConnection(connectionName, true);
    }
    
    protected void connectionReadHandler(MSNetID connectionName, MSNetMessage message) {
        Thread.yield();
        MSNetTCPServerUserContext userContext = createUserContext(connectionName);
        notifyMessage(userContext, message);
    }
    
    protected void connectionStreamHandler(MSNetID connectionName, MSNetMessageInputStream in) {
        Thread.yield();
        MSNetTCPServerUserContext userContext = createUserContext(connectionName);
        notifyStream(userContext, in);
    }
    
    private MSNetTCPServerUserContext createUserContext(MSNetID connectionName) {
        MSNetConnectionContext context = getConnectionContext(connectionName);
        MSNetTCPServerUserContext userContext = context.getUserContext();
        return userContext;
    }
    
    @Override
    public boolean reparentConnection(MSNetID connectionName, MSNetLoop newLoop) {
        
        boolean rv = false;
        if (doAcquireIfRunning(writeLock())) {
            try {
                MSNetConnectionContext context = _removeConnectionContext(connectionName);
                
                
                
                if (context != null && context.getType() == MSNetConnectionContextTypeEnum.MANAGED_CONNECTION) {
                    
                    
                    clearListeners(context);
                    unregisterConnection(context);
                    context.reparent(newLoop);
                    rv = true;
                }
            } finally {
                doUnlock(writeLock());
            }
        } else {
            LOGGER.debug("promoteConnection called after the server was shutdown - skipping...");
        }
        return rv;
    }
    
    @Override
    public boolean removeConnection(MSNetID connectionName) {
        return cleanupAndRemoveConnection(connectionName, false);
    }
    
    protected MSNetConnectionContext _removeConnectionContext(MSNetID connectionName) {
        MSNetConnectionContext context = _connectionContextMap.remove(connectionName);
        if (context != null) {
            connectionLimitingServerStateListener.removeConnectionCallback(context.getUserContext());
        }
        return context;
    }
    
    protected MSNetConnectionContext _removeEstablishingContext(MSNetID connectionName) {
        return _establishingContextMap.remove(connectionName);
    }
    
    protected MSNetConnectionContext getConnectionContext(MSNetID connectionName) {
        doAcquire(readLock());
        try {
            return _getConnectionContext(connectionName);
        } finally {
            readLock().unlock();
        }
    }
    
    protected MSNetConnectionContext _getConnectionContext(MSNetID connectionName) {
        return _connectionContextMap.get(connectionName);
    }
    
    protected void registerEventForwarder(MSNetTCPConnection connection) {
        if (connection != null) {
            connection.addListener(_connectionEventForwarder);
        }
    }
    protected void registerListeners(final MSNetConnectionContext context) {
        MSNetTCPConnection connection = context.getConnection();
        
        if (connection != null) {
            connection.addInternalConnectionListener(connectionListener);
        }
    }
    
    protected boolean initContext(MSNetConnectionContext context, boolean forwardEvents) {
        registerListeners(context); 
        
        if(!_insertConnectionContext(context)) { 
            LOGGER.info("Connection rejected by the limiter, abandoning promotion");
            return false;
        }
        
        registerConnection(context);
        if (forwardEvents) {
            initConnection(context);
        }
        
        return true;
    }
    
    protected void initConnection(MSNetConnectionContext context) {
        registerEventForwarder(context.getConnection());
    }
    
    protected void acceptHandler(MSNetTCPSocket socket) {
        if (socket == null) {
            
            notifyServerError(new MSNetServerException(_acceptor.getState().name()));
            return;
        }
        if (doAcquireIfRunning(writeLock())) {
            try {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Connection from {} to {}", socket.getAddress(), getAddress());
                }
                MSNetConnectionContext context = createAcceptedConnectionContext(socket, getNetLoopForConnection());
                MSNetTCPConnection connection = context.getConnection();
                if (isKerberosEncryption())
                    connection.setKerberosEncryption(true);
                if (getChannelBindingMode() != null) {
                    connection.setChannelBindingMode(getChannelBindingMode());
                }
                MasterEstablisherMode mem = masterEstablisherMode;
                boolean skipImplicitKerberos = !validateConnectionEstablishers(connection);
                if (skipImplicitKerberos) {
                    LOGGER.info("Not adding optional kerberos authentication, connection has conflicting establishers.");
                    mem = originalMEMode;
                    for (MSNetEstablisherFactory netEstablisherFactory : configurationSettings.getEstablishers()) {
                        if (netEstablisherFactory != implicitKerberosFactory)
                            connection.addEstablisher(netEstablisherFactory.createEstablisher());
                    }                    
                } else {
                    
                    for (MSNetEstablisherFactory netEstablisherFactory : configurationSettings.getEstablishers()) {
                        connection.addEstablisher(netEstablisherFactory.createEstablisher());
                    }
                }
                connection.setRetryFlag(false);
                _insertEstablishingContext(context);
                connection.addListener(new EstablishListener());
                connection.setMasterEstablisherMode(mem);
                connection.initFromSocket();
                getMetrics().connectionAccepted();
            } finally {
                doUnlock(writeLock());
            }
        } else {
            LOGGER.debug("accept handler called after the server was shutdown - skipping...");
        }
    }
    private boolean validateConnectionEstablishers(MSNetTCPConnection connection) {
        MasterEstablisherMode mem = masterEstablisherMode;
        Iterator<MSNetEstablisher> itr = connection.establisherIterator();
        if (implicitKerberosFactory != null && itr.hasNext()) {
            if (EstablishersValidator.hasKerberosEstablisher(connection) || (mem == MasterEstablisherMode.BACKCOMPAT
                    && EstablishersValidator.hasNonOptionalEstablishersAfterKerberos(connection))) {
                return false;
            }
        }
        return true;
    }
    
    
    protected void promoteConnection(MSNetID connectionName) {
        final MSNetConnectionContext ctx = guardedRemoveEstablishingContext(connectionName);
        if (ctx == null) {
            return;
        }
        if (doAcquireIfRunning(writeLock())) {
            try {
                postProcessContextOnPromotion(ctx);
                if (!initContext(ctx, _forwardAcceptedEvents)) {
                    return;
                }
                notifyNewConnection(ctx.getUserContext());
                
                
                MSNetLoop loop = ctx.getConnection().getNetLoop();
                loop.callbackAfterDelay(0, (new MSNetEventListener() {
                    @Override
                    public void eventOccurred(MSNetEvent e) {
                        ctx.getConnection().enableReading();
                    }
                }));
            } finally {
                doUnlock(writeLock());
            }
        } else {
            LOGGER.debug("promoteConnection called after the server was shutdown - skipping...");
        }
    }
    protected void postProcessContextOnPromotion(MSNetConnectionContext ctx) {
        ctx.getUserContext().copyAuthContext();
    }
    
    protected abstract MSNetConnectionContext createManagedConnectionContext(MSNetTCPConnection connection,
                                                                             MSNetLoop loop);
    
    protected abstract MSNetConnectionContext createAcceptedConnectionContext(MSNetTCPSocket socket, MSNetLoop loop);
    
    protected abstract MSNetConnectionContext createOpenedConnectionContext(MSNetTCPConnection connection,
                                                                            MSNetLoop loop);
    
    protected abstract MSNetLoop getNetLoopForConnection();
    
    @Override
    public void setAdminManager(msjava.msnet.admin.MSNetAdminManager adminManager) {
        configurationSettings.setAdminManager(adminManager);
        
        registerAcceptor();
        for (MSNetTCPServerUserContext ctx : getUserContexts()) {
            configurationSettings.getAdminManager().registerEntity(new MSNetAdminableConnection(ctx.getConnection()));
        }
    }
    public void setControlsMBean(MSNetJMXControls mbean) {
        configurationSettings.setControlsMBean(mbean);
    }
    
    @Override
    public MSNetTCPConnection getConnection(MSNetID connectionName) {
        MSNetConnectionContext context = getConnectionContext(connectionName);
        if (context != null) {
            return context.getConnection();
        }
        return null;
    }
    
    protected void registerConnection(MSNetConnectionContext context) {
        if (context != null) {
            if (configurationSettings.getAdminManager() != null) {
                configurationSettings.getAdminManager().registerEntity(
                        new MSNetAdminableConnection(context.getConnection()));
            }
            if (configurationSettings.getControlsMBean() != null) {
                configurationSettings.getControlsMBean().registerConnection(context.getConnection());
            }
        }
    }
    
    protected void unregisterConnection(MSNetConnectionContext context) {
        if (context != null) {
            if (configurationSettings.getAdminManager() != null) {
                configurationSettings.getAdminManager().unregisterEntity(context.getName());
            }
            if (configurationSettings.getControlsMBean() != null) {
                configurationSettings.getControlsMBean().unregisterConnection(context.getConnection());
            }
        }
    }
    
    protected void registerAcceptor() {
        if (_acceptor != null) {
            if (configurationSettings.getAdminManager() != null) {
                MSNetAdminableTCPAcceptor aa = new MSNetAdminableTCPAcceptor(_acceptor);
                _adminableAcceptorName = aa.getName();
                configurationSettings.getAdminManager().registerEntity(aa);
            }
            if (configurationSettings.getControlsMBean() != null) {
                configurationSettings.getControlsMBean().registerAcceptor(_acceptor);
            }
        }
    }
    
    protected void unregisterAcceptor() {
        if (_acceptor != null) {
            if (configurationSettings.getAdminManager() != null) {
                configurationSettings.getAdminManager().unregisterEntity(_adminableAcceptorName);
                _adminableAcceptorName = null;
            }
            if (configurationSettings.getControlsMBean() != null) {
                configurationSettings.getControlsMBean().unregisterAcceptor(_acceptor);
            }
        }
    }
    
    @Deprecated
    protected MSNetID getNextID() {
        return new MSNetID(configurationSettings.getServerName() + "#" + _connectionCount.incrementAndGet());
    }
    
    protected MSNetID getNextID(MSNetTCPSocket socket) {
        String remoteAddress = socket.getAddress().toString().replace(':', '-');
        ;
        return new MSNetID(configurationSettings.getServerName() + "#" + _connectionCount.incrementAndGet() + "#"
                + remoteAddress);
    }
    
    protected MSNetConnectionContext createConnectionContext(MSNetLoop loop, MSNetTCPConnection connection,
                                                             MSNetConnectionContextTypeEnum type) {
        return new MSNetConnectionContext(loop, type, createUserContext(connection));
    }
    
    protected MSNetTCPServerUserContext createUserContext(MSNetTCPConnection conn) {
        return new MSNetTCPServerUserContext(conn);
    }
    
    @Override
    public List<TCPServerStateListener> getStateListeners() {
        return Collections.unmodifiableList(stateListeners);
    }
    public void setStateListeners(List<TCPServerStateListener> stateListeners) {
        for (TCPServerStateListener serverStateListener : stateListeners) {
            setMessageHandlerIfOldListener(serverStateListener);
        }
        this.stateListeners = stateListeners;
    }
    
    
    public void addConnectionLimitListener(TCPServerConnectionLimitListener listener) {
        connectionLimitListeners.add(listener);
    }
    
    public void removeConnectionLimitListener(TCPServerConnectionLimitListener listener) {
        connectionLimitListeners.remove(listener);
    }
    public TCPServerStreamHandler getStreamHandler() {
        return streamHandler;
    }
    @Override
    public void setStreamHandler(TCPServerStreamHandler streamHandler) {
        assert _forwardAcceptedEvents;
        Assert.isNull(this.messageHandler, "Cant set stream handler since a message handler has been set");
        Assert.isTrue(this.idleTimeout == IdleTimeoutScheduler.TIMEOUT_VALUE_NONE,
                "Cannot set stream handler since an idle timeout has been set on " + getName());
        Assert.notNull(streamHandler, "Stream Service Handler set cannot be null");
        this.streamHandler = streamHandler;
    }
    public TCPServerMessageHandler getMessageHandler() {
        return messageHandler;
    }
    @Override
    public void setMessageHandler(TCPServerMessageHandler messageHandler) {
        assert _forwardAcceptedEvents;
        Assert.isNull(this.streamHandler, "Cant set stream handler since a stream handler has been set");
        Assert.notNull(messageHandler, "Message Handler set cannot be null");
        this.messageHandler = messageHandler;
    }
    public Executor getExecutor() {
        return configurationSettings.getExecutor();
    }
    public void setMessageExecutor(Executor executor) {
        configurationSettings.setExecutor(executor);
    }
    
    public static MSNetInetAddress createAddressFromString(String hostPort) {
        try {
            return new MSNetInetAddress(hostPort);
        } catch (UnknownHostException e) {
            throw new MSNetRuntimeException(e);
        }
    }
    protected boolean isForwardAcceptedEvents() {
        return _forwardAcceptedEvents;
    }
    
    public void setForwardAcceptedEvents(boolean forwardAcceptedEvents) {
        if (isStarted) {
            throw new IllegalStateException("Cannot modify forward events setting after the server has started");
        }
        this._forwardAcceptedEvents = forwardAcceptedEvents;
    }
    public boolean isStarted() {
        return isStarted;
    }
    static abstract class MSNetAbstractTCPServerConfiguration {
        public static final boolean DEFAULT_STREAMING_ENABLED = false;
        private MSNetTCPAcceptor acceptor;
        private MSNetInetAddress address;
        
        private MSNetTCPSocketFactory socketFactory = MSNetConfiguration.getDefaultMSNetTCPSocketFactory();
        private MSNetServerStartupPolicyEnum startupFailurePolicy = MSNetServerStartupPolicyEnum.FAIL_ON_EXCEPTION;
        private int connectionPerHostLimit = MSNetConfiguration.DEFAULT_CONNECTION_PER_HOST_LIMIT;
        private int connectionPerHostWarnThreshold = MSNetConfiguration.DEFAULT_CONNECTION_PER_HOST_WARN_THRESHOLD;
        private ConnectionLimitPolicy connectionLimitPolicy = ConnectionLimitPolicy.HOST;
        
        private ServicePublisher servicePublisher;
        private final PublicationAttributesSupport publicationAttributes = new PublicationAttributesSupport();
        private boolean disableImplicitKerberos = MSNetConfiguration.DISABLE_IMPLICIT_KERBEROS;
        
        private boolean kerberos;
        private boolean anonymousAuthentication;
        private boolean kerberosEncryption = false;
        private ChannelBindingMode channelBindingMode;
        public ServicePublisher getServicePublisher() {
            return servicePublisher;
        }
        public void setServicePublisher(ServicePublisher servicePublisher) {
            this.servicePublisher = servicePublisher;
        }
        public String getServiceName() {
            return ServiceName.forServiceAttributes(publicationAttributes.get());
        }
        public void setServiceName(String serviceName) {
            publicationAttributes.mergeWith(serviceName);
        }
     
        public Map<String, Object>  getPublicationAttributes() {
            return publicationAttributes.get();
        }
        
        public void setPublicationAttributes(Map<String, Object> attributes) {
            publicationAttributes.mergeWith(attributes);
        }
        protected ServiceAttributes publicationAttributes() {
            return publicationAttributes.get();
        }
        
        public abstract MSNetLoop getLoop();
        private final void setAcceptor(MSNetTCPAcceptor acceptor) {
            this.acceptor = acceptor;
        }
        
        public abstract String getServerName();
        public abstract msjava.msnet.admin.MSNetAdminManager getAdminManager();
        public abstract MSNetJMXControls getControlsMBean();
        public abstract MSNetTCPConnectionFactory getConnectionFactory();
        public abstract void setConnectionFactory(MSNetTCPConnectionFactory connectionFactory);
        public abstract List<MSNetEstablisherFactory> getEstablishers();
        public abstract Executor getExecutor();
        public MSNetInetAddress getConfiguredAddress() {
            return address;
        }
        
        public void setConfiguredAddress(MSNetInetAddress address) {
        	this.address = address;
        }
        
        public final String getHostPort() {
        	if (acceptor != null) {
        		return acceptor.getAddress().getAddressString();
        	}
        	
        	return getConfiguredHostPort();
        }
        
        public final String getConfiguredHostPort() {
        	return getConfiguredAddress().getAddressString();
        }
        
        
        public MSNetTCPSocketFactory getSocketFactory() {
            return socketFactory;
        }
        
        
        public void setSocketFactory(MSNetTCPSocketFactory socketFactory) {
            Objects.requireNonNull(socketFactory, "The socket factory cannot be null");
            this.socketFactory = socketFactory;
        }
        public abstract void setAdminManager(MSNetAdminManager adminManager);
        public abstract void setControlsMBean(MSNetJMXControls mbean);
        public abstract void setBeanName(String name);
        public abstract void setExecutor(Executor executor);
        public abstract void setEstablishers(List<MSNetEstablisherFactory> establishers);
        protected void validate() {
            Assert.notNull(
                    getServerName(),
                    "Server name is not allowed to be a null, use the either setBeanName(..) or setName(..) method to set a name for the server or just assign a bean name in your spring config!");
            Assert.notNull(getConfiguredAddress(),
                    "You need to supply an address during configuration");
        }
        
        public void setHostPort(String hostPort) {
        	setConfiguredAddress(createAddressFromString(hostPort));
        }
        
        
        public abstract void setLoop(MSNetLoop loop);
        public void setStartupFailurePolicy(MSNetServerStartupPolicyEnum startupPolicy) {
            this.startupFailurePolicy = startupPolicy;
        }
        public MSNetServerStartupPolicyEnum getStartupFailurePolicy() {
            return startupFailurePolicy;
        }
        
        public int getConnectionPerHostLimit() {
            return connectionPerHostLimit;
        }
        
        public void setConnectionPerHostLimit(int connectionPerHostLimit) {
            this.connectionPerHostLimit = connectionPerHostLimit;
        }
        
        public int getConnectionPerHostWarnThreshold() {
            return connectionPerHostWarnThreshold;
        }
        
        public void setConnectionPerHostWarnThreshold(int connectionPerHostWarnThreshold) {
            this.connectionPerHostWarnThreshold = connectionPerHostWarnThreshold;
        }
        
        public void setConnectionLimitPolicy(ConnectionLimitPolicy connectionLimitPolicy) {
            this.connectionLimitPolicy = connectionLimitPolicy;
        }
        public ConnectionLimitPolicy getConnectionLimitPolicy() {
            return connectionLimitPolicy;
        }
        
        public void setConnectionLimitPolicyName(String connectionLimitPolicyName) throws ClassCastException,
                InstantiationException, IllegalAccessException, ClassNotFoundException {
            setConnectionLimitPolicy(createConnectionLimitPolicy(connectionLimitPolicyName));
        }
        private ConnectionLimitPolicy createConnectionLimitPolicy(String connectionLimitPolicyName)
                throws InstantiationException, IllegalAccessException, ClassNotFoundException, ClassCastException {
            if ("NONE".equals(connectionLimitPolicyName))
                return ConnectionLimitPolicy.NONE;
            else if ("HOST".equals(connectionLimitPolicyName))
                return ConnectionLimitPolicy.HOST;
            else if ("USER".equals(connectionLimitPolicyName))
                return ConnectionLimitPolicy.USER;
            return (ConnectionLimitPolicy) Class.forName(connectionLimitPolicyName).newInstance();
        }
        public final void setKerberos(boolean kerberos) {
            if (!kerberos) {
                this.disableImplicitKerberos = true;
            }
        	this.kerberos = kerberos;
        }
        @Experimental
        public final void setKerberosEncryption(boolean kerberosEncryption) {
            this.kerberosEncryption = kerberosEncryption;
        }
        public final void setAnonymousAuthentication(boolean anonAuthetication) {
        	this.anonymousAuthentication = anonAuthetication;
        }
        public final boolean isAnonymousAuthentication() {
        	return this.anonymousAuthentication;
        }
        public final boolean isKerberos() {
        	return this.kerberos;
        }
        public final boolean isKerberosEncryption() {
            return this.kerberosEncryption;
        }
        
        public ChannelBindingMode getChannelBindingMode() {
            return channelBindingMode;
        }
        
        public void setChannelBindingMode(ChannelBindingMode channelBindingMode) {
            this.channelBindingMode = channelBindingMode;
        }
    }
    
    class DefaultConfiguration extends MSNetAbstractTCPServerConfiguration {
        private Executor executor;
        private MSNetJMXControls controls;
        public DefaultConfiguration(MSNetLoop loop, MSNetInetAddress address, MSNetID serverName,
                                    MSNetAdminManager adminManager, MSNetTCPConnectionFactory connectionFactory,
                                    List<MSNetEstablisherFactory> establishers, Executor executor) {
            _loop = loop;
            _connectionFactory = connectionFactory;
            if (establishers != null) {
                _establishers = establishers;
            }
            _serverName = serverName;
            setAdminManager(adminManager);
            setExecutor(executor);
            setConfiguredAddress(address);
        }
        @Override
        public MSNetAdminManager getAdminManager() {
            return _adminManager;
        }
		@Override
        public MSNetJMXControls getControlsMBean() {
            return controls;
        }
        @Override
        public MSNetTCPConnectionFactory getConnectionFactory() {
            return _connectionFactory;
        }
        @Override
        public void setConnectionFactory(MSNetTCPConnectionFactory connectionFactory) {
            _connectionFactory = connectionFactory;
        }
        @Override
        public List<MSNetEstablisherFactory> getEstablishers() {
            return _establishers;
        }
        @Override
        public final void setEstablishers(List<MSNetEstablisherFactory> establishers) {
            _establishers = establishers;
        }
        @Override
        public Executor getExecutor() {
            return executor;
        }
        @Override
        public MSNetLoop getLoop() {
            return _loop;
        }
        @Override
        public void setLoop(MSNetLoop loop) {
            _loop = loop;
        }
        @Override
        public String getServerName() {
            if (_serverName == null) {
                return null;
            }
            return _serverName.getString();
        }
        @Override
        public void setAdminManager(MSNetAdminManager adminManager) {
            _adminManager = adminManager;
        }
        @Override
        public void setControlsMBean(MSNetJMXControls mbean) {
            this.controls = mbean;
        }
        @Override
        public void setBeanName(String name) {
            _serverName = new MSNetID(name);
        }
        @Override
        public void setExecutor(Executor executor) {
            this.executor = executor;
        }
    }
    
    public static class Configuration extends MSNetAbstractTCPServerConfiguration {
        private MSNetLoop loop;
        private String serverName;
        private msjava.msnet.admin.MSNetAdminManager adminManager;
        private MSNetJMXControls controls;
        private MSNetTCPConnectionFactory connectionFactory;
        private List<MSNetEstablisherFactory> establishers = new ArrayList<>();
        private Executor executor;
        
        
        
        protected Configuration() {
        }
        protected Configuration(MSNetLoop loop, MSNetInetAddress address, MSNetID serverName,
                                MSNetTCPConnectionFactory connectionFactory) {
            super();
            this.loop = loop;
            this.serverName = serverName.getString();
            this.connectionFactory = connectionFactory;
            setConfiguredAddress(address);
        }
        @Override
        public final MSNetLoop getLoop() {
            return loop;
        }
        @Override
        public final void setLoop(MSNetLoop loop) {
            this.loop = loop;
        }
        @Override
        public final String getServerName() {
            return serverName;
        }
        @Override
        public final msjava.msnet.admin.MSNetAdminManager getAdminManager() {
            return adminManager;
        }
        @Override
        public final void setAdminManager(msjava.msnet.admin.MSNetAdminManager adminManager) {
            this.adminManager = adminManager;
        }
        @Override
        public MSNetJMXControls getControlsMBean() {
            return controls;
        }
        @Override
        public void setControlsMBean(MSNetJMXControls controls) {
            this.controls = controls;
        }
        @Override
        public final MSNetTCPConnectionFactory getConnectionFactory() {
            return connectionFactory;
        }
        @Override
        public final void setConnectionFactory(MSNetTCPConnectionFactory connectionFactory) {
            this.connectionFactory = connectionFactory;
        }
        @Override
        public final List<MSNetEstablisherFactory> getEstablishers() {
            return establishers;
        }
        @Override
        public final void setEstablishers(List<MSNetEstablisherFactory> establishers) {
            this.establishers = establishers;
        }
        @Override
        public final Executor getExecutor() {
            return executor;
        }
        @Override
        public void setExecutor(Executor executor) {
            this.executor = executor;
        }
        @Override
        public final void setBeanName(String name) {
            doSetBeanName(name);
        }
        protected void doSetBeanName(String beanName) {
            this.serverName = beanName;
        }
        
        
        @Deprecated
        protected void doSetHostPort(String hostPort) {
        	setHostPort(hostPort);
        }
    }
    private void oneTimeInitialization() {
        doAcquire(writeLock());
        try {
            if (configurationSettings.getLoop() == null) {
                LOGGER.info("No loop on the server! Creating one.");
                configurationSettings.setLoop(makeServerLoop());
                isServerLoopInternal = true;
            }
            _acceptor = new MSNetTCPAcceptor(configurationSettings.getLoop(),
                    configurationSettings.getConfiguredAddress(), configurationSettings.getSocketFactory());
            _acceptor.addListener(new AcceptorListener());
            _metrics = createMetrics();
        } finally {
            doUnlock(writeLock());
        }
    }
    @Override
    public void afterPropertiesSet() {
        state.initializeIfNotInitialized(new Runnable() {
            @Override
            public void run() {
                configurationSettings.validate();
                boolean krbRequired = false;
                boolean krbEnabled = false;
                
                MSNetKerberosEstablisherFactory kerberosEstablisher = findKerberosEstablisher();
                if (configurationSettings.isKerberos()) {
                    if (kerberosEstablisher == null) {
                        try {
                            MSNetKerberosEstablisherFactory kerberosFactory = new MSNetKerberosEstablisherFactory();
                            
                            if (isKerberosEncryption()) {
                                kerberosFactory.enableEncryption();
                            }
                            if (getChannelBindingMode() != null) {
                                kerberosFactory.setChannelBindingMode(getChannelBindingMode());
                            }
                            addEstablisherFactory(kerberosFactory);
                            LOGGER.info(
                                    "Server initialized with setKerberos(true) - kerberos enabled WITHOUT any authorization.");
                        } catch (MSNetAuthenticationException e) {
                            throw new MSNetRuntimeException(
                                    "Caught an exception, while making the establisher factory ", e);
                        }
                    }
                    krbRequired = true;
                    krbEnabled = true;
                } else {
                    if (kerberosEstablisher != null) {
                        krbEnabled = true;
                        krbRequired = !kerberosEstablisher.isOptional();
                        if (kerberosEstablisher.isAuthorizationEnabled()) {
                            LOGGER.info("Kerberos establisher present with authorizer {}",
                                    kerberosEstablisher.getAuthorizer());
                        } else {
                            LOGGER.info("Kerberos establisher present WITHOUT ANY AUTHORIZATION");
                        }
                    }
                }
                if (!krbRequired) {
                    MSKerberosAuthority.assertKerberizedServerNotEnforced("msjava.msnet.server");
                }
                originalMEMode = masterEstablisherMode;
                if (configurationSettings.isAnonymousAuthentication()) {
                    addEstablisherFactory(new MSNetAnonymousEstablisherFactory());
                } else if (!krbEnabled) {
                    turnOnOptionalKerberosIfPossible();
                }
                doCreatePublicationSupport();
            }
        });
    }
    
    
    private void turnOnOptionalKerberosIfPossible() {
        if (configurationSettings.disableImplicitKerberos) {
            LOGGER.info("Implicit kerberization disabled\n" + "More info: " + AUTO_MASTER_EST_URL);
            return;
        }
        
        if (connectTimeout > 0) {
            LOGGER.info("Implicit kerberization is disabled if the server connect timeout is set. More info: " + AUTO_MASTER_EST_URL);
            return;
        }
        try {
            MSNetKerberosEstablisherFactory kerberosFactory;
            try {
                kerberosFactory = new MSNetKerberosEstablisherFactory();
            } catch (MSNetAuthenticationException | SecurityException e) {
                LOGGER.info("Cannot automatically enable optional Kerberos because of " + e);
                return;
            }
            kerberosFactory.setOptional(true);
            
            ArrayList<MSNetEstablisher> establishers = new ArrayList<>();
            for (MSNetEstablisherFactory establisherFactory : configurationSettings.getEstablishers()) {
                establishers.add(establisherFactory.createEstablisher());
            }
            establishers.add(kerberosFactory.createEstablisher());
            
            ValidationResult result = EstablishersValidator.validateME(masterEstablisherMode == MasterEstablisherMode.OFF
                    ? MasterEstablisherMode.BACKCOMPAT : masterEstablisherMode, true, establishers);
            
            if (result.isError()) {
                LOGGER.warn(
                        "Cannot automatically enable optional Kerberos with master establisher in \"BACKCOMPAT\" "
                                + "mode as validation of the benchmark master establisher failed with the below exception\n"
                                + "More info: " + AUTO_MASTER_EST_URL + ". Validation message: {}",
                        result);
                return;
            }
            
            String before = String.format("(ME=%s,KRB=%s)", masterEstablisherMode, "OFF");
            if (masterEstablisherMode == MasterEstablisherMode.OFF) {
                masterEstablisherMode = MasterEstablisherMode.BACKCOMPAT;
                LOGGER.info("Master Establisher Mode automatically set to BACKCOMPAT");
            }
            
            this.implicitKerberosFactory = kerberosFactory;
            addEstablisherFactory(kerberosFactory);
            String after = String.format("(ME=%s,KRB=%s)", masterEstablisherMode, "ON");
            LOGGER.info("Kerberos optionally enabled automatically without any authorization on server " + getName()
                    + "; " + before + " -> " + after + ".\n" + "More info: " + AUTO_MASTER_EST_URL);
        } catch (Exception e) {
            LOGGER.warn("Implicitly turning on Kerberos failed\n" + "More info: " + AUTO_MASTER_EST_URL, e);
        }
    }
    private MSNetKerberosEstablisherFactory findKerberosEstablisher() {
        for (MSNetEstablisherFactory establisher : configurationSettings.getEstablishers()) {
            if (establisher instanceof MSNetKerberosEstablisherFactory) {
                return (MSNetKerberosEstablisherFactory) establisher;
            }
        }
        return null;
    }
    
    protected boolean requiresKerberos() {
        MSNetKerberosEstablisherFactory kef = findKerberosEstablisher();
        return kef != null && !kef.isOptional();
    }
    
    
    public MSNetTCPSocketFactory getSocketFactory() {
        return configurationSettings.getSocketFactory();
    }
    
    public void setSocketFactory(MSNetTCPSocketFactory socketFactory) {
        configurationSettings.setSocketFactory(socketFactory);
    }
    @Override
    public void setLoop(MSNetLoop loop) {
        configurationSettings.setLoop(loop);
    }
    
    public void setHostPort(String hostPort) {
        configurationSettings.setConfiguredAddress(createAddressFromString(hostPort));
    }
    @Override
    public void setBeanName(String name) {
        configurationSettings.setBeanName(name);
    }
    public final void setKerberos(boolean kerberos) {
        configurationSettings.setKerberos(kerberos);
    }
    @Experimental
    public final void setKerberosEncryption(boolean encryption) {
        configurationSettings.setKerberosEncryption(encryption);
    }
    
    public void setChannelBindingMode(ChannelBindingMode channelBindingMode) {
        configurationSettings.setChannelBindingMode(channelBindingMode);
    }
    
    public ChannelBindingMode getChannelBindingMode() {
        return configurationSettings.getChannelBindingMode(); 
    }
    
    @Deprecated
    public final void setAnonymousAuthentication(boolean anonAuthetication) {
        configurationSettings.setAnonymousAuthentication(anonAuthetication);
    }
    public final boolean isAnonymousAuthentication() {
        return configurationSettings.isAnonymousAuthentication();
    }
    public final boolean isKerberos() {
        return configurationSettings.isKerberos();
    }
    public final boolean isKerberosEncryption() { return configurationSettings.isKerberosEncryption();}
    public void setIdleTimeout(long timeout) {
        if (timeout != IdleTimeoutScheduler.TIMEOUT_VALUE_NONE && getStreamHandler() != null) {
            throw new IllegalArgumentException("Cannot set an idle timeout if streaming is enabled");
        }
        idleTimeout = timeout;
    }
    
    public void setAsyncConnectTimeout(long timeout) {
        connectTimeout = timeout;
    }
    
    public void setListeners(List<TCPServerStateListener> listeners) {
        for (TCPServerStateListener listener : listeners) {
            addStateListener(listener);
        }
    }
    public void setOrder(int order) {
        this.order = order;
    }
    protected static MSNetLoop makeServerLoop() {
        MSNetLoopThread thread = new MSNetLoopThread("Server Loop");
        thread.setDaemon(false);
        thread.setPriority(Thread.MAX_PRIORITY);
        return thread.startLoop();
    }
    public void setEstablishers(List<MSNetEstablisherFactory> establishers) {
        configurationSettings.setEstablishers(establishers);
    }
    @Override
    public void setStartupFailurePolicy(MSNetServerStartupPolicyEnum startupPolicy) {
        configurationSettings.setStartupFailurePolicy(startupPolicy);
    }
    
    public int getConnectionPerHostLimit() {
        return configurationSettings.getConnectionPerHostLimit();
    }
    
    public void setConnectionPerHostLimit(int connectionPerHostLimit) {
        configurationSettings.setConnectionPerHostLimit(connectionPerHostLimit);
    }
    
    public int getConnectionPerHostWarnThreshold() {
        return configurationSettings.getConnectionPerHostWarnThreshold();
    }
    
    public void setConnectionPerHostWarnThreshold(int connectionPerHostWarnThreshold) {
        configurationSettings.setConnectionPerHostWarnThreshold(connectionPerHostWarnThreshold);
    }
    
    @Override
    public boolean isAutoStartup() {
        return true;
    }
    
    @Override
    public boolean isRunning() {
        return this.isStarted;
    }
    
    @Override
    public int getPhase() {
        return order;
    }
    
    public MasterEstablisherMode getMasterEstablisherMode() {
        return masterEstablisherMode;
    }
    
    public void setMasterEstablisherMode(MasterEstablisherMode masterEstablisherMode) {
        if (masterEstablisherMode == MasterEstablisherMode.OFF) {
            configurationSettings.disableImplicitKerberos = true;
        }
        this.masterEstablisherMode = masterEstablisherMode;
    }
    
    @Deprecated
    public MasterEstablisherMode getExperimentalMasterEstablisherMode() {
        return getMasterEstablisherMode();
    }
    
    @Deprecated
    public void setExperimentalMasterEstablisherMode(MasterEstablisherMode masterEstablisherMode) {
        setMasterEstablisherMode(masterEstablisherMode);
    }
    
    public ConnectionLimitPolicy getConnectionLimitPolicy() {
        return configurationSettings.getConnectionLimitPolicy();
    }
    
    public void setConnectionLimitPolicy(ConnectionLimitPolicy connectionLimitPolicy) {
        this.configurationSettings.setConnectionLimitPolicy(connectionLimitPolicy);
    }
    
    public void setConnectionLimitPolicyName(String connectionLimitPolicyName) throws ClassCastException,
            InstantiationException, IllegalAccessException, ClassNotFoundException {
        configurationSettings.setConnectionLimitPolicyName(connectionLimitPolicyName);
    }
    @Override
    public ServiceDescription buildServiceDescription() {
        if (configurationSettings.publicationAttributes() == null) {
            LOGGER.info("No publication attributes, skipping publication of " + getName());
            return null;
        }
        MapServiceDescription sd = new MapServiceDescription(configurationSettings.publicationAttributes());
        sd.setEndpointUrl(prefix() + getAddress());
        
        try {
            String host = getAddress().getHost();
            if (host == null || host.isEmpty()) {
                host = InetAddress.getLocalHost().getCanonicalHostName();
            }
            sd.setHost(host);
            sd.setPort(getAddress().getPort());
        } catch (UnknownHostException e) {
            LOGGER.debug("Failed to get hostname for service publication, will only publish the endpoint URL", e);
        }
        if (requiresKerberos()) {
            sd.setAuthMethods(Collections.singleton(AuthMethod.KERBEROS.value()));
        }
        return sd;
    }
}
