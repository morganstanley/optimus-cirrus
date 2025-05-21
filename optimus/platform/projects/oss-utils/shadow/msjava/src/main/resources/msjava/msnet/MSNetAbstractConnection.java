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
import msjava.base.annotation.Internal;
import msjava.base.spring.lifecycle.BeanState;
import msjava.msnet.internal.LockSafeMSNetLoopExecutor;
import msjava.msnet.internal.MSNetConnectingLock;
import msjava.msnet.internal.MSNetConnectionConfigurationSupport;
import msjava.msnet.spring.ApplicationNetLoopAware;
import msjava.msnet.utils.IdleTimeoutScheduler;
import msjava.msnet.utils.MSNetConfiguration;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.InitializingBean;
import com.google.common.annotations.VisibleForTesting;
import java.net.UnknownHostException;
import java.util.Collections;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
public abstract class MSNetAbstractConnection implements MSNetConnection, InitializingBean, ApplicationNetLoopAware,
        BeanNameAware {
    private static final Logger LOGGER = LoggerFactory.getLogger(MSNetAbstractConnection.class);
    
    protected MSNetLoop _loop;
    
    protected MSNetID _id;
    
    private MSNetConnectionStateEnum _state; 
    
    private final Object stateLock = new Object();
    protected Date _connectTime;
    protected boolean _doRetry = true;
    protected boolean _established = false;
    protected int _maxRetries = InfiniteRetries;
    protected int _numRetries = 0;
    protected int _maxDisconnects = DefaultMaxDisconnects;
    protected int _numDisconnects = 0;
    protected long _retryTime = 0; 
    private Long _initialRetryTime; 
    protected long _maxRetryTime = DefaultMaxRetryTime;
    protected IdleTimeoutScheduler timeoutScheduler;
    
    private final CopyOnWriteArrayList<MSNetConnectionListener> listeners = new CopyOnWriteArrayList<MSNetConnectionListener>();
    protected final BeanState state = new BeanState();
    String hostPort;
    
    private Executor callbackExecutor; 
    
    protected final MSNetConnectingLock connectingLock = MSNetConnectingLock.newInstance();
    
    public MSNetAbstractConnection() {
    }
    
    public MSNetAbstractConnection(MSNetLoop loop_, MSNetID id_) {
        _loop = loop_ != null ? loop_ : new MSNetLoop(new MSNetLoopNullImpl());
        _id = id_;
        _initAbstractConnection();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Connection[{}] is initialized with [{}, NULL_IMPL?{}]", new Object[] { _id, _loop,
                    (_loop.getImpl() instanceof MSNetLoopNullImpl) });
        }
    }
    public MSNetAbstractConnection(MSNetConnectionConfigurationSupport configSup) throws UnknownHostException {
        if (configSup == null) {
            throw new IllegalArgumentException("Connection configuration support is not set!");
        }
        if (!configSup.isInitialized()) {
            configSup.init();
        }
        _loop = configSup.getLoop();
        if (_loop == null) {
            _loop = new MSNetLoop(new MSNetLoopNullImpl());
        }
        _id = new MSNetID(configSup.getConnectionId());
        _initAbstractConnection();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Connection[{}] is initialized with [{}]", _id, _loop);
        }
    }
    protected void doInitialize() throws Exception {
        Validate.notNull(_loop, "You must provide a loop or explicitly set it to null!");
        if(_initialRetryTime == null){
            _initialRetryTime = Math.min(_maxRetryTime, MSNetConnection.DefaultInitialRetryTime);
        }
        Validate.isTrue(_initialRetryTime <= _maxRetryTime, "initialRetryTime should not be greater than maxRetryTime");
        MSNetAbstractConnection.this.validateAddresses();
        Validate.notNull(_id, "Connection name must be set! Use setName() or setBeanName()");
        _initAbstractConnection();
    }
    
    @Override
    public final void afterPropertiesSet() throws UnknownHostException {
        state.initializeIfNotInitialized(new Runnable() {
            @Override
            public void run() {
                try {
                    doInitialize();
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }
    void validateAddresses() throws UnknownHostException {
        if (getPrimaryAddress() == null) {
            Validate.notNull(hostPort, "You must either specify the primary address or the hostPort property!");
            setPrimaryAddress(new MSNetInetAddress(hostPort));
        }
    }
    
    public void setListeners(List<MSNetConnectionListener> listeners) {
        this.listeners.clear();
        this.listeners.addAll(listeners);
    }
    
    @VisibleForTesting
    public boolean hasListeners() {
        return !listeners.isEmpty();
    }
    
    @VisibleForTesting
    public int countListeners() {
        return listeners.size();
    }
    @Override
    public void setLoop(MSNetLoop loop_) {
        if (loop_ == null) {
            _loop = new MSNetLoop(new MSNetLoopNullImpl());
        } else {
            this._loop = loop_;
        }
    }
    @Override
    public void setBeanName(String name) {
        if(getName() == null){
            setName(new MSNetID(name));
        } else {
            String connectionNameSetInCtor = getName().toString();
            if (!connectionNameSetInCtor.equals(name)){
                LOGGER.info("Connection name ({}) specified in constructor is kept (not overridden by spring bean name ({}))", connectionNameSetInCtor, name);
            }
        }
    }
    public void setHostPort(String hostPort) {
        this.hostPort = hostPort;
    }
    void _initAbstractConnection() {
        if(callbackExecutor == null) {
            setCallbackExecutor(null); 
        }
        _connectTime = new Date(0);
        setState(MSNetConnectionStateEnum.INVALID);
    }
    
    @Override
    public void addListener(MSNetConnectionListener listener) {
        listeners.add(listener);
    }
    
    @Override
    public void removeListener(MSNetConnectionListener listener) {
        listeners.remove(listener);
    }
    
    protected List<MSNetConnectionListener> getListeners() {
        return Collections.unmodifiableList(listeners);
    }
    
    @Override
    public Date connectTime() {
        return _connectTime;
    }
    
    @Override
    public MSNetConnectionStateEnum getState() {
        synchronized (stateLock) {
            return _state;
        }
    }
    
    protected void waitForConnectedState(long timeOutmillisec) throws MSNetSelectTimeoutException, MSNetSelectException,
            MSNetSelectInterruptException {
        synchronized (stateLock) {
            if (_state == MSNetConnectionStateEnum.CONNECTED){
                
                return;
            }
            if (_state != MSNetConnectionStateEnum.ESTABLISHING && _state != MSNetConnectionStateEnum.CONNECTING) {
                
                throw new MSNetSelectException(_state.toString());
            }
            if (timeOutmillisec <= 0) {
                
                throw new MSNetSelectTimeoutException(timeOutmillisec);
            }
            long waitUntil = System.currentTimeMillis() + timeOutmillisec;
            long waitFor = timeOutmillisec;
            
            do {
                try {
                    stateLock.wait(waitFor);
                } catch (InterruptedException e) {
                    throw new MSNetSelectInterruptException();
                }
                waitFor = waitUntil - System.currentTimeMillis();
            } while (waitFor > 0
                    && (_state == MSNetConnectionStateEnum.ESTABLISHING || _state == MSNetConnectionStateEnum.CONNECTING));
            if (_state == MSNetConnectionStateEnum.ESTABLISHING || _state == MSNetConnectionStateEnum.CONNECTING) {
                
                throw new MSNetSelectTimeoutException(timeOutmillisec);
            }
            if (_state != MSNetConnectionStateEnum.CONNECTED) {
                
                throw new MSNetSelectException(_state.toString());
            }
        }
    }
    
    protected void setState(final MSNetConnectionStateEnum state) {
        setStateWithoutNotification(state);
        notifyStateChanged(state);
    }
    
    @Deprecated
    protected final void setStateWithoutNotification(final MSNetConnectionStateEnum state) {
        synchronized (stateLock) {
            if (this._state == state) {
                return;
            }
            this._state = state;
            
            stateLock.notifyAll();
        }
    }
    
    @Internal
    protected void internalOnStateChanged(MSNetConnectionStateEnum state) {
        
    }
    
    @Deprecated
    final void notifyStateChanged(final MSNetConnectionStateEnum state) {
        internalOnStateChanged(state);
        if (MSNetConfiguration.USE_NEW_CALLBACK_STRATEGY) {
            getCallbackExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    for (MSNetConnectionListener listener : getListeners()) {
                        listener.stateCallback(_id, state);
                    }
                }
            });
        } else {
            for (MSNetConnectionListener listener : getListeners()) {
                listener.stateCallback(_id, state);
            }
        }
    }
    
    @Override
    public boolean getRetryFlag() {
        return _doRetry;
    }
    
    @Override
    public void setRetryFlag(boolean retryFlag) {
        this._doRetry = retryFlag;
    }
    
    @Override
    @Deprecated
    public long getRetryTime() {
        return _retryTime;
    }
    
    @Override
    @Deprecated
    public void setRetryTime(long time) {
        this._retryTime = time;
    }
    
    @Override
    public long getMaxRetryTime() {
        return _maxRetryTime;
    }
    
    @Override
    public void setMaxRetryTime(long time) {
        this._maxRetryTime = time;
    }
    
    @Override
    public int getMaxRetries() {
        return _maxRetries;
    }
    
    @Override
    public void setMaxRetries(int retries) {
        this._maxRetries = retries;
    }
    
    @Override
    public int getMaxDisconnects() {
        return _maxDisconnects;
    }
    
    @Override
    public void setMaxDisconnects(int maxDisconnects) {
        this._maxDisconnects = maxDisconnects;
    }
    
    @Override
    public MSNetID getName() {
        return _id;
    }
    
    @Override
    public void setName(MSNetID id) {
        _id = id;
    }
    
    @Override
    public MSNetLoop getNetLoop() {
        return _loop;
    }
    
    @Override
    public boolean isConnected() {
        return (getState() == MSNetConnectionStateEnum.CONNECTED);
    }
    
    @Override
    public boolean isEstablished() {
        return _established;
    }
    
    IdleTimeoutScheduler getTimeoutScheduler() {
        return timeoutScheduler;
    }
    protected Object getStateLock() {
        return stateLock;
    }
    
    public void setCallbackExecutor(Executor callbackExecutor) {
        if (callbackExecutor == null) {
            this.callbackExecutor = new LockSafeMSNetLoopExecutor(_loop, connectingLock);
        } else {
            LOGGER.info(
                    "Callback executor set for the connection. Please read the documentation of "
                            + "msjava.msnet.MSNetAbstractConnection.setCallbackExecutor(Executor) "
                            + "or http:
                            + "to learn about the dangers of this operation.\n"
                            + "(Executor type: {}, instance: {})", callbackExecutor.getClass(), callbackExecutor);
            this.callbackExecutor = callbackExecutor;
        }
    }
    
    public Executor getCallbackExecutor() {
        if (callbackExecutor == null) {
            LOGGER.warn("Callback executor null, returning substitute. This is not normal, "
                    + "unless something broke in your initialization please double-check your "
                    + "configs or write to msjavahelp.", new Exception("Included for stack trace"));
            return r -> r.run();
        }
        return callbackExecutor;
    }
    
    @Override
    public String toString() {
        return makeString().toString();
    }
    private StringBuilder makeString() {
        StringBuilder builder = new StringBuilder();
        builder.append("[");
        builder.append(getClass().getName());
        builder.append("@");
        builder.append(Integer.toHexString(hashCode()));
        builder.append(" (");
        fillString(builder);
        builder.append(")]");
        return builder;
    }
    
    protected void fillString(StringBuilder builder) {
        builder.append("id=").append(_id);
        builder.append(", ");
        builder.append("loop=").append(_loop);
        builder.append(", ");
        builder.append("state=").append(_state);
    }
    
    public final String getDiagnosticInformation(int lod) {
        StringBuilder builder = makeString();
        if (lod > 0) {
            builder.append("\n");
            fillDiagnosticInformation(builder, lod);
        }
        return builder.toString();
    }
    
    protected void fillDiagnosticInformation(StringBuilder builder, int lod) {
        builder.append("    ").append("connectTime=")
                .append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSXXX").format(_connectTime));
        builder.append("\n");
        builder.append("    ").append("established=").append(_established);
        builder.append("\n");
        builder.append("    ").append("doRetry=").append(_doRetry);
        builder.append("\n");
        builder.append("    ").append("numDisconnects=").append(_numDisconnects);
        builder.append("\n");
        builder.append("    ").append("numRetries=").append(_numRetries);
        if (lod >= 2) {
            builder.append("\n");
            builder.append("    ").append("listeners=").append(getListeners());
        }
    }
    
    public long getInitialRetryTime() {
        if(_initialRetryTime == null){
            return 0;
        }
        return _initialRetryTime;
    }
    
    public void setInitialRetryTime(long _initialRetryTime) {
        Validate.isTrue(_initialRetryTime > 0, "initialRetryTime should be greater than 0");
        this._initialRetryTime = _initialRetryTime;
    }
}