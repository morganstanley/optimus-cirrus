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
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import msjava.msnet.utils.MSNetConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class MSNetTCPAcceptor implements MSNetChannelListener {
    protected MSNetTCPAcceptorStateEnum state;
    protected MSNetLoop loop;
    protected MSNetChannel acceptChannel;
    protected MSNetInetAddress address;
    
    private MSNetTCPSocketFactory socketFactory = MSNetConfiguration.getDefaultMSNetTCPSocketFactory();
    protected MSNetTCPSocket socket;
    protected Date listenStartTime;
    protected List<MSNetTCPAcceptorListener> listeners = new CopyOnWriteArrayList<MSNetTCPAcceptorListener>();
    protected int backlog = MSNetConfiguration.DEFAULT_SERVER_SOCKET_BACKLOG_SIZE;
    private static final Logger LOGGER = LoggerFactory.getLogger(MSNetTCPAcceptor.class);
    
    @Deprecated
    public MSNetTCPAcceptor(MSNetLoop aLoop) {
        this(aLoop, null);
    }
    
    @Deprecated
    public MSNetTCPAcceptor(MSNetLoop aLoop, MSNetInetAddress address) {
        this(aLoop, address, MSNetConfiguration.getDefaultMSNetTCPSocketFactory());
    }
    
    
    public MSNetTCPAcceptor(MSNetLoop aLoop, MSNetInetAddress address, MSNetTCPSocketFactory socketFactory) {
        Objects.requireNonNull(socketFactory, "The socket factory cannot be null");
        loop = aLoop;
        this.address = address;
        this.socketFactory = socketFactory;
        state = MSNetTCPAcceptorStateEnum.INVALID;
    }
    public String getInfoMessage() {
        return getState().toString();
    }
    
    public void addListener(MSNetTCPAcceptorListener listener) {
        listeners.add(listener);
    }
    
    public void removeListener(MSNetTCPAcceptorListener listener) {
        listeners.remove(listener);
    }
    
    
    public MSNetTCPSocketFactory getSocketFactory() {
        return socketFactory;
    }
    
    
    public void setSocketFactory(MSNetTCPSocketFactory socketFactory) {
        Objects.requireNonNull(socketFactory, "The socket factory cannot be null");
        this.socketFactory = socketFactory;
    }
    public MSNetTCPSocket getSocket() {
        return socket;
    }
    public void setBacklog(int backlog_) {
        backlog = backlog_;
    }
    public int getBacklog() {
        return backlog;
    }
    
    protected void acceptNotify(MSNetTCPSocket aSocket) {
        for (MSNetTCPAcceptorListener l : listeners) {
            l.acceptCallback(aSocket);
        }
    }
    
    protected void stateNotify(MSNetTCPAcceptorStateEnum state) {
        for (MSNetTCPAcceptorListener l : listeners) {
            l.stateCallback(state);
        }
    }
    
    public void channelReady(MSNetChannelEvent e) {
        MSNetTCPSocket aSocket = socket.accept();
        if (aSocket == null) {
            setState(MSNetTCPAcceptorStateEnum.ACCEPT_ERROR);
            LOGGER.error("accept() returned an invalid socket.");
        }
        acceptNotify(aSocket);
    }
    
    protected void setupAcceptHandler() {
        acceptChannel = new MSNetChannel("ACCEPT_CHANNEL[" + getAddress() + "]", socket);
        acceptChannel.enable(MSNetChannelOpertaion.ACCEPT);
        acceptChannel.setAcceptListener(this);
        loop.registerChannel(acceptChannel);
    }
    
    public MSNetInetAddress getAddress() {
        return address;
    }
    
    public void setAddress(MSNetInetAddress address) {
        this.address = address;
    }
    
    public boolean listen(MSNetInetAddress address) {
        this.address = address;
        return listen();
    }
    
    public boolean listen(int backlog) {
        setBacklog(backlog);
        return listen();
    }
    
    public boolean listen() {
        try {
            return throwingListen();
        } catch (IOException e) {
            setState(MSNetTCPAcceptorStateEnum.LISTEN_ERROR);
            LOGGER.error("Listen error on '{}': '{}'", address, e);
            return false;
        } catch (MSNetIOException e) {
            setState(MSNetTCPAcceptorStateEnum.LISTEN_ERROR);
            LOGGER.error("Listen error on '{}': '{}'", address, e);
            return false;
        }
    }
    
    public boolean listen(MSNetServerStartupPolicyEnum startupPolicy) {
        if (startupPolicy == MSNetServerStartupPolicyEnum.FAIL_ON_EXCEPTION) {
            try {
                return throwingListen();
            } catch (Exception e) {
                throw new MSNetRuntimeException("Server failed on startup (" + getAddress() + ")", e);
            }
        } else {
            return listen();
        }
    }
    
    boolean throwingListen() throws IOException, MSNetIOException {
        if ((address != null) && (address.isValid() == false)) {
            setState(MSNetTCPAcceptorStateEnum.LISTEN_ERROR);
            LOGGER.error("Invalid address.");
            return false;
        }
        
        if (socket != null) {
            socket.close();
            socket = null;
        }
        
        socket = getSocketFactory().createMSNetTCPSocket(true);
        socket.bind(address, backlog);
        setupAcceptHandler();
        listenStartTime = new Date();
        setState(MSNetTCPAcceptorStateEnum.LISTEN);
        
        
        if (address.getPort() == 0) {
            address = new MSNetInetAddress(address.getHost(), socket.getLocalAddress().getPort());
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Listening on '{}'", address);
        }
        
        return true;
    }
    
    public Date listenStartTime() {
        return listenStartTime;
    }
    public boolean isListening() {
        
        return (state == MSNetTCPAcceptorStateEnum.LISTEN) && (socket != null) && (socket.isValid());
    }
    
    public void close() {
        if (acceptChannel != null) {
            loop.unregisterChannel(acceptChannel);
            acceptChannel = null;
        }
        try {
            if ((socket != null) && socket.isValid()) {
                socket.close();
            }
            socket = null;
        } catch (IOException e) {
            LOGGER.error("Could not close socket: " + e, e);
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Closed acceptor at '{}'", address);
        }
        setState(MSNetTCPAcceptorStateEnum.CLOSED);
    }
    
    public MSNetTCPAcceptorStateEnum getState() {
        return state;
    }
    
    protected void setState(MSNetTCPAcceptorStateEnum state) {
        this.state = state;
        stateNotify(state);
    }
    public void setLoop(MSNetLoop loop) {
        this.loop = loop;
    }
}
