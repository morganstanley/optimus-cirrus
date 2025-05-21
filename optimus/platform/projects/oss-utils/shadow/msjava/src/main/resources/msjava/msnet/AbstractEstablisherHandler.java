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
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeoutException;
import msjava.msnet.auth.MSNetAuthenticator;
import msjava.msnet.auth.MSNetKerberosAuthMechanism;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public abstract class AbstractEstablisherHandler {
    static final Logger LOG = LoggerFactory.getLogger(AbstractEstablisherHandler.class);
    
    protected final MSNetTCPConnection conn;
    
    final ArrayList<MSNetEstablisher> establishers;
    
    int establisherIdx = 0;
    protected MSNetEstablisher currEstablisher;
    
    AbstractEstablisherHandler(MSNetTCPConnection conn, MasterEstablisherMode mode,
            Collection<? extends MSNetEstablisher> est) {
        this(conn, mode, est, conn.isInitFromSocket());
    }
    AbstractEstablisherHandler(MSNetTCPConnection conn, MasterEstablisherMode mode,
            Collection<? extends MSNetEstablisher> est, boolean server) {
        this.conn = conn;
        this.establishers = new ArrayList<MSNetEstablisher>(est);
        
        for (MSNetEstablisher e : establishers) {
            e.setEnabled(true);
        }
        MasterEstablisher masterEstablisher = new MasterEstablisher(establishers, mode);
        masterEstablisher.init(server, conn);
        currEstablisher = masterEstablisher;
        LOG.debug("Abstract establish handler initialized. mode={} establishers={}", mode, establishers);
    }
    boolean nextEstablisher() {
        while (establisherIdx < establishers.size() && !establishers.get(establisherIdx).isEnabled()) {
            ++establisherIdx;
        }
        if (establisherIdx < establishers.size()) {
            currEstablisher = establishers.get(establisherIdx);
            ++establisherIdx;
            return true;
        }
        currEstablisher = null;
        return false;
    }
    
    boolean callEstablishersUntilReturnsContinue(MSNetTCPSocketBuffer readBuffer) throws MSNetEstablishException,
            MSNetProtocolException {
        if (currEstablisher == null) {
            return true;
        }
        MSNetEstablishStatus status;
        assert currEstablisher.isEnabled() : currEstablisher.getClass();
        assert !currEstablisher.getStatus().isComplete() : currEstablisher.getClass();
        for (;;) {
            try {
                
                status = currEstablisher.establishAndCheck(readBuffer);
                assert status == currEstablisher.getStatus();
            } finally {
                
                MSNetTCPSocketBuffer outputBuffer = currEstablisher.getOutputBuffer();
                if (outputBuffer != null && !outputBuffer.isEmpty()) {
                    writeOutputBuffer(outputBuffer);
                }
            }
            if (status.isContinue()) {
                return false;
            }
            assert status == MSNetEstablishStatus.COMPLETE;
            LOG.debug("{} establisher returned: {}.", currEstablisher.getEstablisherName(), status);
            if (!nextEstablisher()) {
                
                return true;
            }
        }
    }
    
    
    boolean onDisconnect() {
        return currEstablisher.onDisconnect();
    }
    abstract void writeOutputBuffer(MSNetTCPSocketBuffer b) throws MSNetEstablishException;
    
    static class SyncEstablishHandler extends AbstractEstablisherHandler {
        SyncEstablishHandler(MSNetTCPConnection conn, MasterEstablisherMode mode,
                Collection<MSNetEstablisher> establishers) {
            super(conn, mode, establishers);
        }
        @Override
        void writeOutputBuffer(MSNetTCPSocketBuffer b) throws MSNetEstablishException {
            MSNetIOStatus ioStatus = conn._syncSendBuffer(b, getTimeoutInMs());
            if (ioStatus.inError()) {
                String err = ioStatus.toString();
                throw new MSNetEstablishException("Failed to write in sync establisher loop: " + err);
            }
        }
        void syncEstablisherLoop() throws MSNetEstablishException {
            assert conn._readBuffer.isEmpty() : "Read buffer size:" + conn._readBuffer.size();
            MSNetTCPSocketBuffer syncReadBuffer = new MSNetTCPSocketBuffer();
            boolean done = false;
            do {
                try {
                    done = callEstablishersUntilReturnsContinue(syncReadBuffer);
                } catch (MSNetProtocolException e) {
                    StringBuilder msg = new StringBuilder("Establisher(" + currEstablisher.getEstablisherName()
                            + ") failed while establishing connection (" + conn.getName()
                            + ") because it received invalid message, " + "please check establisher configuration, "
                            + "configured establishers are " + establishers);
                    
                    throw new MSNetEstablishException(msg.toString(), e);
                }
                if (!done) {
                    
                    MSNetIOStatus ioStatus = conn.syncReadBuffer(syncReadBuffer, getTimeoutInMs(), true);
                    if (ioStatus.inError()) {
                        if (ioStatus.getNumBytesProcessed() < 0){
                            boolean needsReconnect = onDisconnect();
                            if (needsReconnect){
                                throw new MSNetReconnectException("Establisher requested reconnect on disconnect");
                            }
                        }
                        String err = ioStatus.toString();
                        throw new MSNetEstablishException("Failed to read in sync establisher loop: " + err);
                    }
                }
            } while (!done);
            
            if (!syncReadBuffer.isEmpty()) {
                conn._readBuffer.store(syncReadBuffer);
            }
        }
        
        protected int getTimeoutInMs() {
            int syncTimeoutInSec = currEstablisher.getSyncTimeout();
            if (syncTimeoutInSec > 1000) {
                LOG.warn("Timeout returned by {} establisher is greater than 1000 second for peer address {}",
                        currEstablisher, conn.getAddress());
            }
            return syncTimeoutInSec * 1000;
        }
    }
    
    static class AsyncEstablishHandler extends AbstractEstablisherHandler implements MSNetChannelListener,
            MSNetEventListener {
        private final boolean enableAsyncRead;
        private final int eventId;
        AsyncEstablishHandler(MSNetTCPConnection conn, MasterEstablisherMode mode,
                Collection<MSNetEstablisher> establisherIter, boolean enableAsyncRead, int eventId) {
            super(conn, mode, establisherIter);
            this.enableAsyncRead = enableAsyncRead;
            this.eventId = eventId;
        }
        @Override
        public void eventOccurred(MSNetEvent e) {
            
            asyncEstablish();
        }
        @Override
        public void channelReady(MSNetChannelEvent event) {
            
            MSNetIOStatus ioStatus = conn.doRawRead();
            if (ioStatus.inError()) {
                String errMsg = ioStatus.toString();
                LOG.error("Establish failed. Read error in establisherReadHandler {}", errMsg);
                asyncEstablishFailed(errMsg);
                if (ioStatus.getNumBytesProcessed() < 0) {
                    boolean needsReconnect = onDisconnect();
                    if (needsReconnect){
                        
                        conn.clearRetryHandler();
                        LOG.info( currEstablisher.getEstablisherName() + " client fallback async");
                        conn.connect();
                    }
                }
                return;
            }
            asyncEstablish();
        }
        @Override
        void writeOutputBuffer(MSNetTCPSocketBuffer b) {
            conn.sendBuffer(b, true);
        }
        void asyncEstablish() {
            if (!conn.connectingLock.lockAsync(eventId)){
                LOG.info("Event id invalid, skipping async establish event");
                return;
            }
            assert conn.connectingLock.assertsLockedByCurrentThread();
            boolean done = false;
            try {
                done = callEstablishersUntilReturnsContinue(conn._readBuffer);
                if (done) {
                    conn.finishEstablish(false, enableAsyncRead);
                    return;
                }
                
                
            } catch (MSNetEstablishException e) {
                StringBuilder msg = new StringBuilder("Establisher(" + (currEstablisher == null ? "null" : currEstablisher.getEstablisherName())
                        + ") failed while establishing connection (" + conn.getName()
                        + "), please check establisher configuration, " + "configured establishers are " + establishers);
                MSNetTCPSocket socket = conn._socket;
                if (socket != null) {
                    msg.append(" (localAddress=" + socket.getLocalAddress() + " remoteAddress=" + socket.getAddress()
                            + ")");
                }
                LOG.error(msg.toString(), e);
                asyncEstablishFailed(e.toString());
            } catch (MSNetProtocolException e) {
                StringBuilder msg = new StringBuilder("Establisher(" + (currEstablisher == null ? "null" : currEstablisher.getEstablisherName())
                        + ") failed while establishing connection (" + conn.getName()
                        + ") because it received invalid message, " + "please check establisher configuration, "
                        + "configured establishers are " + establishers);
                MSNetTCPSocket socket = conn._socket;
                if (socket != null) {
                    msg.append(" (localAddress=" + socket.getLocalAddress() + " remoteAddress=" + socket.getAddress()
                            + ")");
                }
                if (e.isFatal()) {
                    LOG.error(msg.toString(), e);
                } else {
                    msg.append(": " + e.getMessage());
                    LOG.info(msg.toString());
                }
                asyncEstablishFailed(e.toString());
            } finally {
                if (done){
                    
                    conn.connectingLock.unlockAsync();
                } else {
                    
                    conn.connectingLock.unlockAndReschedule();
                }
                assert conn.connectingLock.notHeldByCurrentThread();
            }
        }
        void asyncEstablishFailed(String msg) {
            MSNetChannel c = conn._channel;
            if (null != c) {
                c.setReadListener(null);
            }
            
            conn.resetConnection();
            conn.establishFailedNotify(msg);
        }
    }
    
    static class SocketEstablishHandler extends AbstractEstablisherHandler {
        private final Socket socket;
        private final long timeoutMs;
        SocketEstablishHandler(MSNetTCPConnection conn, MasterEstablisherMode mode,
                Collection<? extends MSNetEstablisher> establisherIter, Socket socket, boolean server, long timeoutMs) {
            super(conn, mode, establisherIter);
            this.socket = socket;
            this.timeoutMs = timeoutMs;
        }
        @Override
        void writeOutputBuffer(MSNetTCPSocketBuffer b) throws MSNetEstablishException {
            try {
                socket.getOutputStream().write(b.retrieve());
            } catch (SocketTimeoutException e) {
                throw new MSNetEstablishException("Timeout of " + timeoutMs + " ms expired when establishing on "
                        + socket.getRemoteSocketAddress(), e);
            } catch (IOException e) {
                throw new MSNetEstablishException("Failed to write to socket");
            }
        }
        void establish() throws MSNetEstablishException {
            long finishBy = System.currentTimeMillis() + timeoutMs;
            int remaining = (int) timeoutMs;
            int originalTimeout = 0;
            try {
                originalTimeout = socket.getSoTimeout();
            } catch (SocketException e) {
                LOG.warn("Failed to get original SO_TIMEOUT on socket, using default", e);
            }
            try {
                MSNetTCPSocketBuffer syncReadBuffer = new MSNetTCPSocketBuffer();
                boolean done = false;
                do {
                    if ((remaining = (int) (finishBy - System.currentTimeMillis())) <= 0) {
                        throw new MSNetEstablishException(new TimeoutException("Timeout of " + timeoutMs
                                + " ms expired when establishing on " + socket.getRemoteSocketAddress()));
                    }
                    try {
                        socket.setSoTimeout(remaining);
                    } catch (Exception e) {
                        LOG.warn("Failed to set SO_TIMEOUT on socket, using default", e);
                    }
                    try {
                        done = callEstablishersUntilReturnsContinue(syncReadBuffer);
                    } catch (MSNetProtocolException e) {
                        StringBuilder msg = new StringBuilder("Establisher(" + currEstablisher.getEstablisherName()
                                + ") failed while establishing socket (" + socket
                                + ") because it received invalid message, " + "please check establisher configuration, "
                                + "configured establishers are " + establishers);
                        
                        throw new MSNetEstablishException(msg.toString(), e);
                    }
                    if (!done) {
                        
                        if ((remaining = (int) (finishBy - System.currentTimeMillis())) <= 0) {
                            throw new MSNetEstablishException(new TimeoutException("Timeout of " + timeoutMs
                                    + " ms expired when establishing on " + socket.getRemoteSocketAddress()));
                        }
                        try {
                            socket.setSoTimeout(remaining);
                        } catch (Exception e) {
                            LOG.warn("Failed to set SO_TIMEOUT on socket, using default", e);
                        }
                        try {
                            
                            int read = socket.getInputStream().read();
                            if (read == -1) {
                                throw new MSNetEstablishException("Failed to read, socket closed");
                            }
                            syncReadBuffer.store((byte) (read & 0xff));
                        } catch (SocketTimeoutException e) {
                            throw new MSNetEstablishException("Timeout of " + timeoutMs
                                    + " ms expired when establishing on " + socket.getRemoteSocketAddress(), e);
                        } catch (IOException e) {
                            throw new MSNetEstablishException("Failed to read from socket", e);
                        }
                    }
                } while (!done);
            } finally {
                try {
                    socket.setSoTimeout(originalTimeout);
                } catch (Exception e) {
                    LOG.warn("Failed to reset SO_TIMEOUT on socket, using default", e);
                }
            }
        }
    }
    static String authOnSocket(msjava.msnet.auth.MSNetAuthMechanism authMechanism, Socket socket, boolean server, long timeoutMs)
            throws MSNetEstablishException {
        
        MSNetTCPConnection c = new MSNetTCPConnection();
        MSNetAuthenticator auth = new MSNetAuthenticator(authMechanism);
        auth.init(server, c);
        
        new SocketEstablishHandler(c, MasterEstablisherMode.OFF, Collections.singletonList(auth), socket, server, timeoutMs)
                .establish();
        
        return c.getAuthContext().getAuthID();
    }
    
    public static String kerberosAuth(final Socket socket, boolean server, long timeoutMs) throws MSNetEstablishException {
        return authOnSocket(new MSNetKerberosAuthMechanism(), socket, server, timeoutMs);
    }
    
    public static String kerberosAuth(final Socket socket, boolean server) throws MSNetEstablishException {
        return kerberosAuth(socket, server, 20000);
    }
}