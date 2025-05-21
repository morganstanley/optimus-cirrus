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
import java.nio.channels.SelectableChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;
import msjava.base.annotation.Internal;
import msjava.msnet.utils.MSNetConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class MSNetTCPSocket {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(MSNetTCPSocket.class);
    final MSNetTCPSocketImpl impl;
    int backlog =  MSNetConfiguration.DEFAULT_SERVER_SOCKET_BACKLOG_SIZE;
    private final MSNetTCPSocketFactory parentFactory;
    
    @Deprecated
    public MSNetTCPSocket() throws MSNetIOException {
        this(true);
    }
    
    @Deprecated
    public MSNetTCPSocket(boolean clientSocket) throws MSNetIOException {
        try {
            if (clientSocket) {
                impl = new MSNetTCPClientSocket(SocketChannel.open());
            } else {
                impl = new MSNetTCPServerSocket(ServerSocketChannel.open(), MSNetConfiguration.getDefaultMSNetTCPSocketFactory());
            }
        } catch (IOException x) {
            throw new MSNetIOException(x);
        }
        
        parentFactory = null;
    }
    
    @Deprecated
    MSNetTCPSocket(SocketChannel socketChannel) {
        this(new MSNetTCPClientSocket(socketChannel));
    }
    
    @Deprecated
    MSNetTCPSocket(ServerSocketChannel serverSocketChannel) {
        this(new MSNetTCPServerSocket(serverSocketChannel, MSNetConfiguration.getDefaultMSNetTCPSocketFactory()));
    }
    
    @Deprecated
    final void initClientSocket() {
        setBlocking(false);
        setKeepAlive(true);
        setSendBufferSize(-1);
        setReceiveBufferSize(-1);
    }
    
    @Deprecated
    final void initServerSocket() {
        setBlocking(false);
    }
    
    @Deprecated
    MSNetTCPSocket(MSNetTCPSocketImpl impl) {
        this(impl, null);
    }
    
    @Internal
    MSNetTCPSocket(MSNetTCPSocketImpl impl, MSNetTCPSocketFactory parentFactory) {
        assert impl != null;
        this.impl = impl;
        this.parentFactory = parentFactory;
    }
    MSNetTCPSocketImpl getImpl() {
        return impl;
    }
    
    public MSNetTCPSocketFactory getParentFactory() {
        return parentFactory;
    }
    
    public MSNetInetAddress getAddress() {
        return impl.getAddress();
    }
    
    public MSNetInetAddress getLocalAddress() {
        return impl.getLocalAddress();
    }
    
    public boolean hasError() {
        return impl.hasError();
    }
    
    public boolean isConnected() {
        return impl.isConnected();
    }
    
    public boolean isValid() {
        return impl.isValid();
    }
    public SelectableChannel getSelectableChannel() {
        return impl.getSelectableChannel();
    }
    
    public void bind(MSNetInetAddress address) throws IOException {
        bind(address, backlog);
    }
    
    public void bind(MSNetInetAddress address, int backlog) throws IOException {
        if (backlog < 1) {
            backlog = this.backlog;
        }
        impl.bind(address, backlog);
    }
    
    public MSNetConnectStatus connect(MSNetInetAddress address) throws IOException {
        return impl.connect(address);
    }
    public boolean finishConnect() throws MSNetIOException {
        return impl.finishConnect();
    }
    
    public void close() throws IOException {
        impl.close();
    }
    
    public void setBacklog(int backlog) {
        this.backlog = backlog;
    }
    
    public int getBacklog() {
        return backlog;
    }
    
    public int getReceiveBufferSize() {
        return impl.getReceiveBufferSize();
    }
    public void setReceiveBufferSize(int size) {
        if (-1 != size) {
            impl.setReceiveBufferSize(size);
        }
    }
    
    public int getSendBufferSize() {
        return impl.getSendBufferSize();
    }
    
    public void setSendBufferSize(int size) {
        if (-1 != size) {
            impl.setSendBufferSize(size);
        }
    }
    
    public void setTcpNoDelay(boolean tcpNoDelay) {
        impl.setTcpNoDelay(tcpNoDelay);
    }
    
    public boolean getTcpNoDelay() {
        return impl.getTcpNoDelay();
    }
    
    public void setReuseAddress(boolean on) {
       impl.setReuseAddress(on);
    }
    
    public boolean getReuseAddress() {
        return impl.getReuseAddress();
    }
    
    public void setSoLinger(boolean on, int linger) {
        if (on) {
             impl.setSoLinger(on, linger);
        }
    }
    
    public int getSoLinger() {
        return impl.getSoLinger();
    }
    
    public void setKeepAlive(boolean keepAlive) {
        impl.setKeepAlive(keepAlive);
    }
    
    public boolean getKeepAlive() {
        return impl.getKeepAlive();
    }
    
    public void setBlocking(boolean blocking) {
        impl.setBlocking(blocking);
    }
    
    public boolean getBlocking() {
        return impl.getBlocking();
    }
    
    public MSNetTCPSocket accept() {
        return impl.accept();
    }
    
    public MSNetIOStatus read(MSNetTCPSocketBuffer buf) {
        try {
            return impl.read(buf);
        } catch (OutOfMemoryError e) {
            logErrorAndCallGc(e);
            return impl.read(buf);
        }
    }
    public MSNetIOStatus write(MSNetTCPSocketBuffer buf) {
        try {
            return impl.write(buf);
        } catch (OutOfMemoryError e) {
            logErrorAndCallGc(e);
            return impl.write(buf);
        }
    }
    public MSNetIOStatus write(List<MSNetTCPSocketBuffer> bufs) {
        try {
            return impl.write(bufs);
        } catch (OutOfMemoryError e) {
            logErrorAndCallGc(e);
            return impl.write(bufs);
        }
    }
    public String toString() {
        return impl.toString();
    }
    private void logErrorAndCallGc(OutOfMemoryError e) {
        LOGGER.warn("OutOfMemoryError reading, calling System.gc() and trying again.");
        try {
            LOGGER.warn(stackTraceAsString(e));
        } catch (OutOfMemoryError ee) {
            
        }
        System.gc();
    }
    private static String stackTraceAsString(Throwable t) {
        if (t == null)
            return "";
        java.io.StringWriter writer = new java.io.StringWriter();
        t.printStackTrace(new java.io.PrintWriter(writer));
        return writer.toString().trim();
    }
    
    public boolean isCloseFinished() {
        return impl.isCloseFinished();
    }
}
