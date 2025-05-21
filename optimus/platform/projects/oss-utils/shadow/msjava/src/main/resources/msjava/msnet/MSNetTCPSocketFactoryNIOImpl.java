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
import javax.annotation.Nullable;
import com.google.common.base.Preconditions;
public class MSNetTCPSocketFactoryNIOImpl implements MSNetTCPSocketFactory {
    private static class Holder {
        private static final MSNetTCPSocketFactoryNIOImpl INSTANCE = new MSNetTCPSocketFactoryNIOImpl();
    }
    
    public static MSNetTCPSocketFactoryNIOImpl defaultInstance() {
        return Holder.INSTANCE;
    }
    @Override
    public MSNetTCPSocketImpl createMSNetTCPSocketImpl(boolean isServer) throws MSNetIOException {
        try {
            MSNetTCPSocketImpl socketImpl;
            if (isServer) {
                ServerSocketChannel serverSocketChannel = createNIOServerSocketChannel();
                initServerSocketChannel(serverSocketChannel);
                socketImpl = createMSNetTCPServerSocketImpl(serverSocketChannel);
            } else {
                SocketChannel socketChannel = createNIOClientSocketChannel();
                initSocketChannel(socketChannel, false);
                socketImpl = createMSNetTCPClientSocketImpl(socketChannel, false);
            }
            initSocketImpl(socketImpl, isServer, false);
            return socketImpl;
        } catch (IOException e) {
            throw new MSNetIOException(
                    "Failed to create " + (isServer ? "server" : "client") + " socket, see cause for details", e);
        }
    }
    
    protected MSNetTCPSocketImpl createMSNetTCPServerSocketImpl(ServerSocketChannel serverSocketChannel)
            throws MSNetIOException {
        return new MSNetTCPServerSocket(serverSocketChannel, this);
    }
    
    protected MSNetTCPSocketImpl createMSNetTCPClientSocketImpl(SocketChannel socketChannel, boolean isAcceptedClient)
            throws MSNetIOException {
        return new MSNetTCPClientSocket(socketChannel);
    }
    
    protected void initSocketImpl(MSNetTCPSocketImpl socketImpl, boolean isServer, boolean isAcceptedClient)
            throws MSNetIOException {
        
    }
    
    protected ServerSocketChannel createNIOServerSocketChannel() throws IOException {
        return ServerSocketChannel.open();
    }
    
    protected SocketChannel createNIOClientSocketChannel() throws IOException {
        return SocketChannel.open();
    }
    
    @Nullable
    protected SocketChannel acceptNIOClientSocketChannel(ServerSocketChannel serverSocketChannel) throws IOException {
        return serverSocketChannel.accept();
    }
    
    protected void initServerSocketChannel(ServerSocketChannel serverSocketChannel) throws IOException {
        
    }
    
    protected void initSocketChannel(SocketChannel socketChannel, boolean isAcceptedChannel) throws IOException {
        
    }
    @Override
    public MSNetTCPSocketImpl acceptMSNetTCPSocketImpl(MSNetTCPSocketImpl serverSocket) throws MSNetIOException {
        SelectableChannel sc = serverSocket.getSelectableChannel();
        Preconditions.checkArgument(sc instanceof ServerSocketChannel,
                "The SelectableChannel property of the supplied MSNet socket is not a ServerSocketChannel"
                        + " -- make sure you supplied a server socket");
        try {
            SocketChannel acceptedSocket = acceptNIOClientSocketChannel((ServerSocketChannel) sc);
            initSocketChannel(acceptedSocket, true);
            MSNetTCPSocketImpl socketImpl = createMSNetTCPClientSocketImpl(acceptedSocket, true);
            initSocketImpl(socketImpl, false, true);
            return socketImpl;
        } catch (IOException e) {
            throw new MSNetIOException("Failed to create accepted socket, see cause for details", e);
        }
    }
}
