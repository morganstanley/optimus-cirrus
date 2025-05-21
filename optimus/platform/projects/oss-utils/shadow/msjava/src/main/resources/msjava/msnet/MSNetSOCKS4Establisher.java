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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import msjava.msnet.utils.MSNetConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
class MSNetSOCKS4Establisher extends MSNetEstablisher {
    private static final Logger LOGGER = LoggerFactory.getLogger(MSNetSOCKS4Establisher.class);
    
    private static final int DEFAULT_ESTABLISH_PRIORITY = 1000;
    private static final int DEFAULT_SYNC_TIMEOUT = 5; 
    private static final byte
        SOCKS4_COMMAND_VERSION = 4,
        SOCKS4_COMMAND_CONNECT = 1,
        SOCKS4_COMMAND_BIND    = 2; 
    private static final byte
        SOCKS4_REPLY_VERSION = 0,
        SOCKS4_REPLY_SUCCESS                          = 90,
        SOCKS4_REPLY_FAILURE_GENERIC                  = 91,
        SOCKS4_REPLY_FAILURE_IDENTD_CONNECTION_FAILED = 92,
        SOCKS4_REPLY_FAILURE_IDENTD_IDENTITY_MISMATCH = 93;
    enum State {
        STALE,
        FAILURE,
        CONNECT_REQUEST_SENT,
        COMPLETE
    }
    MSNetTCPConnection conn;
    String userName;
    State state = State.STALE;
    MSNetTCPSocketBuffer writeBuffer;
    public MSNetSOCKS4Establisher() {
        super(DEFAULT_ESTABLISH_PRIORITY, DEFAULT_SYNC_TIMEOUT);
        writeBuffer = new MSNetTCPSocketBuffer();
    }
    public String getEstablisherName() {
        return "SOCKS4";
    }
    
    public void setUserName(String userName) {
        this.userName = userName;
    }
    public String getUserName() {
        if (userName == null) {
            return MSNetConfiguration.USER_NAME;
        }
        return userName;
    }
    public MSNetTCPSocketBuffer getOutputBuffer() {
        return writeBuffer;
    }
    public void init(boolean isServerSide, MSNetTCPConnection conn) {
        if (isServerSide) {
            throw new IllegalStateException("This establisher can only be used on the clientside");
        }
        this.conn = conn;
        
        cleanup();
    }
    public MSNetEstablishStatus establish(MSNetTCPSocketBuffer inputBuffer) {
        
        if (state == State.STALE) {
            final MSNetInetAddress target = (MSNetInetAddress) conn.getAddress();
            
            byte[] userID = getUserName().getBytes(StandardCharsets.US_ASCII);
            ByteBuffer packet = ByteBuffer.allocate(8 + userID.length + 1);
            packet.put(SOCKS4_COMMAND_VERSION);
            packet.put(SOCKS4_COMMAND_CONNECT);
            packet.putShort((short) target.getPort());
            packet.put(target.getInetAddress().getAddress());
            packet.put(userID);
            packet.put((byte) '\0');
            packet.flip();
            writeBuffer.store(packet);
            
            state = State.CONNECT_REQUEST_SENT;
            return getStatus();
        }
        
        if (state == State.CONNECT_REQUEST_SENT) {
            
            final int responseSize = 8;
            
            if (inputBuffer.size() < responseSize) {
                return getStatus();
            }
            
            byte[] response = inputBuffer.retrieve(responseSize);
            final int version = response[0];
            final int reply = response[1];
            if (version == SOCKS4_REPLY_VERSION && reply == SOCKS4_REPLY_SUCCESS) {
                LOGGER.info("Proxy successfully connected to remote party");
                state = State.COMPLETE;
            } else {
                
                if (version != SOCKS4_REPLY_VERSION) {
                    LOGGER.error("Protocol error! Unexpected response version: " + version);
                }
                switch (reply) {
                    case SOCKS4_REPLY_SUCCESS:
                        break;
                    case SOCKS4_REPLY_FAILURE_GENERIC:
                        LOGGER.error("Proxy rejected request or is unable to complete connection");
                        break;
                    case SOCKS4_REPLY_FAILURE_IDENTD_CONNECTION_FAILED:
                        LOGGER.error("Proxy failed to connect to identd, thus rejected the connection");
                        break;
                    case SOCKS4_REPLY_FAILURE_IDENTD_IDENTITY_MISMATCH:
                        LOGGER.error("Proxy reported identd mismatch, and is refusing to complete the connection");
                        break;
                    default:
                        LOGGER.error("Protocol error! Unexpected response code: " + reply);
                        break;
                }
                state = State.FAILURE;
            }
        }
        return getStatus();
    }
    public MSNetEstablishStatus getStatus() {
        switch(state) {
            case STALE:
                return MSNetEstablishStatus.UNKNOWN;
            case CONNECT_REQUEST_SENT:
                return MSNetEstablishStatus.CONTINUE;
            case FAILURE:
                return MSNetEstablishStatus.FAILURE;
            case COMPLETE:
                return MSNetEstablishStatus.COMPLETE;
        }
        throw new IllegalStateException();
    }
    public void cleanup() {
        state = State.STALE;
        writeBuffer.clear();
    }
}
