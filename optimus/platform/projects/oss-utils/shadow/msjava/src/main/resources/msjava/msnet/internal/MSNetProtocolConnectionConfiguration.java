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
package msjava.msnet.internal;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.Executor;
import msjava.base.annotation.Experimental;
import msjava.msnet.MSNetEstablisher;
import msjava.msnet.MSNetLoop;
import msjava.msnet.MSNetProtocol;
import msjava.msnet.MSNetProtocolTCPConnection;
import msjava.msnet.MSNetTCPSocketFactory;
import msjava.msnet.utils.MSNetConfiguration;
public interface MSNetProtocolConnectionConfiguration {
    
    MSNetProtocol getProtocol();
    
    void setProtocol(MSNetProtocol protocol);
    
    Executor getStreamCallbackExecutor();
    
    void setStreamCallbackExecutor(Executor streamCallbackExecutor);
    
    MSNetConnectionConfigurationSupport getPlainConnectionConfiguration();
    
    void setPlainConnectionConfiguration(MSNetConnectionConfigurationSupport plainConnectionConfiguration);
    
    String getHostPort();
    
    void setHostPort(String hostPort);
    
    MSNetTCPSocketFactory getSocketFactory();
    
    void setSocketFactory(MSNetTCPSocketFactory socketFactory);
    
    MSNetLoop getLoop();
    
    void setLoop(MSNetLoop netLoop);
    
    boolean isInitialized();
    void init() throws UnknownHostException;
    
    boolean getAsyncReading();
    
    void setAsyncReading(boolean asyncReading);
    
    boolean getRetryFlag();
    
    void setRetryFlag(boolean retryFlag);
    
    long getRetryTime();
    
    void setRetryTime(long retryTime);
    
    long getMaxRetryTime();
    
    void setMaxRetryTime(long maxRetryTime);
    
    int getMaxRetries();
    
    void setMaxRetries(int maxRetries);
    
    int getMaxDisconnects();
    
    void setMaxDisconnects(int maxDisconnects);
    
    boolean getTapping();
    
    void setTapping(boolean tapping);
    
    boolean getKeepAlive();
    
    void setKeepAlive(boolean keepAlive);
    
    boolean getTcpNoDelay();
    
    void setTcpNoDelay(boolean tcpNoDelay);
    
    int getReceiveBufferSize();
    
    void setReceiveBufferSize(int receiveBufferSize);
    
    int getSendBufferSize();
    
    void setSendBufferSize(int sendBufferSize);
    
    List<String> getBackupAddresses();
    
    void setBackupAddresses(List<String> backupAddresses) throws UnknownHostException;
    
    List<MSNetEstablisher> getEstablishers();
    
    void setEstablishers(List<MSNetEstablisher> establishers);
    
    Executor getCallbackExecutor();
    
    void setCallbackExecutor(Executor callbackExecutor);
    
    String getConnectionName();
    
    void setConnectionName(String connectionName);
    
    String getConnectionId();
    void afterPropertiesSet() throws Exception;
    boolean isSkipHostPortValidation();
    void setSkipHostPortValidation(boolean skipHostPortValidation);
    
    String getBindAddress();
    
    void setBindAddress(String bindAddress);
    
    void setWriteListThreshold(long size);
    
    long getWriteListThreshold();
    
    boolean getKerberos();
    
    void setKerberos(boolean kerberos);
    boolean getKerberosEncryption();
    @Experimental
    void setKerberosEncryption(boolean kerberos);
    
    boolean getAnonymousAuthentication();
    
    @Deprecated
    void setAnonymousAuthentication(boolean anonymousAuthentication);
    
    int getStreamListenerQueuesize();
    
    void setStreamListenerQueuesize(int streamListenerQueuesize);
    
    int getStreamBufferSizeLimit();
    
    void setStreamBufferSizeLimit(int streamBufferSizeLimit);
}