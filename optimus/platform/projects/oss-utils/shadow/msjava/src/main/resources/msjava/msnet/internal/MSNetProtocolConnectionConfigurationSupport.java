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
import msjava.msnet.MSNetStringProtocol;
import msjava.msnet.MSNetTCPSocketFactory;
import msjava.msnet.spring.ApplicationNetLoopAware;
import msjava.msnet.utils.MSNetConfiguration;
import org.apache.commons.lang3.Validate;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.InitializingBean;
public class MSNetProtocolConnectionConfigurationSupport implements BeanNameAware, InitializingBean,
        ApplicationNetLoopAware, MSNetProtocolConnectionConfiguration, Cloneable {
    private MSNetProtocol protocol = new MSNetStringProtocol();
    private Executor streamCallbackExecutor;
    private int streamListenerQueuesize;
    
    private int streamBufferSizeLimit = MSNetConfiguration.getDefaultStreamBufferSizeLimit();
    private MSNetConnectionConfigurationSupport plainConnectionConfiguration = new MSNetConnectionConfigurationSupport();
    private boolean initialized;
    
    @Override
    public MSNetProtocol getProtocol() {
        return protocol;
    }
    
    @Override
    public void setProtocol(MSNetProtocol protocol) {
        this.protocol = protocol;
    }
    
    @Override
    public Executor getStreamCallbackExecutor() {
        return streamCallbackExecutor;
    }
    
    @Override
    public void setStreamCallbackExecutor(Executor streamCallbackExecutor) {
        this.streamCallbackExecutor = streamCallbackExecutor;
    }
    
    @Override
    public MSNetConnectionConfigurationSupport getPlainConnectionConfiguration() {
        return plainConnectionConfiguration;
    }
    
    @Override
    public void setPlainConnectionConfiguration(MSNetConnectionConfigurationSupport plainConnectionConfiguration) {
        this.plainConnectionConfiguration = plainConnectionConfiguration;
    }
    
    @Override
    public String getHostPort() {
        return plainConnectionConfiguration.getHostPort();
    }
    
    @Override
    public void setHostPort(String hostPort) {
        plainConnectionConfiguration.setHostPort(hostPort);
    }
    
    @Override
    public MSNetTCPSocketFactory getSocketFactory() {
        return plainConnectionConfiguration.getSocketFactory();
    }
    
    @Override
    public void setSocketFactory(MSNetTCPSocketFactory socketFactory) {
        plainConnectionConfiguration.setSocketFactory(socketFactory);
    }
    
    @Override
    public MSNetLoop getLoop() {
        return plainConnectionConfiguration.getLoop();
    }
    
    @Override
    public void setLoop(MSNetLoop netLoop) {
        plainConnectionConfiguration.setLoop(netLoop);
    }
    
    @Override
    public boolean isInitialized() {
        return initialized;
    }
    @Override
    public void init() throws UnknownHostException {
        if (plainConnectionConfiguration == null) {
            throw new IllegalArgumentException("MSNetConnectionConfigurationSupport must be set!");
        }
        if (!plainConnectionConfiguration.isInitialized()) {
            plainConnectionConfiguration.init();
        }
        if (protocol == null) {
            throw new IllegalArgumentException("MSNetProtocol must be set for protocol TCP connections!");
        }
        initialized = true;
    }
    
    @Override
    public boolean getAsyncReading() {
        return plainConnectionConfiguration.getAsyncReading();
    }
    
    @Override
    public void setAsyncReading(boolean asyncReading) {
        plainConnectionConfiguration.setAsyncReading(asyncReading);
    }
    
    @Override
    public boolean getRetryFlag() {
        return plainConnectionConfiguration.getRetryFlag();
    }
    
    @Override
    public void setRetryFlag(boolean retryFlag) {
        plainConnectionConfiguration.setRetryFlag(retryFlag);
    }
    
    @Override
    @Deprecated
    public long getRetryTime() {
        return plainConnectionConfiguration.getRetryTime();
    }
    
    @Override
    @Deprecated
    public void setRetryTime(long retryTime) {
        plainConnectionConfiguration.setRetryTime(retryTime);
    }
    
    @Override
    public long getMaxRetryTime() {
        return plainConnectionConfiguration.getMaxRetryTime();
    }
    
    @Override
    public void setMaxRetryTime(long maxRetryTime) {
        plainConnectionConfiguration.setMaxRetryTime(maxRetryTime);
    }
    
    @Override
    public int getMaxRetries() {
        return plainConnectionConfiguration.getMaxRetries();
    }
    
    @Override
    public void setMaxRetries(int maxRetries) {
        plainConnectionConfiguration.setMaxRetries(maxRetries);
    }
    
    @Override
    public int getMaxDisconnects() {
        return plainConnectionConfiguration.getMaxDisconnects();
    }
    
    @Override
    public void setMaxDisconnects(int maxDisconnects) {
        plainConnectionConfiguration.setMaxDisconnects(maxDisconnects);
    }
    
    @Override
    public boolean getTapping() {
        return plainConnectionConfiguration.getTapping();
    }
    
    @Override
    public void setTapping(boolean tapping) {
        plainConnectionConfiguration.setTapping(tapping);
    }
    
    @Override
    public boolean getKeepAlive() {
        return plainConnectionConfiguration.getKeepAlive();
    }
    
    @Override
    public void setKeepAlive(boolean keepAlive) {
        plainConnectionConfiguration.setKeepAlive(keepAlive);
    }
    
    @Override
    public boolean getTcpNoDelay() {
        return plainConnectionConfiguration.getTcpNoDelay();
    }
    
    @Override
    public void setTcpNoDelay(boolean tcpNoDelay) {
        plainConnectionConfiguration.setTcpNoDelay(tcpNoDelay);
    }
    
    @Override
    public int getReceiveBufferSize() {
        return plainConnectionConfiguration.getReceiveBufferSize();
    }
    
    @Override
    public void setReceiveBufferSize(int receiveBufferSize) {
        plainConnectionConfiguration.setReceiveBufferSize(receiveBufferSize);
    }
    
    @Override
    public int getSendBufferSize() {
        return plainConnectionConfiguration.getSendBufferSize();
    }
    
    @Override
    public void setSendBufferSize(int sendBufferSize) {
        plainConnectionConfiguration.setSendBufferSize(sendBufferSize);
    }
    
    @Override
    public List<String> getBackupAddresses() {
        return plainConnectionConfiguration.getBackupAddresses();
    }
    
    @Override
    public void setBackupAddresses(List<String> backupAddresses) throws UnknownHostException {
        plainConnectionConfiguration.setBackupAddresses(backupAddresses);
    }
    
    @Override
    public List<MSNetEstablisher> getEstablishers() {
        return plainConnectionConfiguration.getEstablishers();
    }
    
    @Override
    public void setEstablishers(List<MSNetEstablisher> establishers) {
        plainConnectionConfiguration.setEstablishers(establishers);
    }
    
    @Override
    public Executor getCallbackExecutor() {
        return plainConnectionConfiguration.getCallbackExecutor();
    }
    
    @Override
    public void setCallbackExecutor(Executor callbackExecutor) {
        plainConnectionConfiguration.setCallbackExecutor(callbackExecutor);
    }
    
    public String getBeanName() {
        return plainConnectionConfiguration.getBeanName();
    }
    
    @Override
    public void setBeanName(String name) {
        plainConnectionConfiguration.setBeanName(name);
    }
    @Override
    public String getConnectionName() {
        return plainConnectionConfiguration.getConnectionName();
    }
    @Override
    public void setConnectionName(String connectionName) {
        plainConnectionConfiguration.setConnectionName(connectionName);
    }
    @Override
    public String getConnectionId() {
        return plainConnectionConfiguration.getConnectionId();
    }
    @Override
    public void afterPropertiesSet() throws Exception {
        init();
    }
    @Override
    public boolean isSkipHostPortValidation() {
        return plainConnectionConfiguration.isSkipHostPortValidation();
    }
    @Override
    public void setSkipHostPortValidation(boolean skipHostPortValidation) {
        plainConnectionConfiguration.setSkipHostPortValidation(skipHostPortValidation);
    }
    
    @Override
    public String getBindAddress() {
        return plainConnectionConfiguration.getBindAddress();
    }
    
    @Override
    public void setBindAddress(String bindAddress) {
        plainConnectionConfiguration.setBindAddress(bindAddress);
    }
    
    @Override
    public void setWriteListThreshold(long size) {
        plainConnectionConfiguration.setWriteListThreshold(size);
    }
    
    @Override
    public long getWriteListThreshold() {
        return plainConnectionConfiguration.getWriteListThreshold();
    }
    
    @Override
    public boolean getKerberos() {
        return plainConnectionConfiguration.getKerberos();
    }
    
    @Override
    public void setKerberos(boolean kerberos) {
        plainConnectionConfiguration.setKerberos(kerberos);
    }
    
    @Override
    public boolean getKerberosEncryption() {
        return plainConnectionConfiguration.getKerberosEncryption();
    }
    
    @Experimental
    @Override
    public void setKerberosEncryption(boolean kerberos) {
        plainConnectionConfiguration.setKerberosEncryption(kerberos);
    }
    
    @Override
    public boolean getAnonymousAuthentication() {
        return plainConnectionConfiguration.getAnonymousAuthentication();
    }
    
    @Override
    @Deprecated
    public void setAnonymousAuthentication(boolean anonymousAuthentication) {
        plainConnectionConfiguration.setAnonymousAuthentication(anonymousAuthentication);
    }
    
    @Override
    public int getStreamListenerQueuesize() {
        return streamListenerQueuesize;
    }
    
    @Override
    public void setStreamListenerQueuesize(int streamListenerQueuesize) {
        this.streamListenerQueuesize = streamListenerQueuesize;
    }
    
    
    @Override
    public int getStreamBufferSizeLimit() {
        return streamBufferSizeLimit;
    }
    
    @Override
    public void setStreamBufferSizeLimit(int streamBufferSizeLimit) {
        Validate.isTrue(streamBufferSizeLimit != 0, "0 buffer size limit is not allowed");
        this.streamBufferSizeLimit = streamBufferSizeLimit;
    }
    
    
    @Override
    public MSNetProtocolConnectionConfigurationSupport clone() {
        MSNetProtocolConnectionConfigurationSupport clone = new MSNetProtocolConnectionConfigurationSupport();
        
        clone.setAnonymousAuthentication(getAnonymousAuthentication());
        clone.setAsyncReading(getAsyncReading());
        clone.setBeanName(getBeanName());
        clone.setConnectionName(getConnectionName());
        clone.setCallbackExecutor(getCallbackExecutor());
        clone.setEstablishers(getEstablishers());
        clone.setKeepAlive(getKeepAlive());
        clone.setLoop(getLoop());
        clone.setMaxDisconnects(getMaxDisconnects());
        clone.setMaxRetries(getMaxRetries());
        clone.setMaxRetryTime(getMaxRetryTime());
        clone.setProtocol(getProtocol());
        clone.setReceiveBufferSize(getReceiveBufferSize());
        clone.setRetryFlag(getRetryFlag());
        clone.setRetryTime(getRetryTime());
        clone.setSendBufferSize(getSendBufferSize());
        clone.setStreamCallbackExecutor(getStreamCallbackExecutor());
        clone.setStreamListenerQueuesize(getStreamListenerQueuesize());
        clone.setTapping(getTapping());
        clone.setTcpNoDelay(getTcpNoDelay());
        clone.setWriteListThreshold(getWriteListThreshold());
        clone.setStreamBufferSizeLimit(getStreamBufferSizeLimit());
        clone.setPlainConnectionConfiguration(getPlainConnectionConfiguration().clone());
        
        return clone;
    }
}
