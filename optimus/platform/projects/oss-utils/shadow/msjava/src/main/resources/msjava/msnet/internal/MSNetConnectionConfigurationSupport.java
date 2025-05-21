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
import com.ms.infra.net.establishers.auth.mechanism.kerberos.ChannelBindingMode;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import msjava.base.annotation.Experimental;
import msjava.base.slf4j.ContextLogger;
import msjava.base.util.internal.SystemPropertyUtils;
import msjava.msnet.MSNetAbstractConnection;
import msjava.msnet.MSNetConnection;
import msjava.msnet.MSNetEstablisher;
import msjava.msnet.MSNetInetAddress;
import msjava.msnet.MSNetLoop;
import msjava.msnet.MSNetTCPConnection;
import msjava.msnet.MSNetTCPSocketFactory;
import msjava.msnet.spring.ApplicationNetLoopAware;
import msjava.msnet.utils.MSNetConfiguration;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.InitializingBean;
public class MSNetConnectionConfigurationSupport implements BeanNameAware, InitializingBean, ApplicationNetLoopAware, Cloneable {
	private static final Logger LOGGER = ContextLogger.safeLogger();
	
	
	
	private static java.io.OutputStream _tapFile;
	static {
		String path = SystemPropertyUtils.getProperty("msjava.msnet.tap_file", LOGGER);
		if (path == null) {
			
		} else if ("-".equals(path)) {
			_tapFile = System.err;
			LOGGER.info("Tapping to stderr");
		} else {
			try {
				java.io.File f = new java.io.File(path);
				_tapFile = new java.io.FileOutputStream(f);
				LOGGER.info("Tapping to " + f.getAbsolutePath());
			} catch (IOException e) {
				LOGGER.error("Couldn't open tap file: " + path, e);
			}
		}
	}
	private static final AtomicInteger LAST_DEFAULT_CONNECTION_NUMBER = new AtomicInteger();
	private String hostPort;
	private MSNetTCPSocketFactory socketFactory = MSNetConfiguration.getDefaultMSNetTCPSocketFactory();
	private MSNetLoop loop;
	private String beanName;
	private String connectionName;
	private String connectionId;
	private boolean initialized;
	private boolean asyncReading = true;
	private boolean retryFlag = true;
	private long retryTime = 0;
	private Long initialRetryTime;
	private long maxRetryTime = MSNetConnection.DefaultMaxRetryTime;
	private int maxRetries = MSNetConnection.InfiniteRetries;
	private int maxDisconnects = MSNetConnection.DefaultMaxDisconnects;
	private boolean tapping = _tapFile != null;
	private boolean keepAlive = MSNetConfiguration.DEFAULT_TCP_KEEPALIVE;
	private boolean tcpNoDelay = MSNetConfiguration.DEFAULT_TCP_NODELAY;
	private int receiveBufferSize = MSNetConfiguration.DEFAULT_SOCKET_RCV_BUFFER_SIZE;
	private int sendBufferSize = MSNetConfiguration.DEFAULT_SOCKET_SEND_BUFFER_SIZE;
	private List<String> backupAddresses;
	private List<MSNetEstablisher> establishers;
	private Executor callbackExecutor;
	private boolean skipHostPortValidation = false;
	private String bindAddress;
	private long writeListThreshold;
	private boolean kerberos;
	private boolean anonymousAuthentication;
	private boolean connectNotificationOnSyncConnect = false;
	private boolean kerberosEncryption = false;
    private ChannelBindingMode channelBindingMode;
	
	public boolean isInitialized() {
		return initialized;
	}
	public void init() throws UnknownHostException {
		if (!skipHostPortValidation) {
			if (hostPort == null) {
				throw new IllegalArgumentException("Connection address is not set!");
			} else {
				
				new MSNetInetAddress(hostPort);
			}
			if (backupAddresses != null) {
				
				for (Iterator<String> i = getBackupAddresses().iterator(); i.hasNext();) {
					new MSNetInetAddress(i.next());
				}
			}
			if (bindAddress != null) {
				new MSNetInetAddress(bindAddress);
			}
		}
		if (connectionId == null) {
			if (connectionName != null) {
				connectionId = connectionName;
			} else if (beanName != null) {
				connectionId = beanName;
			} else {
				connectionId = "DefConId" + LAST_DEFAULT_CONNECTION_NUMBER.getAndIncrement();
			}
		}
		if (receiveBufferSize <= 0 && receiveBufferSize != -1) {
			throw new IllegalArgumentException("Invalid receive size");
		}
		if (sendBufferSize <= 0 && sendBufferSize != -1) {
			throw new IllegalArgumentException("Invalid receive size");
		}
		if(initialRetryTime != null){
			Validate.isTrue(initialRetryTime <= maxRetryTime, "initialRetryTime should not be greater than maxRetryTime");
		}
		initialized = true;
	}
	@Override
	public void afterPropertiesSet() throws Exception {
		init();
	}
	
	public String getHostPort() {
		return hostPort;
	}
	
	public void setHostPort(String hostPort) {
		this.hostPort = hostPort;
	}
	
	public MSNetTCPSocketFactory getSocketFactory() {
		return socketFactory;
	}
	
	public void setSocketFactory(MSNetTCPSocketFactory socketFactory) {
		Objects.requireNonNull(socketFactory, "The socket factory cannot be null");
		this.socketFactory = socketFactory;
	}
	
	public MSNetLoop getLoop() {
		return loop;
	}
	
	@Override
	public void setLoop(MSNetLoop netLoop) {
		
		if (netLoop != null) {
			this.loop = netLoop;
		}
	}
	
	public String getBeanName() {
		return beanName;
	}
	
	@Override
	public void setBeanName(String name) {
		this.beanName = name;
	}
	
	public String getConnectionName() {
		return connectionName;
	}
	
	public void setConnectionName(String connectionName) {
		this.connectionName = connectionName;
	}
	
	public String getConnectionId() {
		return connectionId;
	}
	
	public boolean getAsyncReading() {
		return asyncReading;
	}
	
	public void setAsyncReading(boolean asyncReading) {
		this.asyncReading = asyncReading;
	}
	
	public boolean getRetryFlag() {
		return retryFlag;
	}
	
	public void setRetryFlag(boolean retryFlag) {
		this.retryFlag = retryFlag;
	}
	
	@Deprecated
	public long getRetryTime() {
		return retryTime;
	}
	
	@Deprecated
	public void setRetryTime(long retryTime) {
		this.retryTime = retryTime;
	}
	
	public long getMaxRetryTime() {
		return maxRetryTime;
	}
	
	public void setMaxRetryTime(long maxRetryTime) {
		this.maxRetryTime = maxRetryTime;
	}
	
	public int getMaxRetries() {
		return maxRetries;
	}
	
	public void setMaxRetries(int maxRetries) {
		this.maxRetries = maxRetries;
	}
	
	public int getMaxDisconnects() {
		return maxDisconnects;
	}
	
	public void setMaxDisconnects(int maxDisconnects) {
		this.maxDisconnects = maxDisconnects;
	}
	
	public boolean getTapping() {
		return tapping;
	}
	
	public void setTapping(boolean tapping) {
		this.tapping = tapping;
	}
	
	public boolean getKeepAlive() {
		return keepAlive;
	}
	
	public void setKeepAlive(boolean keepAlive) {
		this.keepAlive = keepAlive;
	}
	
	public boolean getTcpNoDelay() {
		return tcpNoDelay;
	}
	
	public void setTcpNoDelay(boolean tcpNoDelay) {
		this.tcpNoDelay = tcpNoDelay;
	}
	
	public int getReceiveBufferSize() {
		return receiveBufferSize;
	}
	
	public void setReceiveBufferSize(int receiveBufferSize) {
		this.receiveBufferSize = receiveBufferSize;
	}
	
	public int getSendBufferSize() {
		return sendBufferSize;
	}
	
	public void setSendBufferSize(int sendBufferSize) {
		this.sendBufferSize = sendBufferSize;
	}
	
	public List<String> getBackupAddresses() {
		return backupAddresses;
	}
	
	public void setBackupAddresses(List<String> backupAddresses) throws UnknownHostException {
		this.backupAddresses = backupAddresses;
	}
	
	public List<MSNetEstablisher> getEstablishers() {
		return establishers;
	}
	
	public void setEstablishers(List<MSNetEstablisher> establishers) {
		this.establishers = establishers;
	}
	
	public Executor getCallbackExecutor() {
		return this.callbackExecutor;
	}
	
	public void setCallbackExecutor(Executor callbackExecutor) {
		this.callbackExecutor = callbackExecutor;
	}
	public boolean isSkipHostPortValidation() {
		return skipHostPortValidation;
	}
	public void setSkipHostPortValidation(boolean skipHostPortValidation) {
		this.skipHostPortValidation = skipHostPortValidation;
	}
	
	public String getBindAddress() {
		return bindAddress;
	}
	
	public void setBindAddress(String bindAddress) {
		this.bindAddress = bindAddress;
	}
	
	public void setWriteListThreshold(long size) {
		writeListThreshold = size;
	}
	
	public long getWriteListThreshold() {
		return writeListThreshold;
	}
	
	public boolean getKerberos() {
		return kerberos;
	}
	
	public void setKerberos(boolean kerberos) {
		this.kerberos = kerberos;
	}
	
	public boolean getAnonymousAuthentication() {
		return anonymousAuthentication;
	}
	
	@Deprecated
	public void setAnonymousAuthentication(boolean anonymousAuthentication) {
		this.anonymousAuthentication = anonymousAuthentication;
	}
	public static java.io.OutputStream getTapFile() {
		return _tapFile;
	}
	
	public boolean isConnectNotificationOnSyncConnect() {
		return connectNotificationOnSyncConnect;
	}
	
	public void setConnectNotificationOnSyncConnect(boolean connectNotificationOnSyncConnect) {
		this.connectNotificationOnSyncConnect = connectNotificationOnSyncConnect;
	}
	
	@Override
	public MSNetConnectionConfigurationSupport clone(){
		MSNetConnectionConfigurationSupport clone = new MSNetConnectionConfigurationSupport();
		clone.setAnonymousAuthentication(getAnonymousAuthentication());
		clone.setAsyncReading(getAsyncReading());
		try {
			clone.setBackupAddresses(getBackupAddresses());
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
		clone.setBeanName(getBeanName());
		clone.setConnectionName(getConnectionName());
		clone.setBindAddress(getBindAddress());
		clone.setCallbackExecutor(getCallbackExecutor());
		clone.setConnectNotificationOnSyncConnect(isConnectNotificationOnSyncConnect());
		clone.setEstablishers(getEstablishers());
		clone.setHostPort(getHostPort());
		clone.setSocketFactory(getSocketFactory());
		clone.setKeepAlive(getKeepAlive());
		clone.setKerberos(getKerberos());
		clone.setLoop(getLoop());
		clone.setMaxDisconnects(getMaxDisconnects());
		clone.setMaxRetries(getMaxRetries());
		clone.setMaxRetryTime(getMaxRetryTime());
		clone.setReceiveBufferSize(getReceiveBufferSize());
		clone.setRetryFlag(getRetryFlag());
		clone.setRetryTime(getRetryTime());
		if(getInitialRetryTime() > 0){ 
			clone.setInitialRetryTime(getInitialRetryTime());
		}
		clone.setSendBufferSize(getSendBufferSize());
		clone.setSkipHostPortValidation(isSkipHostPortValidation());
		clone.setTapping(getTapping());
		clone.setTcpNoDelay(getTcpNoDelay());
		clone.setWriteListThreshold(getWriteListThreshold());
		return clone;
	}
	
	public long getInitialRetryTime() {
		if(initialRetryTime == null){
			return 0;
		}
		return initialRetryTime;
	}
	
	public void setInitialRetryTime(long initialRetryTime) {
		Validate.isTrue(initialRetryTime > 0, "initialRetryTime should be greater than 0");
		this.initialRetryTime = initialRetryTime;
	}
	public boolean getKerberosEncryption() {
		return kerberosEncryption;
	}
	@Experimental
	public void setKerberosEncryption(boolean kerberosEncryption) {
		this.kerberosEncryption = kerberosEncryption;
	}
    public ChannelBindingMode getChannelBindingMode() {
        return channelBindingMode;
    }
    
    public void setChannelBindingMode(ChannelBindingMode channelBindingMode) {
        this.channelBindingMode = channelBindingMode;
    }
}
