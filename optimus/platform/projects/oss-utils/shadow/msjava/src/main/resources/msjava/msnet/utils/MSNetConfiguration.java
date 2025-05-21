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

package msjava.msnet.utils;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import msjava.base.annotation.Internal;
import msjava.base.util.internal.SystemPropertyUtils;
import msjava.msnet.*;
import msjava.msnet.MSNetLoop.MSNetLoopErrorHandler;
import msjava.msnet.MSNetLoop.MSNetLoopErrorHandlingStrategy;
import msjava.msnet.MSNetLoop.MaxLoopRestartsErrorHandler;
import msjava.msnet.configurationmanager.ConnectionConfigurationManager;
import msjava.msnet.configurationmanager.ConnectionConfigurationManagerFactory;
import msjava.msnet.ptcp.ExistingStoreAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ServiceLoader;
import java.util.Set;
import javax.annotation.Nonnull;
public class MSNetConfiguration {
    private static final Logger LOGGER = LoggerFactory.getLogger(MSNetConfiguration.class);
    static {
        SystemPropertyUtils.warnIfDepricatedPropertySet("msjava.msnet.buffersizethresholdbytes",
                "Since 7.3 MSNetTCPSocketBuffer has new internal representation", LOGGER);
        SystemPropertyUtils.warnIfDepricatedPropertySet("msjava.msnet.num_writev_buffers", "Since 7.3", LOGGER);
    }
    public static final String OS_NAME = SystemPropertyUtils.getProperty("os.name", LOGGER);
    public static final boolean OS_IS_WINDOWS = OS_NAME.startsWith("Windows");
    public static final boolean OS_IS_LINUX = OS_NAME.startsWith("Linux");
    public static final String USER_NAME = SystemPropertyUtils.getProperty("user.name", LOGGER);
    public final static String FILE_SEPARATOR = SystemPropertyUtils.getProperty("file.separator", LOGGER);
    public final static String TMP_DIR = SystemPropertyUtils.getProperty("java.io.tmpdir", LOGGER);
    
    public static final int RECEIVE_BUFFER_SIZE = SystemPropertyUtils.getScaledInteger("msjava.msnet.recvbuf",
            256 * 1024, LOGGER);
    
    public static final int WRITE_BUFFER_SIZE = SystemPropertyUtils.getScaledInteger("msjava.msnet.writebuf",
            256 * 1024, LOGGER);
    
    public static final int MAX_WRITE_SIZE = SystemPropertyUtils.getScaledInteger("msjava.msnet.max_write_size",
            5 * 1024 * 1024, LOGGER);
    
    public static final int MIN_SOCKETBUFFER_FRAGMENT_SIZE = SystemPropertyUtils.getScaledInteger(
            "msjava.msnet.min_buffer_fragement_size", 8 * 1024, LOGGER);
    
    public static final boolean SHOULD_RETRY_ON_FATAL_PROTOCOL_ERRORS = SystemPropertyUtils.getBoolean(
            "msjava.msnet.retry_fatal_protocol_errors", false, LOGGER);
    
    public static final boolean DISABLE_IMPLICIT_KERBEROS = SystemPropertyUtils.getBoolean(
            "msjava.msnet.disable_implicit_kerberos", false, LOGGER);
    
    
    public static final int DEFAULT_SERVER_SOCKET_BACKLOG_SIZE = SystemPropertyUtils.getInteger(
            "msjava.msnet.server_socket_backlog", 50, LOGGER);
    
    public static final int DEFAULT_REREAD_COUNT = SystemPropertyUtils.getInteger("msjava.msnet.reread_count", 16,
            LOGGER);
    
    public static final boolean DEFAULT_TCP_KEEPALIVE = SystemPropertyUtils.getBoolean("msjava.msnet.so_keepalive",
            true, LOGGER);
    
    public static final boolean DEFAULT_TCP_NODELAY = SystemPropertyUtils.getBoolean("msjava.msnet.tcp_nodelay", true,
            LOGGER);
    
    public static final int DEFAULT_SOCKET_RCV_BUFFER_SIZE = SystemPropertyUtils.getScaledInteger(
            "msjava.msnet.so_rcvbuf", -1, LOGGER);
    
    public static final int DEFAULT_SOCKET_SEND_BUFFER_SIZE = SystemPropertyUtils.getScaledInteger(
            "msjava.msnet.so_sndbuf", -1, LOGGER);
    
    public static final boolean DEFAULT_DISABLE_SEND_DONE_CALLBACK = SystemPropertyUtils.getBoolean(
            "msjava.msnet.disable_senddonecallback", true, LOGGER);
    
    public static final String DEFAULT_MSNET_BIND_ADDRESS = System.getenv("MSNET_BIND_ADDRESS");
    private static final String DIRECT_BUFFER = SystemPropertyUtils.getProperty("msjava.msnet.directbuffer", LOGGER);
    public final static boolean ALLOW_DIRECT_TCP_SOCKET_BUFFER = !("never".equals(DIRECT_BUFFER));
    
    public static final int DEFAULT_OUTPUTSTREAM_FRAGMENT_SIZE = SystemPropertyUtils.getScaledInteger(
            "msjava.msnet.msg_outputstream_fragment_size", 1024, LOGGER);
    
    public static final int DEFAULT_STREAM_LISTENER_QUEUESIZE = SystemPropertyUtils.getInteger(
            "msjava.msnet.streamlistener_queuesize", 5, LOGGER);
    
    public static final int DEFAULT_OUTPUTSTREAM_TIMEOUT = SystemPropertyUtils.getInteger(
            "msjava.msnet.msg_outputstream_timeout", 30000, LOGGER);
    
    
    public static final int DEFAULT_NETMATCH_TIMEOUT = SystemPropertyUtils.getInteger(
            "msjava.msnet.admin.netmatch_timeout", 30000, LOGGER);
    
    public static final String DEFAULT_KUU_CONF = SystemPropertyUtils.getProperty("msjava.msnet.kuu_cf",
            "
    
    public static final String DEFAULT_NETMATCH = SystemPropertyUtils.getProperty("msjava.msnet.netmatch",
            "
    
    @Internal
    public static final boolean CONNECTINGLOCK_OLD_IMPL = SystemPropertyUtils.getBoolean(
            "msjava.msnet.connectingLock.old", LOGGER);
    private static int UPDATE_VERSION = setUpdateVersion();
    private static int setUpdateVersion() {
        int version = SystemPropertyUtils.getScaledInteger("msjava.msnet.update_version", 0, LOGGER);
        checkUpdateVersion(version);
        return version;
    }
    private static void checkUpdateVersion(int version) {
        if (version != 0 && version != 1) {
            throw new IllegalArgumentException(String.format("Wrong MSNet update version provided: %d. Supported versions: 0, 1", version));
        }
    }
    
    public static int getUpdateVersion() {
        return UPDATE_VERSION;
    }
    
    
    public static final int DEFAULT_LOOP_SHUTDOWN_TIMEOUT = SystemPropertyUtils.getInteger(
            "msjava.msnet.threadpool_loop_shutdown_timeout", 5000, LOGGER);
    
    public static final boolean DEFAULT_USE_CANONICAL_NAME = SystemPropertyUtils.getBoolean(
            "msjava.msnet.use_canonical_hostname", true, LOGGER);
    public static final String DEFAULT_CONNECTION_PER_HOST_WARN_THRESHOLD_PROPERTY = "msjava.msnet.connection_per_host.warn_threshold";
    public static final int DEFAULT_CONNECTION_PER_HOST_WARN_THRESHOLD = SystemPropertyUtils.getInteger(
            DEFAULT_CONNECTION_PER_HOST_WARN_THRESHOLD_PROPERTY, 20, LOGGER);
    public static final String DEFAULT_CONNECTION_PER_HOST_LIMIT_PROPERTY = "msjava.msnet.connection_per_host.limit";
    public static final int DEFAULT_CONNECTION_PER_HOST_LIMIT;
    
    
    
    static {
        Integer newVal = Integer.getInteger(DEFAULT_CONNECTION_PER_HOST_LIMIT_PROPERTY);
        Integer oldVal = Integer.getInteger("DEFAULT_CONNECTION_PER_HOST_LIMIT_PROPERTY");
        if (newVal != null || oldVal == null) {
            if (oldVal != null) {
                LOGGER.warn("Depricated \"DEFAULT_CONNECTION_PER_HOST_LIMIT_PROPERTY\" system propery is set, "
                                + "using overridden value from '{}' system property",
                        DEFAULT_CONNECTION_PER_HOST_LIMIT_PROPERTY);
            }
            
            DEFAULT_CONNECTION_PER_HOST_LIMIT = SystemPropertyUtils.getInteger(
                    DEFAULT_CONNECTION_PER_HOST_LIMIT_PROPERTY, 70, LOGGER);
        } else {
            LOGGER.warn("Depricated \"DEFAULT_CONNECTION_PER_HOST_LIMIT_PROPERTY\" system propery is set,"
                    + " please use {} instead, using limit ({})", DEFAULT_CONNECTION_PER_HOST_LIMIT_PROPERTY, oldVal);
            DEFAULT_CONNECTION_PER_HOST_LIMIT = oldVal;
        }
    }
    
    
    private static int loopStartupTimeout = SystemPropertyUtils.getInteger("msjava.msnet.default_loop_startup_timeout",
            10000, LOGGER);
    
    @Nonnull
    private static MSNetLoopErrorHandler loopErrorHandler;
    private static Boolean propagateChannelCallbackExceptions;
    static {
        
        setDefaultLoopErrorHandler(null);
    }
    
    private static final String defaultSocketFactory_default = MSNetTCPSocketFactoryNIOImpl.class.getName();
    
    @Nonnull
    private static MSNetTCPSocketFactory defaultSocketFactory;
    static {
        
        setDefaultMSNetTCPSocketFactory(null);
    }
    
    private static boolean adoptBuffers = SystemPropertyUtils.getBoolean("msjava.msnet.default_adopt_buffers", true,
            LOGGER);
    
    private static boolean socketBufferDirect = "always".equals(MSNetConfiguration.DIRECT_BUFFER);
    
    private static String writevMode = SystemPropertyUtils.getProperty("msjava.msnet.writev_mode", "direct", LOGGER);
    
    private static int defaultStreamBufferSizeLimit = SystemPropertyUtils.getInteger("msjava.msnet.default_stream_buffer_size_limit", -1, LOGGER);
    
    
    public static final int PTCP_VERSION_IMPLEMENTED = 2;
    
    public static final int MAX_PTCP_VERSION_SUPPORTED;
    static {
        Integer v = SystemPropertyUtils.getInteger("msjava.msnet.max_supported_ptcp_ver", PTCP_VERSION_IMPLEMENTED,
                LOGGER);
        if (v >= 0 && v <= PTCP_VERSION_IMPLEMENTED) {
            
            MAX_PTCP_VERSION_SUPPORTED = v;
        } else {
            LOGGER.warn("Invalid ptcp version specified in \"msjava.msnet.max_supported_ptcp_ver\", using default");
            MAX_PTCP_VERSION_SUPPORTED = PTCP_VERSION_IMPLEMENTED;
        }
    }
    public static final String DEFAULT_PTCP_NIO_STORE_DIR = getDefaultStoreDirectory();
    private static String getDefaultStoreDirectory() {
        String storeDir;
        if (MSNetConfiguration.OS_IS_WINDOWS) {
            
            String tmpdir = MSNetConfiguration.TMP_DIR;
            if (!tmpdir.endsWith(MSNetConfiguration.FILE_SEPARATOR)) {
                tmpdir = tmpdir + MSNetConfiguration.FILE_SEPARATOR;
            }
            storeDir = tmpdir + "MSNetPTCPManager." + MSNetConfiguration.USER_NAME;
        } else {
            storeDir = "/var/tmp/MSNetPTCPManager." + MSNetConfiguration.USER_NAME;
        }
        return storeDir;
    }
    
    private static int ptcpRecordSize = SystemPropertyUtils.getScaledInteger("MSNET_PT_RECORD_SIZE", 4096, LOGGER);
    
    private static int ptcpMaxRecords = SystemPropertyUtils.getInteger("MSNET_PT_MAX_RECORDS", 100, LOGGER);
    
    private static int ptcpAutoIncrement = SystemPropertyUtils.getInteger("MSNET_PT_AUTO_INCREMENT", 100, LOGGER);
    
    private static int ptcpSafetyLevel = SystemPropertyUtils.getInteger("MSNET_PT_SAFETY_LEVEL", 0, LOGGER);
    
    private static long ptcpMaxStoreSize = SystemPropertyUtils.getLong("MSNET_PT_MAX_STORE_SIZE", -1, LOGGER);
    
    private static int ptcpHeartbeatTimeout = SystemPropertyUtils.getInteger("MSNET_PT_HEARTBEAT_TIMEOUT", 20000,
            LOGGER);
    
    private static int ptcpPeerIdleTimeout = SystemPropertyUtils.getInteger("MSNET_PT_IDLE_TIMEOUT", 45000, LOGGER);
    
    private static String ptcpStoreDirectory = SystemPropertyUtils.getProperty("MSNET_PT_STORE_DIR",
            DEFAULT_PTCP_NIO_STORE_DIR, LOGGER);
    
    private static String ptcpManagerName = SystemPropertyUtils.getProperty("MSNET_PT_MANAGER_NAME", LOGGER);
    
    private static boolean ptcpSeqnoBumpEnabled = SystemPropertyUtils.getBoolean("MSNET_PT_SEQNO_BUMP_ENABLED", true, LOGGER);
    
    private static ExistingStoreAction ptcpExistingStoreAction =
            ExistingStoreAction.fromString(
                    SystemPropertyUtils.getProperty("MSNET_PT_EXISTING_STORE_ACTION", ExistingStoreAction.defaultValue().toString(), LOGGER));
    
    public static final long DEFAULT_PTCP_STALE_SESSION_EXPIRY_TIMEOUT = SystemPropertyUtils.getLong("msjava.msnet.ptcp.stale.session.expiry.timeout", 60000, LOGGER);
    
    
    private static boolean debugAdminUsers = SystemPropertyUtils.getBoolean("msjava.msnet.debug_admin_users", LOGGER);
    public static final String AUTH_PROTOCOL_DEBUG_PROPERTY = "msjava.msnet.auth.protocol_debug";
    
    private static boolean debugAuthProtocol = SystemPropertyUtils.getBoolean(AUTH_PROTOCOL_DEBUG_PROPERTY, LOGGER);
    private static int defaultMaxMessageSize = SystemPropertyUtils.getInteger("msjava.msnet.max_messagesize", MSNetStringProtocol.DEFAULT_MAX_MESSAGE_SIZE, LOGGER);
    
    public static final boolean USE_NEW_CALLBACK_STRATEGY = SystemPropertyUtils.getBoolean(
            "msjava.msnet.use_new_callback_strategy", false, LOGGER);
    
    
    public static final boolean DEFAULT_LOCKSAFE_EXECUTOR_FAILONBADLOCKSTATE = SystemPropertyUtils.getBoolean("msnet.locksafeexecutor.failonbadlockstate", USE_NEW_CALLBACK_STRATEGY, LOGGER);
    
    public static final boolean DEFAULT_LOCKSAFE_EXECUTOR_WARNIFBADLOCKSTATE = SystemPropertyUtils.getBoolean("msnet.locksafeexecutor.warnifbadlockstate", false, LOGGER);
    
    
    public static final boolean DEFAULT_LOCKSAFE_EXECUTOR_WARNIFNOLOOPINGLOOP = SystemPropertyUtils.getBoolean("msnet.locksafeexecutor.warnifnoloopingloop", true, LOGGER);
    
    public static final boolean IGNORE_WRITE_FAILURE = SystemPropertyUtils.getBoolean("msjava.msnet.experimental_ignore_write_failure", false, LOGGER);
    static {
        String krbImpl = SystemPropertyUtils.getProperty("msjava.msnet.auth.krbImpl", LOGGER);
        if (krbImpl != null) {
            LOGGER.warn("msjava.msnet.auth.krbImpl property is set. Its value will be ignored.");
        }
    }
    private MSNetConfiguration() {
    }
    
    public static int getLoopStartupTimeout() {
        return loopStartupTimeout;
    }
    
    public static void setLoopStartupTimeout(int loopStartupTimeout) {
        MSNetConfiguration.loopStartupTimeout = loopStartupTimeout;
    }
    
    public static MSNetLoopErrorHandler getDefaultLoopErrorHandler() {
        return loopErrorHandler;
    }
    
    public static void setDefaultLoopErrorHandler(final MSNetLoopErrorHandler loopErrorHandler) {
        if (loopErrorHandler == null) {
            String confVal = SystemPropertyUtils.getProperty("msjava.msnet.default_loop_error_handling_strategy",
                    MSNetLoopErrorHandlingStrategy.LOG_AND_RETHROW.name(), LOGGER);
            MSNetLoopErrorHandler leh = null;
            try {
                leh = MSNetLoopErrorHandlingStrategy.valueOf(confVal);
            } catch (Exception e) {
                
            }
            if (leh == null) {
                try {
                    Class<?> klass = MSNetConfiguration.class.getClassLoader().loadClass(confVal);
                    if (!MSNetLoopErrorHandler.class.isAssignableFrom(klass)) {
                        LOGGER.error("Failed to instantiate MSNetLoopErrorHandler class " + confVal
                                + " - class does not seem to implement MSNetLoopErrorHandler, "
                                + "falling back to default strategy MSNetLoopErrorHandlingStrategy.LOG_AND_RETHROW.");
                        leh = MSNetLoopErrorHandlingStrategy.LOG_AND_RETHROW;
                    } else {
                        leh = (MSNetLoopErrorHandler) klass.newInstance();
                    }
                } catch (Throwable t) {
                    LOGGER.error("Failed to instantiate MSNetLoopErrorHandler class " + confVal
                            + ", falling back to default strategy MSNetLoopErrorHandlingStrategy.LOG_AND_RETHROW. "
                            + "Make sure you specified the class name correctly (internal classes are specified with a dollar sign!), "
                            + "can be found on the runtime classpath and has a parameterless constructor. If you meant to specify "
                            + "a default implementation from MSNetLoopErrorHandlingStrategy, double-check your spelling and note "
                            + "that they are case-sensitive.", t);
                    leh = MSNetLoopErrorHandlingStrategy.LOG_AND_RETHROW;
                }
            }
            MSNetConfiguration.loopErrorHandler = leh;
        } else {
            MSNetConfiguration.loopErrorHandler = loopErrorHandler;
        }
        
        if (propagateChannelCallbackExceptions == null) {
            propagateChannelCallbackExceptions = SystemPropertyUtils
                    .getBoolean("msjava.msnet.propagate_channel_callback_exceptions", false, LOGGER);
        }
        
        
        if (!propagateChannelCallbackExceptions
                && (loopErrorHandler != null
                || System.getProperty("msjava.msnet.default_loop_error_handling_strategy") != null)
                && System.getProperty("msjava.msnet.propagate_channel_callback_exceptions") == null) {
            propagateChannelCallbackExceptions = true;
            LOGGER.info(
                    "Automatically enabled channel callback exception propagation because a loop error handler was explicitly set");
        }
    }
    
    public static boolean propagateChannelCallbackExceptions() {
        return propagateChannelCallbackExceptions;
    }
    
    @Nonnull
    public static MSNetTCPSocketFactory getDefaultMSNetTCPSocketFactory() {
        return defaultSocketFactory;
    }
    
    public static void setDefaultMSNetTCPSocketFactory(MSNetTCPSocketFactory socketFactory) {
        if (socketFactory == null) {
            String confVal = SystemPropertyUtils.getProperty("msjava.msnet.default_socket_factory",
                    defaultSocketFactory_default, LOGGER);
            if (!confVal.isEmpty() && !confVal.equals(defaultSocketFactory_default)) {
                try {
                    Class<?> klass = MSNetConfiguration.class.getClassLoader().loadClass(confVal);
                    if (!MSNetTCPSocketFactory.class.isAssignableFrom(klass)) {
                        LOGGER.error("Failed to instantiate MSNetTCPSocketFactory class " + confVal
                                + " - class does not seem to implement MSNetTCPSocketFactory, "
                                + "falling back to default MSNetTCPSocketFactoryNIOImpl.");
                    } else {
                        socketFactory = (MSNetTCPSocketFactory) klass.newInstance();
                    }
                } catch (Throwable t) {
                    LOGGER.error("Failed to instantiate MSNetTCPSocketFactory class " + confVal
                            + ", falling back to default MSNetTCPSocketFactoryNIOImpl. "
                            + "Make sure you specified the class name correctly (internal classes are specified with a dollar sign!), "
                            + "can be found on the runtime classpath and has a parameterless constructor.", t);
                }
            }
            if (socketFactory == null) {
                socketFactory = MSNetTCPSocketFactoryNIOImpl.defaultInstance();
            }
        }
        defaultSocketFactory = socketFactory;
    }
    
    public static boolean isAdoptBuffers() {
        return adoptBuffers;
    }
    
    public static void setAdoptBuffers(boolean adoptBuffers) {
        MSNetConfiguration.adoptBuffers = adoptBuffers;
    }
    
    public static boolean isSocketBufferDirect() {
        return socketBufferDirect;
    }
    
    public static void setSocketBufferDirect(boolean socketBufferDirect) {
        MSNetConfiguration.socketBufferDirect = socketBufferDirect;
    }
    
    public static String getWritevMode() {
        return writevMode;
    }
    
    public static void setWritevMode(String writevMode) {
        MSNetConfiguration.writevMode = writevMode;
    }
    
    public static long getPtcpMaxStoreSize() {
        return ptcpMaxStoreSize;
    }
    
    public static void setPtcpMaxStoreSize(long ptcpMaxStoreSize) {
        MSNetConfiguration.ptcpMaxStoreSize = ptcpMaxStoreSize;
    }
    
    public static int getPtcpHeartbeatTimeout() {
        return ptcpHeartbeatTimeout;
    }
    
    public static void setPtcpHeartbeatTimeout(int ptcpHeartbeatTimeout) {
        MSNetConfiguration.ptcpHeartbeatTimeout = ptcpHeartbeatTimeout;
    }
    
    public static int getPtcpPeerIdleTimeout() {
        return ptcpPeerIdleTimeout;
    }
    
    public static void setPtcpPeerIdleTimeout(int ptcpPeerIdleTimeout) {
        MSNetConfiguration.ptcpPeerIdleTimeout = ptcpPeerIdleTimeout;
    }
    
    public static int getPtcpRecordSize() {
        return ptcpRecordSize;
    }
    
    public static void setPtcpRecordSize(int ptcpRecordSize) {
        MSNetConfiguration.ptcpRecordSize = ptcpRecordSize;
    }
    
    public static int getPtcpMaxRecords() {
        return ptcpMaxRecords;
    }
    
    public static void setPtcpMaxRecords(int ptcpMaxRecords) {
        MSNetConfiguration.ptcpMaxRecords = ptcpMaxRecords;
    }
    
    public static int getPtcpAutoIncrement() {
        return ptcpAutoIncrement;
    }
    
    public static void setPtcpAutoIncrement(int ptcpAutoIncrement) {
        MSNetConfiguration.ptcpAutoIncrement = ptcpAutoIncrement;
    }
    
    public static int getPtcpSafetyLevel() {
        return ptcpSafetyLevel;
    }
    
    public static void setPtcpSafetyLevel(int ptcpSafetyLevel) {
        MSNetConfiguration.ptcpSafetyLevel = ptcpSafetyLevel;
    }
    
    public static String getPtcpStoreDirectory() {
        return ptcpStoreDirectory;
    }
    
    public static void setPtcpStoreDirectory(String ptcpStoreDirectory) {
        MSNetConfiguration.ptcpStoreDirectory = ptcpStoreDirectory;
    }
    
    public static String getPtcpManagerName() {
        return ptcpManagerName;
    }
    
    public static void setPtcpManagerName(String ptcpManagerName) {
        MSNetConfiguration.ptcpManagerName = ptcpManagerName;
    }
    
    public static boolean isPtcpSeqnoBumpEnabled() {
        return ptcpSeqnoBumpEnabled;
    }
    
    public static void setPtcpSeqnoBumpEnabled(boolean ptcpSeqnoBumpEnabled) {
        MSNetConfiguration.ptcpSeqnoBumpEnabled = ptcpSeqnoBumpEnabled;
    }
    
    public static ExistingStoreAction getPtcpExistingStoreAction() {
        return ptcpExistingStoreAction;
    }
    
    public static void setPtcpExistingStoreAction(ExistingStoreAction ptcpExistingStoreAction) {
        MSNetConfiguration.ptcpExistingStoreAction = ptcpExistingStoreAction;
    }
    
    @Deprecated
    public static String getKrbImpl() {
        return null;
    }
    
    @Deprecated
    public static void setKrbImpl(String krbImpl) {}
    
    public static boolean isDebugAdminUsers() {
        return debugAdminUsers;
    }
    
    public static void setDebugAdminUsers(boolean debugAdminUsers) {
        MSNetConfiguration.debugAdminUsers = debugAdminUsers;
    }
    
    public static boolean isDebugAuthProtocol() {
        return debugAuthProtocol;
    }
    
    public static void setDebugAuthProtocol(boolean debugAuthProtocol) {
        MSNetConfiguration.debugAuthProtocol = debugAuthProtocol;
    }
    private static final ImmutableList<ConnectionConfigurationManagerFactory> CONFIG_MANAGER_FACTORIES = loadConfigManagerFactories();
    private static ImmutableList<ConnectionConfigurationManagerFactory> loadConfigManagerFactories() {
        try {
            String requiredConfigManagersEnv = System.getenv("MSNET_CONFIGURATION_MANAGER");
            if (requiredConfigManagersEnv == null) {
                requiredConfigManagersEnv = "";
            }
            Set<String> reqiredManagers = new HashSet<>(Splitter.on(",").trimResults().omitEmptyStrings()
                    .splitToList(requiredConfigManagersEnv));
            
            String ipcMode = System.getenv("MSNET_IPC_AUTH_MODE");
            if (!(ipcMode == null || "DISABLE".equals(ipcMode))) {
                reqiredManagers.add("IPC-AUTH-" + ipcMode);
            }
            if (reqiredManagers.isEmpty()) {
                LOGGER.debug("No required connection configuration manager specified");
                return ImmutableList.of();
            }
            LOGGER.info("Loading reqired configuration managers: {}", reqiredManagers.size());
            ImmutableList<ConnectionConfigurationManagerFactory> availConfigMannFact = AccessController.doPrivileged(
                    new PrivilegedExceptionAction<ImmutableList<ConnectionConfigurationManagerFactory>>() {
                        @Override
                        public ImmutableList<ConnectionConfigurationManagerFactory> run() throws Exception {
                            ServiceLoader<ConnectionConfigurationManagerFactory> serviceLoader = ServiceLoader
                                    .load(ConnectionConfigurationManagerFactory.class);
                            return ImmutableList.copyOf(serviceLoader.iterator());
                        }
                    });
            HashMap<String, ConnectionConfigurationManagerFactory> managersByName = new HashMap<String, ConnectionConfigurationManagerFactory>();
            for (ConnectionConfigurationManagerFactory m : availConfigMannFact) {
                ConnectionConfigurationManagerFactory dupl = managersByName.put(m.getName(), m);
                if (dupl != null) {
                    throw new RuntimeException("More than one configuration manager using same name. name="
                            + m.getName() + " class1=" + dupl.getClass() + " class2=" + m.getClass());
                }
            }
            ArrayList<ConnectionConfigurationManagerFactory> rmfs = new ArrayList<ConnectionConfigurationManagerFactory>(
                    reqiredManagers.size());
            for (String reqManName : reqiredManagers) {
                ConnectionConfigurationManagerFactory rmf = managersByName.get(reqManName);
                if (rmf == null) {
                    throw new RuntimeException("Required configuration manager (" + reqManName
                            + ") cannot be loaded (available=" + managersByName.keySet()
                            + "), please check apllication classpath");
                }
                rmf.initialize();
                rmfs.add(rmf);
            }
            return ImmutableList.copyOf(rmfs);
        } catch (PrivilegedActionException e) {
            throw new RuntimeException("Failed to load available connfiguration managers", e);
        }
    }
    private static final ImmutableList<ConnectionConfigurationManager> CONFIG_MANAGERS = ImmutableList.copyOf(
            CONFIG_MANAGER_FACTORIES.stream().map(from -> from.createConfigurationManager())::iterator);
    public static ImmutableList<ConnectionConfigurationManager> getConfigurationManagers() {
        return CONFIG_MANAGERS;
    }
    public static int getDefaultMaxMessageSize() {
        return defaultMaxMessageSize;
    }
    public static void setDefaultMaxMessageSize(int newDefault) {
        defaultMaxMessageSize = newDefault;
    }
    
    public static int getDefaultStreamBufferSizeLimit() {
        return defaultStreamBufferSizeLimit;
    }
    
    public static void setStreamBufferSizeLimit(int streamBufferSizeLimit) {
        if (streamBufferSizeLimit == 0)
            throw new IllegalArgumentException("0 buffer size limit is not allowed");
        defaultStreamBufferSizeLimit = streamBufferSizeLimit;
    }
}
