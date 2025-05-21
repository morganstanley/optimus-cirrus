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

package msjava.zkapi.internal;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.ms.infra.zookeeper.utils.ConnectionInfo;
import com.ms.infra.zookeeper.utils.ImmutableConnectionInfo;
import com.ms.infra.zookeeper.utils.curator.ExponentialBackoffRetryForever;
import com.ms.infra.zookeeper.utils.curator.MsCuratorFrameworkCustomizer;
import com.ms.infra.zookeeper.utils.hosted.ZkConnectionResolver;
import com.ms.infra.zookeeper.utils.hosted.ZkEnv;
import com.ms.infra.zookeeper.utils.hosted.ZkRegion;
import java.io.Closeable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.concurrent.GuardedBy;
import msjava.base.slf4j.ContextLogger;
import msjava.base.slr.internal.ServiceEnvironment;
import msjava.zkapi.ZkaAttr;
import msjava.zkapi.ZkaConfig;
import msjava.zkapi.ZkaPathContext;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.client.ConnectStringParser;
import org.slf4j.Logger;
public final class ZkaResourceContext implements Closeable {
    
    private static final List<ZkaResourceContext> sharedResourceContexts = new ArrayList<>();
    
    @VisibleForTesting
    static int getOpenContexts() {
        synchronized (sharedResourceContexts) {
            return sharedResourceContexts.size();
        }
    }
    public static ZkaResourceContext getResourceContext(ZkaContext ctx) {
        return getResourceContext((ZkaPathContext) ctx);
    }
    
    public static ZkaResourceContext getResourceContext(ZkaPathContext ctx) {
        return getResourceContext(ctx, new DefaultACLProvider(ctx.getBasePath(), ctx.getConnectionTimeout()));
    }
    public static ZkaResourceContext getResourceContext(ZkaPathContext ctx, ACLProvider aclProvider) {
        if (ctx.isJailed()) {
            return new ZkaResourceContext(ctx, aclProvider);
        }
        synchronized (sharedResourceContexts) {
            
            for (final ZkaResourceContext rctx : sharedResourceContexts) {
                if (rctx.usesSameEnsemble(ctx)) {
                    log.debug("adding context {} to {}", ctx, rctx);
                    
                    rctx.registeredContexts.add(ctx);
                    return rctx;
                }
            }
            
            
            final ZkaResourceContext rctx = new ZkaResourceContext(ctx, aclProvider);
            sharedResourceContexts.add(rctx);
            return rctx;
        }
    }
    
    
    @Deprecated
    public static final RetryPolicy  RETRY_POLICY = ExponentialBackoffRetryForever.builder().build();
    private static final Logger      log          = ContextLogger.safeLogger();
    private static final AtomicLong  NEXT_ID      = new AtomicLong(0);
    private final long               id           = NEXT_ID.incrementAndGet();
    
    private final CuratorFramework   curator;
    
	private final String             connection;
    
    @GuardedBy("sharedResourceContexts")
    private final Set<ZkaPathContext> registeredContexts = new HashSet<>();
    
    private final ACLProvider         aclProvider;
    private ZkaResourceContext(ZkaPathContext context, ACLProvider aclProvider) {
		this.aclProvider = aclProvider;
        this.connection = connectionKey(context);
        log.info(
				"creating a new resource context for {}.  This involves the creation of threads, buffers, and connection objects.",
				connection);
        curator = newCuratorFramework(context, aclProvider);
        curator.getConnectionStateListenable().addListener(new LoggingConnectionStateListener(this));
        curator.start();
        log.debug("adding context {} to {}", context, this);
        
        
        registeredContexts.add(context);
    }
    private static String connectionKey(ZkaPathContext context) {
        return context.getEnsemble() + context.getRootNode() + (kerberized(context) ? "" : "(non-krb)");
    }
    
    public CuratorFramework getCurator() {
        return curator;
    }
    public final ACLProvider getAclProvider() {
        return aclProvider;
    }
    
    private boolean usesSameEnsemble(ZkaPathContext ctx) {
		return connection.equals(connectionKey(ctx));
    }
    public static CuratorFramework newCurator(ZkaConfig config, ACLProvider aclProvider) {
        return newCuratorFramework(new ConfigProperties(config), aclProvider);
    }
    
    private static CuratorFramework newCuratorFramework(ZkaPathContext context, ACLProvider aclProvider) {
        return newCuratorFramework(new ContextProperties(context), aclProvider);
    }
    private static CuratorFramework newCuratorFramework(ConnectionProperties context, ACLProvider aclProvider) {
        boolean needAuthenticated = kerberized(context);
        final ImmutableConnectionInfo connectionInfo;
        if (context.isHosted()) {
            ZkEnv zkEnv = ZkEnv.valueOf(context.getEnvironment().name().toUpperCase());
            ZkRegion zkRegion = ZkRegion.valueOf(context.getRegion().toUpperCase());
            ConnectionInfo resolvedInfo = ZkConnectionResolver
                .fromEnvRegionChroot(zkEnv, zkRegion, context.getRootNode())
                .authenticated(needAuthenticated)
                .resolve();
            connectionInfo = ImmutableConnectionInfo
                .fromServers(resolvedInfo.servers())
                .chroot(resolvedInfo.chroot())
                .proid(resolvedInfo.proid())
                .build();
        } else {
            ImmutableConnectionInfo parsedInfo = ImmutableConnectionInfo
                .parse(context.getEnsemble() + context.getRootNode());
            boolean parsedHasProid = parsedInfo.proid() != null;
            if (parsedHasProid == needAuthenticated) {
                connectionInfo = parsedInfo;
            } else if (!needAuthenticated) {
                connectionInfo = parsedInfo.forUnauthenticated();
            } else {
                throw new IllegalArgumentException("The connection string does not have a proid, "
                    + "which is required for a kerberized connection: " + context.getEnsemble());
            }
        }
        
        checkResolvedAddresses(connectionInfo.servers());
        int connectionTimeout = context.getInteger(ZkaAttr.CONNECTION_TIMEOUT);
        int sessionTimeout = context.getInteger(ZkaAttr.SESSION_TIMEOUT);
        log.debug("connectionTimeout={}  sessionTimeout={}", connectionTimeout, sessionTimeout);
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder()
            .ensembleProvider(new ImmutableMsEnsembleProvider(connectionInfo))
            .connectionTimeoutMs(connectionTimeout)
            .sessionTimeoutMs(sessionTimeout)
            .retryPolicy(RETRY_POLICY)
            .aclProvider(aclProvider);
        return MsCuratorFrameworkCustomizer.createMinimal().customize(builder).build();
    }
    
    private static void checkResolvedAddresses(String zkConn) {
        Collection<InetSocketAddress> serverAddresses = new ConnectStringParser(zkConn).getServerAddresses();
        int count = 0;
        for (InetSocketAddress address : serverAddresses) {
            try {
                String curHostString = address.getHostString();
                List<InetAddress> resolvedAddressesList = Arrays.asList(InetAddress.getAllByName(curHostString));
                if (!resolvedAddressesList.isEmpty())
                    count++;
            } catch (UnknownHostException e) {
                log.error("Unable to resolve address: {}", address.toString(), e);
            }
        }
        if (count == 0) {
            throw new IllegalArgumentException(
                    "A HostProvider may not be empty!");
        }
    }
    public static final class ParsedPath {
        private final String rootNode;
        private final String basePath;
        private final String source;
        private ParsedPath(String path) {
            this.source  = path;
            String[] split  = path.split("/", 3);
            this.rootNode    = "/" + split[1];
            this.basePath    = "/" + (split.length == 3 ? split[2] : "");
        }
        public String rootNode() {return rootNode;}
        public String basePath() {return basePath;}
        public String source()   {return source;}
    }
    public static ParsedPath validatePath(String path) {
        if (path != null && path.startsWith("/") && !path.startsWith("
            return new ParsedPath(path);
        else
            throw new IllegalArgumentException("Malformed path [" + path
                    + "], must have a root node, must start with a forward slash, but not end with a forward slash.");
    }
    private interface ConnectionProperties {
        boolean isHosted();
        String getRootNode();
        String getEnsemble();
        boolean getBoolean(ZkaAttr attr);
        int getInteger(ZkaAttr attr);
        ServiceEnvironment getEnvironment();
        String getRegion();
    }
    private static class ContextProperties implements ConnectionProperties {
        private final ZkaPathContext context;
        public ContextProperties(ZkaPathContext context) {
            this.context = context;
        }
        @Override
        public boolean isHosted() {
            return context.isHosted();
        }
        @Override
        public String getRootNode() {
            return context.getRootNode();
        }
        @Override
        public String getEnsemble() {
            return context.getEnsemble();
        }
        @Override
        public boolean getBoolean(ZkaAttr attr) {
            return context.getBoolean(attr);
        }
        @Override
        public int getInteger(ZkaAttr attr) {
            return context.getInteger(attr);
        }
        @Override
        public ServiceEnvironment getEnvironment() {
            return context.getEnvironment();
        }
        @Override
        public String getRegion() {
            return context.getRegion();
        }
    }
    private static class ConfigProperties implements ConnectionProperties {
        private final ZkaConfig config;
        private final String rootNode;
        public ConfigProperties(ZkaConfig config) {
            this.config = config;
            this.rootNode = ZkaResourceContext.validatePath(config.getPath()).rootNode();
        }
        @Override
        public boolean isHosted() {
            return config.isHosted();
        }
        @Override
        public String getRootNode() {
            return rootNode;
        }
        @Override
        public String getEnsemble() {
            return config.getEnsemble();
        }
        @Override
        public boolean getBoolean(ZkaAttr attr) {
            return config.getBoolean(attr);
        }
        @Override
        public int getInteger(ZkaAttr attr) {
            return config.getInteger(attr);
        }
        @Override
        public ServiceEnvironment getEnvironment() {
            return config.getEnvironment();
        }
        @Override
        public String getRegion() {
            return config.getRegion();
        }
    }
    @VisibleForTesting
    static boolean kerberized(ZkaPathContext context) {
        return kerberized(new ContextProperties(context));
    }
    private static boolean kerberized(ConnectionProperties context) {
        boolean krb = context.getBoolean(ZkaAttr.KERBEROS);
        if (krb) {
            return true;
        }
        if (context.getEnvironment() == ServiceEnvironment.prod) {
            log.warn("Non-kerberized connection to a Prod ZooKeeper instance requested");
        } else {
            log.info("Non-kerberized connection requested");
        }
        return false;
    }
    @Deprecated
    public void unregister(ZkaContext context) {
        unregister((ZkaPathContext) context);
    }
    
    public void unregister(ZkaPathContext context) {
        synchronized (sharedResourceContexts) {
            log.info("unregistering {}", context);
            registeredContexts.remove(context);
            log.debug("removed context {} from {}", context, this);
            if (registeredContexts.size() == 0) {
                close();
            }
        }
    }
    @Deprecated
    public void register(ZkaContext context) {
        register((ZkaPathContext) context);
    }
    
    public void register(ZkaPathContext context) {
        synchronized (sharedResourceContexts) {
            Preconditions.checkState(!registeredContexts.isEmpty());
            log.info("registering {}", context);
            registeredContexts.add(context);
            log.debug("added context {} from {}", context, this);
        }
    }
    
    @Override
    public void close() {
        synchronized (sharedResourceContexts) {
            sharedResourceContexts.remove(this);
            log.debug("number of resource contexts remaining {}", sharedResourceContexts.size());
            curator.close();
        }
    }
    @Override
    public String toString() {
        synchronized (sharedResourceContexts) {
			return String.format("ZkaResourceContext #%d [contexts=%s,connection=%s]", id,
					registeredContexts, connection);
        }
    }
    @VisibleForTesting
    static List<ZkaResourceContext> sharedContexts() {
        synchronized (sharedResourceContexts) {
            return new ArrayList<>(sharedResourceContexts);
        }
    }
    
    private static final class LoggingConnectionStateListener implements ConnectionStateListener {
        private final static String host = getHost();
        private final static String user = System.getProperty("user.name");
        private final String fmt;
        private LoggingConnectionStateListener(ZkaResourceContext zrc) {
            this.fmt = String.format("[New state={}, sessionid={}, host=%s, user=%s, connection=%s]", host, user, zrc.connection);
        }
        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState) {
            long currentSessionId = getSessionId(client);
            if (0 < currentSessionId) {
                log.info(fmt, newState, toSessionIdString(currentSessionId));
            }
        }
        private static String getHost() {
            try {
                return InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                return "<NA>";
            }
        }
        private static long getSessionId(CuratorFramework client) {
            try {
                return client.getZookeeperClient().getZooKeeper().getSessionId();
            } catch (Exception e) {
                return -1; 
            }
        }
        private static String toSessionIdString(long sessionid) {
            return 0 < sessionid ? "0x" + Long.toHexString(sessionid) : "<NA>";
        }
    }
}
