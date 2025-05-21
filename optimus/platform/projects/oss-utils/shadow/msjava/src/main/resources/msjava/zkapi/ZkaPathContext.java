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

package msjava.zkapi;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import msjava.base.annotation.Internal;
import msjava.base.slf4j.ContextLogger;
import msjava.base.slr.internal.ServiceEnvironment;
import msjava.zkapi.annotation.ZkaAsyncOperation;
import msjava.zkapi.annotation.ZkaSyncOperation;
import msjava.zkapi.internal.DefaultACLProvider;
import msjava.zkapi.internal.ZkaResourceContext;
import msjava.zkapi.internal.ZkaResourceContext.ParsedPath;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.ACLBackgroundPathAndBytesable;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.api.PathAndBytesable;
import org.apache.curator.framework.api.Pathable;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import com.google.common.collect.Maps;
public class ZkaPathContext implements Closeable {
    
    
    private static final Logger        log                    = ContextLogger.safeLogger();
    private static final AtomicLong    NEXT_ID                = new AtomicLong(0);
    
    private final long                 id                     = NEXT_ID.incrementAndGet();
    
    private final CuratorFramework     curator;
    
    @Nullable
    private final ZkaResourceContext   resourceContext;
    
    @GuardedBy("itself")
    private final Set<Closeable>       references = new HashSet<>();
    
    private final String               rootNode;
    private final String               basePath;
    private final ServiceEnvironment   environment;
    private final String               region;
    private final String               ensemble; 
    private final boolean              hosted;
    private final Map<ZkaAttr, String> attributes;
    private final DefaultACLProvider   aclProvider;
    private volatile boolean           selfDestructing;
    private volatile boolean           closed = false;
    
	
    
    
    private static final BackgroundCallback ERROR_LOGGING_CALLBACK = (client, event) -> {
        if (event.getResultCode() != 0) { 
            Code result = KeeperException.Code.get(event.getResultCode());
            log.warn("Operation {} failed for path {} with error code {}", event.getType(), event.getPath(), result);
        }
    };
    private final CuratorListener authFailedListener = (client, event) -> {
        
        
        
        if (event.getType() == CuratorEventType.WATCHED) {
            if (event.getWatchedEvent().getState() == KeeperState.AuthFailed) {
                
                if(closed) {
                    log.error("#{} Authentication failed. Already closed: {}.", this.id, getClass().getSimpleName());
                } else {
                    log.error("#{} Authentication failed. Closing this {}.", this.id, getClass().getSimpleName());
                    this.close(); 
                }
            }
        }
    };
    public ZkaPathContext(ZkaConfig config) {
        this(config, null);
    }
    public ZkaPathContext(ZkaConfig config, @Nullable CuratorFramework curator) {
        ParsedPath path = ZkaResourceContext.validatePath(config.getPath());
        this.rootNode    = path.rootNode();
        this.basePath    = path.basePath();
        this.attributes  = Maps.immutableEnumMap(config.getAttributes());
		this.hosted = config.isHosted();
		this.ensemble = config.getEnsemble();
		if (isHosted()) {
			this.environment = Objects.requireNonNull(config.getEnvironment(), "Environment must be set.");
			this.region = Objects.requireNonNull(config.getRegion(), "Region must be set.");
			log.info("Using environment {} with basePath {} and attributes {}", this.environment, path,
					this.attributes);
		} else {
			this.environment = null;
			this.region = null;
			log.info("Using ensemble {} with basePath {} and attributes {}", this.ensemble, path,
					this.attributes);
		}
        if (curator != null) {
            this.resourceContext = null;
            this.curator = curator;
            
            if (this.curator.getState() == CuratorFrameworkState.LATENT) {
                this.curator.start();
            }
            this.aclProvider = withCurator(new DefaultACLProvider(
                ZkaResourceContext.validatePath(config.getPath()).basePath(),
                config.getInteger(ZkaAttr.CONNECTION_TIMEOUT)), this.curator);
        } else {
            DefaultACLProvider aclp = new DefaultACLProvider(this.basePath, this.getConnectionTimeout());
            this.resourceContext = ZkaResourceContext.getResourceContext(this, aclp); 
            
            this.curator = resourceContext.getCurator();
            if (resourceContext.getAclProvider() == aclp) {
                this.aclProvider = aclp;
            } else {
                this.aclProvider = withCurator(
                    new DefaultACLProvider(ZkaResourceContext.validatePath(config.getPath()).basePath(),
                    config.getInteger(ZkaAttr.CONNECTION_TIMEOUT)), this.curator);
            }
        }
        this.curator.getCuratorListenable().addListener(authFailedListener);
        this.aclProvider.getDefaultAcl();
    }
    
    
    public static ZkaPathContext contextForSubPath(ZkaPathContext context, String subPath) {
        return new ZkaPathContext(context, ZKPaths.makePath(context.basePath, subPath));
    }
    protected ZkaPathContext(ZkaPathContext context, String basePath) {
        
        synchronized (context.references) {
            if (closed)
                throw new IllegalArgumentException("The provided context is already closed! (" + context + ")" );
            
			this.hosted = context.hosted;
			this.ensemble = context.ensemble;
            this.curator = context.curator;
            this.rootNode = context.rootNode;
            this.basePath = basePath;
            this.environment = context.environment;
            this.region = context.region;
            this.attributes = context.attributes;
            this.resourceContext = context.resourceContext;
            if (resourceContext != null) {
                resourceContext.register(this);
                if (resourceContext.getAclProvider() != null)
                    resourceContext.getAclProvider().getDefaultAcl();
            }
            this.aclProvider = withCurator(new DefaultACLProvider(basePath, getInteger(ZkaAttr.CONNECTION_TIMEOUT)),
                this.curator);
            this.aclProvider.getDefaultAcl();
        }
    }
    private static DefaultACLProvider withCurator(DefaultACLProvider aclProvider, CuratorFramework curator) {
        aclProvider.curator(curator);
        return aclProvider;
    }
    @Internal
    protected final ZkaResourceContext zkaResourceContext() { return resourceContext; }
    
    
    public final void setCreateAcl(List<ACL> defaultAcl) {
        this.aclProvider.setDefaultAcl(defaultAcl);
    }
    
    public final List<ACL> getCreateAcl() {
        return this.aclProvider.getDefaultAcl();
    }
    
    public final boolean setSharedDefaultAcl(List<ACL> defaultAcl) {
        if (resourceContext == null) {
            log.warn("#{}: Context is created from a Curator instance and does not support setting default ACL [{}]", id, defaultAcl);
            return false;
        } else if (!(resourceContext.getAclProvider() instanceof DefaultACLProvider)) {
            log.warn("#{}: ACL provider [{}] does not support setting default ACL [{}]", id, resourceContext.getAclProvider(), defaultAcl);
            return false;
        } else {
            ((DefaultACLProvider) resourceContext.getAclProvider()).setDefaultAcl(defaultAcl);
            return true;
        }
    }
    
    
    public final Optional<List<ACL>> getSharedDefaultAcl() {
        if (resourceContext == null) {
            return Optional.empty();
        } else if (!(resourceContext.getAclProvider() instanceof DefaultACLProvider)) {
            return Optional.empty();
        } else {
            return Optional.of(((DefaultACLProvider) resourceContext.getAclProvider()).getDefaultAcl());
        }
    }
    
    public boolean isSelfDestructing() {
        return selfDestructing;
    }
    
    
    public void setSelfDestructing(boolean selfDestructing) {
        this.selfDestructing = selfDestructing;
    }
    
    public static ZkaPathContext selfDestructing(ZkaConfig cfg) {
        ZkaPathContext c = new ZkaPathContext(cfg);
        c.setSelfDestructing(true);
        return c;
    } 
    public static ZkaPathContext selfDestructing(ZkaConfig cfg, CuratorFramework curator) {
        ZkaPathContext c = new ZkaPathContext(cfg, curator);
        c.setSelfDestructing(true);
        return c;
    } 
    public final String getRootNode() {
        return rootNode;
    }
    
	public final boolean isHosted() {
		return hosted;
	}
	public final String getEnsemble() {
		return ensemble;
	}
    
    @GuardedBy("references")
    @Override
    public final void close() {
        synchronized(references) {
            if (closed)
                return;
            log.info("closing #{}", this);
            
            this.curator.getCuratorListenable().removeListener(authFailedListener);
            
            for (final Closeable closeable : references) {
                CloseableUtils.closeQuietly(closeable);
            }
            if (resourceContext != null) {
                
                resourceContext.unregister(this);
            }
            references.clear();
            closed = true;
            log.debug("#{} closed", id);
        }
    }
    
    public final String getBasePath() {
        return basePath;
    }
    
    public final boolean isClosed() {
        return closed;
    }
    public final CuratorFramework getCurator() {
        return curator;
    }
    
    @GuardedBy("references")
    public final void register(Closeable closeable) {
        synchronized (references) {
            if (closed) {
                throw new IllegalStateException(getClass().getSimpleName() + "#" + id + " already closed");
            }
            
            if (closeable instanceof ConnectionStateListener) {
                curator.getConnectionStateListenable().addListener((ConnectionStateListener) closeable);
            }
            references.add(closeable);
        }
    }
    
    
    @GuardedBy("references")
    public final void unregister(Closeable o) {
        if (o instanceof ConnectionStateListener) {
            curator.getConnectionStateListenable().removeListener((ConnectionStateListener) o);
        }
        synchronized(references) {
            if (references.remove(o) && selfDestructing && references.isEmpty()) {
                CloseableUtils.closeQuietly(this);
            }
        }
    }
    
    public final boolean isConnected() {
        return curator.getZookeeperClient().isConnected();
    }
    
    public final String paths(String... paths) {
        if (paths.length == 0) {
            return basePath;
        }
        
        if (paths.length == 1) {
            return resolve(paths[0]);
        }
        
        
        return ZKPaths.makePath(basePath, paths[0], Arrays.copyOfRange(paths, 1, paths.length));
    }
    
    public final String resolve(String childPath) {
        return (childPath == null || childPath.isEmpty()) ? basePath : ZKPaths.makePath(basePath, childPath);
    }
    
    public final @Nullable Stat exists(String childPath) throws IOException {
        return forChildPath(curator.checkExists(), childPath);
    }
    
    @ZkaSyncOperation
    public final boolean create(byte[] data, String childPath) throws IOException {
        return create(data, false, childPath);
    }
    @ZkaSyncOperation
    
    public final boolean create(byte[] data, boolean createParents, String childPath) throws IOException {
        return create(data, getCreateAcl(), createParents, childPath);
    }
    
    
    public final boolean create(byte[] data, List<ACL> acls, boolean createParents, String childPath) throws IOException {
        ACLBackgroundPathAndBytesable<String> cmd = createParents ? curator.create().creatingParentsIfNeeded() : curator.create();
        try {
            cmd.withACL(acls, createParents).forPath(resolve(childPath), data);
            return true;
        } catch (KeeperException.NodeExistsException e) {
            return false;
        } catch (IOException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
    
    @ZkaAsyncOperation
    public final void createInBackground(byte[] data, boolean createParents, String childPath) {
        PathAndBytesable<String> cmd = (createParents ? curator.create().creatingParentsIfNeeded() : curator.create())
                .withACL(getCreateAcl(), createParents).inBackground(ERROR_LOGGING_CALLBACK);
        forChildPath(cmd, data, childPath, TO_RUNTIME_EXCEPTION);
    }
    
    @ZkaSyncOperation
    public final Stat update(byte[] data, String childPath) throws IOException {
        return forChildPath(curator.setData(), data, childPath);
    }
    @ZkaSyncOperation
    public final void delete(String childPath) throws IOException {
        forChildPath(curator.delete().deletingChildrenIfNeeded(), childPath);
    }
    @ZkaAsyncOperation
    public final void deleteInBackground(String childPath) throws IOException {
        forChildPath(curator.delete().deletingChildrenIfNeeded().inBackground(), childPath);
    }
    
    
    @ZkaSyncOperation
    public final byte[] getNodeData(String childPath) throws IOException {
        return forChildPath(curator.getData(), childPath);
    }
    @ZkaSyncOperation
    public final List<ACL> getACL(String childPath) throws IOException {
        return forChildPath(curator.getACL(), childPath);
    }
    
    @ZkaSyncOperation
    public final List<String> getChildren(String childPath) throws IOException {
        return forChildPath(curator.getChildren(), childPath);
    }
    @ZkaSyncOperation
    public final <D> Map<String, D> getChildrenData(String basePath, Function<byte[], D> mapper) throws IOException {
        List<String> children = getChildren(basePath);
        Map<String, D> map = new HashMap<>(children.size());
        for (String child : children) {
            String p = ZKPaths.makePath(basePath, child);
            log.debug("getting info for child path {}", p);
            try {
                byte[] nodeData = getNodeData(p);
                if (nodeData != null)
                    map.put(child, mapper.apply(nodeData));
            } catch (Exception e) {
                log.debug("failed to acquire data for childPath " + p, e);
            }
        }
        return map;
    }
    
    public final long getNegotiatedSessionTimeout() {
        try {
            return getCurator().getZookeeperClient().getZooKeeper().getSessionTimeout();
        } catch (final Exception e) {
            return getInteger(ZkaAttr.SESSION_TIMEOUT);
        }
    }
    
    public final long getSessionId() {
        try {
            return getCurator().getZookeeperClient().getZooKeeper().getSessionId();
        } catch (final Exception e) {
            return 0;
        }
    }
    public final boolean getBoolean(ZkaAttr attr) {
        String v = attributes.get(attr);
        return v == null ? (Boolean) attr.getDefaultValue() : Boolean.parseBoolean(v);
    }
    public final int getInteger(ZkaAttr attr) {
        String v = attributes.get(attr);
        return v == null ? (Integer) attr.getDefaultValue() : Integer.parseInt(v);
    }
    public final String getAttribute(ZkaAttr attr) {
        String v = attributes.get(attr);
        return v == null ? (String) attr.getDefaultValue() : v;
    }
    public final void setACL(List<ACL> acls, String childPath) throws IOException {
        forChildPath(curator.setACL().withACL(acls), childPath);
    }
    public final ServiceEnvironment getEnvironment() {
        return environment;
    }
    public final String getRegion() {
        return region;
    }
    public final boolean isJailed() {
        return getBoolean(ZkaAttr.JAIL);
    }
    
    public final int getConnectionTimeout() {
        return getInteger(ZkaAttr.CONNECTION_TIMEOUT);
    }
    
    public final int getDesiredSessionTimeout() {
        return getInteger(ZkaAttr.SESSION_TIMEOUT);
    }
    @Override
    public String toString() {
        if (isHosted()) {
            return String.format("%s #%d [region=%s,environment=%s]", getClass().getSimpleName(), id, getRegion(), getEnvironment());
        } else {
            return String.format("%s #%d non-hosted [ensemble=%s]", getClass().getSimpleName(), id, getEnsemble());
        }
    }
    
    
    public final String toServiceURI(String scheme, String serviceName) {
        StringBuilder bld = new StringBuilder().append(scheme).append(":
                .append(getBasePath());
        
        if (serviceName != null && !serviceName.isEmpty())
            bld.append("?service=").append(serviceName);
        return bld.toString();
    }
    private <T> T forChildPath(Pathable<T> cmd, String childPath) throws IOException {
        try {
            return cmd.forPath(resolve(childPath));
        } catch (final IOException | RuntimeException e) {
            throw e;
        } catch (final Exception e) {
            throw new IOException(e);
        }
    }
    private <T> T forChildPath(PathAndBytesable<T> cmd, byte[] bytes, String childPath) throws IOException {
        return forChildPath(cmd, bytes, childPath, TO_IO_EXEPTION);
    }
    private <T, E extends Exception> T forChildPath(PathAndBytesable<T> cmd, byte[] bytes, String childPath, ErrorMapper<E> mapper) throws E {
        try {
            return cmd.forPath(resolve(childPath), bytes);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw mapper.apply(e);
        }
    }
    
    private static final ErrorMapper<IOException> TO_IO_EXEPTION =  e -> e instanceof IOException ? (IOException) e : new IOException(e); 
    private static final ErrorMapper<RuntimeException> TO_RUNTIME_EXCEPTION =  e -> e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e); 
    
    private interface ErrorMapper<E extends Exception> {
        E apply(Exception e);
    }
    
}
