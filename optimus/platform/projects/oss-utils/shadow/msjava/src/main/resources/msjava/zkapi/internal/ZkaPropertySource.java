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
import static java.util.Collections.singletonMap;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import msjava.base.slf4j.ContextLogger;
import msjava.zkapi.PropertySource;
import msjava.zkapi.ZkaAttr;
import msjava.zkapi.ZkaPathContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException.Code;
import org.slf4j.Logger;
import com.google.common.annotations.VisibleForTesting;
import com.netflix.config.ConfigurationManager;
import com.netflix.config.DynamicWatchedConfiguration;
import com.netflix.config.WatchedConfigurationSource;
import com.netflix.config.WatchedUpdateListener;
import com.netflix.config.WatchedUpdateResult;
public final class ZkaPropertySource implements PropertySource, WatchedConfigurationSource {
    @GuardedBy("installedNamespaces")
    private static DynamicWatchedConfiguration config;
    private static void addSource(ZkaPropertySource source) {
        synchronized(installedNamespaces) {
            installedNamespaces.add(source.namespace);
            if (config == null) {
                config = new DynamicWatchedConfiguration(source);
                ConfigurationManager.install(config);
            } else {
                source.addUpdateListener(config);
            }
        }
    }
    public static boolean isNamespaceInstalled(String namespace) {
        synchronized (installedNamespaces) {
            return installedNamespaces.contains(namespace);
        }
    }
    @GuardedBy("installedNamespaces")
    private static final Set<String>          installedNamespaces    = new HashSet<>();
    
    private static final Logger               log                    = ContextLogger.safeLogger();
    
    private static final String               ZKADATA_VALUE          = "value";
    private static final String               NAMESPACE_SEPARATOR    = ":";
    
    private static final Map<String, Object>  emptyMap               = Collections.emptyMap();
    private final msjava.zkapi.ZkaDirectoryWatcher watcher;
    private final ZkaPathContext              context;
    private final CopyOnWriteArrayList<WatchedUpdateListener> listeners = new CopyOnWriteArrayList<WatchedUpdateListener>();
    private final Map<String, Object>         properties                = new ConcurrentHashMap<String, Object>();
    private final String                      namespace;
    private final PathChildrenCacheListener propertySourceEventListener = new PathChildrenCacheListener() {
        
        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
            
            
            if (event.getData() == null) {
                return;
            }
            final String key = convertPathToKey(event.getData().getPath());
            final Object value = getValue(ZkaData.fromBytes(event.getData().getData()));
            properties.put(key, value);
            handleEvent(event.getType(), key, value);
        }
    };
    public ZkaPropertySource(@Nullable String namespace, ZkaContext context) throws IOException {
        this(context, namespace);
    }
    
    
    public ZkaPropertySource(ZkaPathContext context, @Nullable String namespace) throws IOException {
    	this.context = Objects.requireNonNull(context, "Context cannot be null");
        this.namespace = StringUtils.defaultIfBlank(namespace, "");
        watcher = new msjava.zkapi.ZkaDirectoryWatcher(context, "", propertySourceEventListener, msjava.zkapi.ZkaDirectoryWatcher.Mode.CREATE_PATH);
        watcher.waitForInitialization(context.getConnectionTimeout());
        context.register(this);
        addSource(this);
    }
    @VisibleForTesting
    ZkaPropertySource(ZkaPathContext context, msjava.zkapi.ZkaDirectoryWatcher watcher) {
        this.context = context;
        this.namespace = "";
        this.watcher = watcher;
        this.watcher.subscribe(propertySourceEventListener);
        this.watcher.waitForInitialization(context.getConnectionTimeout());
        context.register(this);
        addSource(this);
    }
    @Override
    public void close() throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("Closing ZkaPropertySource for context {}", context.toServiceURI("zpm", null));
        }
        try {
            this.watcher.close();
        } catch (final IOException e) {
            log.error("error closing watcher", e);
        } finally {
            this.context.unregister(this);
            this.listeners.clear();
            synchronized (installedNamespaces) {
                installedNamespaces.remove(this.namespace);    
            }
        }
    }
    
    @Override
    public void addUpdateListener(WatchedUpdateListener l) {
        log.debug("listener added {}", l);
        if (listeners.addIfAbsent(l)) {
            
            l.updateConfiguration(WatchedUpdateResult.createIncremental(properties, emptyMap, emptyMap));
        }
    }
    
    @Override
    public void removeUpdateListener(WatchedUpdateListener l) {
        listeners.remove(l);
    }
    
    @Override
    public Map<String, Object> getCurrentData() throws Exception {
        ensureInitialized();
        final Map<String, Object> props = new HashMap<String, Object>();
        
        
        for (ChildData cd : watcher.getCurrentData()) {
            String key = convertPathToKey(cd.getPath());
            Object value = getValue(new ZkaData(cd));
            props.put(key, value);
        }
        return props;
    }
    private void ensureInitialized() throws TimeoutException {
        if (!watcher.waitForInitialization(context.getInteger(ZkaAttr.CONNECTION_TIMEOUT))) {
            throw new TimeoutException("Initialization timed out");
        }
    }
    
    @Override
    public Object getProperty(String key) {
        return properties.get(key);
    }
    
    
    @Override
    public void setProperty(String key, String value) {
        final ZkaData zd = new ZkaData();
        zd.put(ZKADATA_VALUE, value);
        key = removeNamespace(key);
        
        
        
        
        
        
        setDataInBackground(context, zd, key);
    }
    private static void setDataInBackground(ZkaPathContext context, final ZkaData zd, final String path) {
        try {
            context.getCurator().setData().inBackground(SET_DATA_CALLBACK, new SetDataContext(context, zd, path)).forPath(context.paths(path), zd.serialize());
        } catch (Exception e) {
            log.warn("Exception while issueing background operation. Probably programming error.", e);
            assert false; 
        }
    }
    private static final class SetDataContext {
        final ZkaPathContext context;
        final ZkaData    data;
        final String     path;
        SetDataContext(ZkaPathContext context, ZkaData data, String path) {
            this.context = context;
            this.data    = data;
            this.path    = path;
        }
    }
    private static final BackgroundCallback SET_DATA_CALLBACK = new BackgroundCallback() {
        @Override
        public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
            SetDataContext ctx = (SetDataContext)event.getContext();
            assert ctx != null;
            Code code = Code.get(event.getResultCode());
            assert code != null;
            switch (code) {
            case OK:
                log.debug("Value for attribute {} successfully updated.", ctx.path);
                break;
            case NONODE:
                log.debug("Attribute {} doesn't exist yet. About to create.", ctx.path);
                ctx.context.createInBackground(ctx.data.serialize(), true, ctx.path);
                break;
            default:
                log.warn("An error '{}' while setting value for key {}", code, ctx.path);
                break;
            }
        }
    };
    
    @Override
    public void setProperty(String key, Object value) throws IOException {
        setProperty(key, value.toString());
    }
    @GuardedBy("watcher") 
    private void handleEvent(PathChildrenCacheEvent.Type eventType, String key, Object value) {
        switch (eventType) {
        case CHILD_ADDED:
            fire(singletonMap(key, value), emptyMap, emptyMap);
            break;
        case CHILD_REMOVED:
            fire(emptyMap, emptyMap, singletonMap(key, value));
            break;
        case CHILD_UPDATED:
            fire(emptyMap, singletonMap(key, value), emptyMap);
            break;
        case CONNECTION_LOST:
        case CONNECTION_RECONNECTED:
        case CONNECTION_SUSPENDED:
        case INITIALIZED:
            break;
        default:
            break;
        }
    }
    @GuardedBy("watcher") 
    private void fire(Map<String, Object> added, Map<String, Object> changed, Map<String, Object> deleted) {
        final WatchedUpdateResult wur = WatchedUpdateResult.createIncremental(added, changed, deleted);
        for (final WatchedUpdateListener l : listeners) {
            try {
                
                if (log.isDebugEnabled()) {
                    log.debug("updating listener {} with results added {} changed {} deleted {}", l, added, changed,
                        deleted);
                }
                l.updateConfiguration(wur);
            } catch (final Throwable t) {
                log.error("failed to update configuration for listener {}", l, t);
            }
        }
    }
    private String convertPathToKey(String path) {
        String node = ZKPaths.getNodeFromPath(path);
        return uncheckedPrefixKey(namespace, node);
    }
    
    private static Object getValue(ZkaData data) {
        Object value = data.get(ZKADATA_VALUE);
        return value == null ? "" : value;
    }
    
    
    private static String removeNamespace(String key) {
        int sep_index = key.indexOf(NAMESPACE_SEPARATOR);
        if (-1 < sep_index && sep_index + 1 < key.length()) {
            return key.substring(sep_index + 1);
        }
        return key;
    }
    
    public static String prefixKey(@Nonnull String namespace, String key) {
        if (key.contains(NAMESPACE_SEPARATOR)) {
            if (!key.startsWith(namespace + NAMESPACE_SEPARATOR)) {
                log.warn("The key '{}' is already prefixed with a different namespace from '{}'", key, namespace);
            }
            return key;
        }
        return uncheckedPrefixKey(namespace, key);
    }
    private static String uncheckedPrefixKey(@Nonnull String namespace, String key) {
        if (namespace.length() != 0) {
            return namespace + NAMESPACE_SEPARATOR + key;
        }
        return key;
    }
}