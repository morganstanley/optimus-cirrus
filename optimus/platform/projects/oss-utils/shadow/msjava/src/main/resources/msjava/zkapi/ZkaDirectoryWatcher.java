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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import msjava.base.slf4j.ContextLogger;
import msjava.zkapi.internal.WaitForPath;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.CloseableExecutorService;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
@ThreadSafe
public  class ZkaDirectoryWatcher implements Closeable {
    
    
    private static final Logger                   log         = ContextLogger.safeLogger();
    private final ZkaPathContext                  context;
    private final PathChildrenCache               cache;
    
    private final CloseableExecutorService        cacheExecutorService;
    @Nullable
    private final Closeable                       waitForPath;
    private final CopyOnWriteArrayList<PathChildrenCacheListener> listeners = new CopyOnWriteArrayList<>();
    
    
    private final CountDownLatch            initialized = new CountDownLatch(1);
    
    
    
    private final Object                    closeLock   = new Object();
    @GuardedBy("closeLock")
    private volatile boolean                closed      = false;
    private final PathChildrenCacheListener dwListener  = new PathChildrenCacheListener() {
        
        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
            
            
            
            if (log.isDebugEnabled()) {
                log.debug(repr(event));
            }
            if (event.getType() == Type.INITIALIZED) {
                initialized.countDown();
            }
            synchronized (closeLock) {
                
                
                if (closed) return;
                
                for (PathChildrenCacheListener listener : listeners) {
                    listener.childEvent(client, event);
                }
            }
        }
    };
    public enum Mode {
    	CREATE_PATH {
			@Override
			Closeable run(ZkaPathContext context, String path, Runnable starter) {
				starter.run();
				return null;
			}
		},
    	WAIT_FOR_PATH {
			@Override
			Closeable run(ZkaPathContext context, String path, Runnable starter) {
				return WaitForPath.awaitInBackground(context, path, starter);
			}
		};
    	abstract @Nullable Closeable run(ZkaPathContext context, String path, Runnable starter);
    }
    
    public ZkaDirectoryWatcher(ZkaConfig config, String path) {
        this(ZkaPathContext.selfDestructing(config), path);
    }
    
    public ZkaDirectoryWatcher(ZkaPathContext context, String path) {
        this(context, path, null, Mode.WAIT_FOR_PATH);
    }
    
    
    public ZkaDirectoryWatcher(ZkaConfig config, String path, @Nullable PathChildrenCacheListener listener) {
        this(ZkaPathContext.selfDestructing(config), path, listener);
    }
    
    public ZkaDirectoryWatcher(ZkaPathContext context, String path, @Nullable PathChildrenCacheListener listener) {
        this(context, path, listener, Mode.WAIT_FOR_PATH);
    }
    
    public ZkaDirectoryWatcher(ZkaConfig config, String path, @Nullable PathChildrenCacheListener listener, Mode startMode) {
        this(ZkaPathContext.selfDestructing(config), path, listener, startMode);
    }
    
    public ZkaDirectoryWatcher(ZkaPathContext context, String path, @Nullable PathChildrenCacheListener listener, Mode startMode) {
        this.context = context;
        String fullPath = context.resolve(path);
        this.cacheExecutorService = new CloseableExecutorService(Executors.newSingleThreadExecutor(PathChildrenCache.defaultThreadFactorySupplier.get()), true);
        this.cache = new PathChildrenCache(context.getCurator(), fullPath, true, false, cacheExecutorService);
        this.cache.getListenable().addListener(dwListener);
        if(listener != null) {
            listeners.add(listener);
        }
        context.register(this);
    	waitForPath = startMode.run(context, path, () -> {
            try {
                log.info("Listening to path {}", fullPath);
                cache.start(StartMode.POST_INITIALIZED_EVENT);
            } catch (Exception e) {
                log.error("Error starting directory watcher for path " + fullPath, e);
                Throwables.throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
        });
    }
    private static String repr(PathChildrenCacheEvent event) {
        return "PathChildrenCacheEvent{type=" + event.getType() + ", data=" + childDataToString(event.getData()) + ", intialData = " + childDataToString(event.getInitialData()) +"}";
    }
    private static String simpleChildDataToString(ChildData cd) {
        if (cd == null)
            return "null";
        else
            return "path = " + cd.getPath() + ", data = " + asString(cd.getData());
    }
    
    private static String asString(byte[] bytes0) {
        byte[] bytes = bytes0.clone();
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] < ' ') {
                bytes[i] = '?';
            }
        }
        return new String(bytes, StandardCharsets.ISO_8859_1);
    }
    private static String childDataToString(ChildData cd) {
        return "ChildData{" + simpleChildDataToString(cd) + "}";
    }
    private static String childDataToString(List<ChildData> cd) {
        if (cd == null || cd.isEmpty()) return "ChildData{}";
        List<String> asString = new ArrayList<>();
        for(ChildData d : cd) { asString.add(simpleChildDataToString(d)); }
        return "ChildData{" + Joiner.on(";").join(asString) + "}";
    }
    @Override
    public final void close() throws IOException {
        synchronized (closeLock) {
            if (closed) return;
            log.debug("Closing ZkaDirectoryWatcher {}", context.toString());
            closed = true;
            CloseableUtils.closeQuietly(waitForPath);
            listeners.clear();
            try {
                cache.getListenable().removeListener(dwListener);
                cacheExecutorService.close();
                cache.close();
            } finally {
                context.unregister(this);
            }
        }
    }
    
    public final List<ChildData> getCurrentData() {
        return cache.getCurrentData();
    }
    
    public final boolean waitForInitialization() {
        return waitForInitialization(0);
    }
    
    public final boolean waitForInitialization(long timeout) {
        try {
            if (timeout == 0) {
                initialized.await();
                return true;
            } else {
                return initialized.await(timeout, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            
            return isInitialized();
        }
    }
    
    public final boolean isInitialized() {
        return initialized.getCount() == 0;
    }
    
    public final void subscribe(PathChildrenCacheListener listener) {
        listeners.addIfAbsent(Objects.requireNonNull(listener, "Listener cannot be null!"));
    }
    
    public final void unsubscribe(PathChildrenCacheListener listener) {
        listeners.remove(Objects.requireNonNull(listener, "Listener cannot be null!"));
    }
}
