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

package msjava.zkapi.pubsub;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import msjava.base.annotation.Internal;
import msjava.base.slf4j.ContextLogger;
import msjava.base.slr.internal.NotificationPolicy;
import msjava.base.slr.internal.NotificationPolicy.NotificationCallback;
import msjava.base.slr.internal.NotificationPolicy.NotificationNextStep;
import msjava.base.slr.internal.ServiceLocatorClient;
import msjava.base.slr.internal.ServiceLocatorClient.ServiceLocatorListener;
import msjava.base.slr.internal.ServiceLookup;
import msjava.base.sr.ServiceDescription;
import msjava.zkapi.ZkaAttr;
import msjava.zkapi.ZkaDirectoryWatcher;
import msjava.zkapi.ZkaPathContext;
import msjava.zkapi.internal.ZkaContext;
import msjava.zkapi.internal.ZkaOverrides;
import msjava.zkapi.internal.ZkaData;
import msjava.zkapi.internal.ZkaServiceDescription;
import msjava.zkapi.sr.DefaultNotificationCallback;
import msjava.zkapi.support.ZkaUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
@Internal
public final class ZkaServiceSubscriber implements ServiceLocatorClient, PathChildrenCacheListener, ConnectionStateListener,
        ServiceLocatorListener {
    private static final Logger                                log            = ContextLogger.safeLogger();
    private final ZkaPathContext                               context;
    private final ZkaDirectoryWatcher                          watcher;
    private final CopyOnWriteArrayList<ServiceLocatorListener> listeners      = new CopyOnWriteArrayList<>();
    private final String                                       service;
    private final String                                       uri;      
    private volatile boolean                                   closed         = false;
    private NotificationCallback                               policyCallback = new DefaultNotificationCallback();
    
    
    public ZkaServiceSubscriber(ZkaContext context) throws IOException {
        this((ZkaPathContext) context);
    }
    
    public ZkaServiceSubscriber(ZkaPathContext context) {
        this.context = context;
        this.service = context.getAttribute(ZkaAttr.SERVICE);
        this.uri     = context.toServiceURI("zps", service);
        this.watcher = new ZkaDirectoryWatcher(context, "", this);
        
        context.register(this);
        ZkaOverrides.instance().registerSubscriber(this.context, this);
    }
    
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        log.debug("closing for uri {}", uri);
        closed = true;
        CloseableUtils.closeQuietly(watcher);
        ZkaOverrides.instance().unregisterSubscriber(this.context, this);
        context.unregister(this);
    }
    public boolean waitForInit(long duration, TimeUnit timeUnit) {
    	
    	if (Thread.currentThread().getName().startsWith("PathChildrenCache")) {
    		return watcher.isInitialized();
    	} else {
    		return duration < 0 ? 
				watcher.waitForInitialization(context.getConnectionTimeout()):
				watcher.waitForInitialization(TimeUnit.MILLISECONDS.convert(duration,  timeUnit));
				
    	}
    }
    
    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        switch (newState) {
        case CONNECTED:
        case RECONNECTED:
            ping(EventType.CONNECT);
            break;
        case LOST:
        case SUSPENDED:
            ping(EventType.DISCONNECT);
            break;
        default:
            break;
        }
    }
    
    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        switch (event.getType()) {
        case INITIALIZED:
            ping(EventType.UPDATE);
            break;
        case CHILD_ADDED:
        case CHILD_UPDATED:
        case CHILD_REMOVED:
            if (watcher.isInitialized() && matches(event.getData())) {
                ping(EventType.UPDATE);
            }
            break;
        default:
            break;
        }
    }
    private void ping(EventType update) {
    	
    	log.info("{} event {} occured", uri, update);
        NotificationNextStep nextStep = policyCallback.onEvent(update);
        nextStep.invoke(this, update);
    }
    private boolean matches(ChildData data) {
    	byte[] bytes = data.getData();
    	if (bytes == null) 
    	    return false;
        return ZkaUtils.serviceMatches(ZkaData.fromBytes(bytes), service);
    }
    
    @Override
    public Set<ServiceDescription> getServices() {
        return getServices(service);
    }
    public Set<ServiceDescription> getServices(String service) {
        Set<ServiceDescription> found = new HashSet<>();
        ZkaOverrides.ServiceRecord record = ZkaOverrides.instance().getServiceRecord(context, service);
        Optional<ServiceDescription> override = record.getOverride(context);
        if (override.isPresent()) {
            log.debug("Using override from service record: {}", record);
            return Collections.singleton(override.get());
        }
        
        if (!waitForInit(context.getConnectionTimeout(), TimeUnit.MILLISECONDS)) {
            log.error("Subscriber not initialized within {} ms. "
                    + "Please ensure that the given path exists and you have read access! Possible reasons are \n"
                    + "(1) The provided path doesn't exists. \n" 
                    + "(2) Don't have read access on the provided node.\n",
                    context.getConnectionTimeout());
            return found;
        }
        try {
            for (ChildData cd : watcher.getCurrentData()) {
                try {
                    ZkaData zd = ZkaData.fromBytes(cd.getData());
                    if (ZkaUtils.serviceMatches(zd, service)) {
                        found.add(new ZkaServiceDescription(zd));
                    }
                } catch (Exception e) {
                    log.error("could not deserialize a published service", e);
                }
            }
        } catch (Throwable t) {
            log.error("failed to generally acquire any published services", t);
        }
        
        record.update(found);
        log.trace("Found services: {}", found);
        return found;
    }
    
    @Override
    public void addListener(ServiceLocatorListener l) {
        if (listeners.addIfAbsent(l) && watcher.isInitialized()) {
	        l.changeOccured(EventType.UPDATE);
        }
    }
    
    @Override
    public void removeListener(ServiceLocatorListener l) {
        listeners.remove(l);
    }
    public void setNotificationPolicy(NotificationPolicy policy) {
        policyCallback = policy.createCallback(this);
    }
    @Override
    protected void finalize() throws Throwable {
        if (closed) {
            return;
        }
        log.warn("ZkaServiceSubscriber is not closed, closing in finalize method");
        close();
    }
    
    @Override
    public void changeOccured(EventType eventType) {
        for (ServiceLocatorListener l : listeners) {
            l.changeOccured(eventType);
        }
    }
    
    public Set<ServiceDescription> getServices(ServiceLookup lookup) {
        return getServices(lookup.getName());
    }
}
