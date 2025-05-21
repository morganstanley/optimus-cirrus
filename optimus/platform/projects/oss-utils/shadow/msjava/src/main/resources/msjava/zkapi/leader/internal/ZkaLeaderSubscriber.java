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

package msjava.zkapi.leader.internal;
import static java.util.stream.Collectors.toSet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.GuardedBy;
import msjava.base.slf4j.ContextLogger;
import msjava.base.slr.internal.NotificationPolicy;
import msjava.base.slr.internal.NotificationPolicy.NotificationCallback;
import msjava.base.slr.internal.NotificationPolicy.NotificationNextStep;
import msjava.base.sr.ServiceDescription;
import msjava.base.slr.internal.ServiceLocatorClient.ServiceLocatorListener;
import msjava.zkapi.ZkaAttr;
import msjava.zkapi.ZkaDirectoryWatcher;
import msjava.zkapi.ZkaPathContext;
import msjava.zkapi.internal.ZkaContext;
import msjava.zkapi.internal.ZkaData;
import msjava.zkapi.internal.ZkaSemaphore;
import msjava.zkapi.internal.ZkaServiceDescription;
import msjava.zkapi.internal.ZkaSharedCount;
import msjava.zkapi.leader.LeaderSubscriber;
import msjava.zkapi.leader.LeaderSubscriberListener;
import msjava.zkapi.sr.DefaultNotificationCallback;
import msjava.zkapi.support.ZkaUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.shared.SharedCountListener;
import org.apache.curator.framework.recipes.shared.SharedCountReader;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import com.google.common.base.Preconditions;
public final class ZkaLeaderSubscriber implements LeaderSubscriber, PathChildrenCacheListener, SharedCountListener,
        ServiceLocatorListener {
    
    private static final Logger logger = ContextLogger.safeLogger();
    private final ZkaPathContext          context;
    
    private ZkaSharedCount                maxLeaders;
    
    private final Set<LeaderSubscriberListener> listeners = new HashSet<>();
    
    private ZkaDirectoryWatcher           candidatesWatcher;
    
    private ZkaDirectoryWatcher           publicationsWatcher;
    
    private ZkaDirectoryWatcher           leasesWatcher;
    @GuardedBy("cacheLock")
    private final Map<Long, LeaderData>   candidateCache = new HashMap<>();
    @GuardedBy("cacheLock")
    private Set<Long>                     currentLeaders = new HashSet<>();
    private final Object                  cacheLock      = new Object();
    private volatile boolean              closed         = false;
    private final String                  service;
    private NotificationCallback          policyCallback = new DefaultNotificationCallback();
    private final CopyOnWriteArrayList<ServiceLocatorListener> locatorListeners = new CopyOnWriteArrayList<>();
    
    private final CountDownLatch          initialized    = new CountDownLatch(1);
    public ZkaLeaderSubscriber(ZkaContext context,LeaderSubscriberListener listener, NotificationPolicy policy) throws IOException {
        this((ZkaPathContext) context, listener, policy);
    }
    
    public ZkaLeaderSubscriber(ZkaPathContext context,LeaderSubscriberListener listener, NotificationPolicy policy) throws IOException {
        if (policy != null) {
        	setNotificationPolicy(policy);
        }
        
    	this.context = context;
        context.register(this);
        if (listener != null) {
        	this.listeners.add(listener);
        }
        
        start();
        
        service = context.getAttribute(ZkaAttr.SERVICE);
    }
    
    
    private void start() throws IOException {
        try {
            maxLeaders = new ZkaSharedCount(context, ZkaLeaderElector.LEADER_COUNT_PATH, 1);
            candidatesWatcher = new ZkaDirectoryWatcher(context, ZkaLeaderElector.CANDIDATE_PATH, this);
            leasesWatcher = new ZkaDirectoryWatcher(context, ZkaSemaphore.LEASES_PATH, this);
            publicationsWatcher = new ZkaDirectoryWatcher(context, ZkaLeaderElector.PUB_SERVICES_PATH, this);
            
        } catch(Exception e) {
            throw new IOException(e);
        }
    }
    @Override
    public synchronized void close() throws IOException {
        Preconditions.checkState(!closed);
        
        closed = true;
        logger.info("closing");
        listeners.clear();
        
        CloseableUtils.closeQuietly(candidatesWatcher);
        CloseableUtils.closeQuietly(leasesWatcher);
        CloseableUtils.closeQuietly(maxLeaders);
        CloseableUtils.closeQuietly(publicationsWatcher);
        context.unregister(this);
    }
    @Override
    public int getMaxLeaders() {
        return maxLeaders.getCount();
    }
    @Override
    public Set<Map<String,Object>> getCandidates() {
        synchronized(cacheLock) {
            return candidateCache.values().stream().map(LeaderData::getData).collect(toSet());
        }
    }
        
    @Override
    public synchronized void subscribe(LeaderSubscriberListener listener) {
        listeners.add(listener);
    }
    @Override
    public Set<Map<String,Object>> getLeaders() {
        synchronized(cacheLock) {
            Set<Map<String,Object>> leaders = new HashSet<>(currentLeaders.size());
            for (long lid : currentLeaders) {
                LeaderData l = candidateCache.get(lid);
                if (l != null) {
                    leaders.add(l.getData());
                }
            }
            return leaders;
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
    public void countHasChanged(SharedCountReader sharedCount, int newCount) throws Exception {
    }
    
    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        
        logger.debug("going to process childEvent: {}",event.getType());
        
        switch(event.getType()) {
        case CHILD_ADDED:
            childAdded(event.getData());
            break;
        case CHILD_REMOVED:
            childRemoved(event.getData());
            break;
        case CHILD_UPDATED:
            if(isCandidateEvent(event.getData())) {
                childUpdated(event.getData());
            }
            break;
        case CONNECTION_LOST:
            break;
        case CONNECTION_RECONNECTED:
            break;
        case CONNECTION_SUSPENDED:
            break;
        case INITIALIZED:
            initialized();
            break;
        default:
            break;
        }
        
        logger.debug("processed childEvent: {}",event.getType());
    }
    
    private void candidateUpdate() {
        for(LeaderSubscriberListener listener : listeners) {
            listener.onCandidateUpdate(this);
        }
        ping(EventType.UPDATE);
    }
    
    private void leaderUpdate() {
        for(LeaderSubscriberListener listener : listeners) {
            listener.onLeaderUpdate(this);
        }
        ping(EventType.UPDATE);
    }
    
    
    private void childUpdated(ChildData event) {
        synchronized(cacheLock) {
            LeaderData zd = new LeaderData(event);
            long lid = zd.getLeaderId();
            
            logger.info("Candidate {} added or updated", lid);
            
            candidateCache.put(lid, zd);
            
            if(currentLeaders.contains(lid)) {
                leaderUpdate();
            } else {
                candidateUpdate();
            }
        }
    }
    
    private void childRemoved(ChildData event) {
        synchronized(cacheLock) {
            if(isCandidateEvent(event)) {
                
                LeaderData zd = new LeaderData(event);
                long lid = zd.getLeaderId();
                
                LeaderData czd = candidateCache.get(lid);
                
                
                if(czd.getStat().getCzxid() == zd.getStat().getCzxid() && candidateCache.remove(lid) != null) {
                    logger.info("removed candidate {}",lid);
                    candidateUpdate();
                }
            } else {
                buildCurrentLeaders();
            }
        }
    }
    
    private void buildCurrentLeaders() {
        
        List<ChildData> currentLeases = leasesWatcher.getCurrentData();
        
        Map<String, ZkaData> leasesData = new HashMap<>();
        for (ChildData cd : currentLeases) {
            leasesData.put(cd.getPath(), new ZkaData(cd));
        }
        
        List<String> leaseKeys = new ArrayList<>(leasesData.keySet());
        
        
        Collections.sort(leaseKeys, ZkaSemaphore.leaseSorter);
        
        
        
        
        int count = maxLeaders.getCount() >= leaseKeys.size() ? leaseKeys.size() : leaseKeys.size() -1;
        
        Set<Long> newLeaders = new HashSet<>();
        for(int i = 0; i < count; ++i) {
            Long lid = ZkaUtils.toLong(leasesData.get(leaseKeys.get(i)).getReservedMap().get(ZkaLeaderElector.LEADER_ID_KEY));
            newLeaders.add(lid);
        }
        
        logger.debug("new set of leaders: {}", newLeaders);
        
        synchronized(cacheLock) {
            if (!newLeaders.equals(currentLeaders)) {
                currentLeaders = newLeaders;
                logger.info("Leadership changed, new leader ids are {}", newLeaders);
                leaderUpdate();
            }
        }
    }
    private void childAdded(ChildData event) {
        if(isCandidateEvent(event)) {
            childUpdated(event);
        } else {
            buildCurrentLeaders();
            ping(EventType.UPDATE);
        }
    }
    
    private boolean isCandidateEvent(ChildData event) {
        String path = event.getPath();
        return path.startsWith(context.getBasePath() + ZkaLeaderElector.CANDIDATE_PATH);
    }
    private void initialized() {
        buildCurrentLeaders(); 
        if (!leasesWatcher.isInitialized() || !candidatesWatcher.isInitialized()
                || !publicationsWatcher.isInitialized()) {
            return;
        }
        
            if (initialized.getCount() == 0) {
                return;
            }
            initialized.countDown();
        logger.info("has initialized");
        for(LeaderSubscriberListener listener : listeners) {
            listener.onInitialized(this);
        }
        ping(EventType.UPDATE);
    }
    
    @Override
    public boolean waitForInitialization() {
        return waitForInitialization(0);
    }
    
    
    @Override
    public boolean waitForInitialization(long timeout) {
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
    
    
    public boolean waitForInit(long timeout, TimeUnit timeUnit)
    		throws InterruptedException {
    	return waitForInitialization(TimeUnit.MILLISECONDS.convert(timeout, timeUnit));
    }
    
    
    @Override
    public boolean isInitialized() {
        return initialized.getCount() == 0;
    }
    
    @Override
    public Set<Map<String, Object>> getNonLeaders() {
        synchronized(cacheLock) {
            return candidateCache.values().stream()
                    .filter(e -> !currentLeaders.contains(e.getLeaderId()))
                    .map(LeaderData::getData)
                    .collect(toSet());
        }
    }
    
    @Override
    public void addListener(ServiceLocatorListener l) {
    	locatorListeners.add(l);
    	
    	if (initialized.getCount() == 0) l.changeOccured(EventType.UPDATE);
    }
    
    @Override
    public void removeListener(ServiceLocatorListener l) {
        locatorListeners.remove(l);
    }
    
    @Override
    public void setNotificationPolicy(NotificationPolicy policy) {
        policyCallback = policy.createCallback(this);
    }
    
    @Override
    public Set<ServiceDescription> getServices(String service) {
        synchronized (cacheLock) {
            return services(currentLeaders, service);
        }
    }
    private Set<ServiceDescription> services(Set<Long> ids, String service) {
        
        final Set<ServiceDescription> sds = new HashSet<ServiceDescription>();
        for(ChildData cd : publicationsWatcher.getCurrentData()) {
            try {
                ZkaData zd = new ZkaData(cd);
                Long lid = ZkaUtils.toLong(zd.get(ZkaLeaderElector.PUBLISH_LEADER_ID_KEY));
                if (!ids.contains(lid)) {
                    continue;
                }
                if (ZkaUtils.serviceMatches(zd, service)) {
                    sds.add(new ZkaServiceDescription(zd));
                }
            } catch(Throwable t) {
                logger.warn("A publication {} didn't have a proper leader id", cd);
            }
        }
        
        return sds;
    }
    
    @Override
    public Set<ServiceDescription> getServices() {
        return getServices(service);
    }
    private void ping(EventType update) {
        NotificationNextStep nextStep = policyCallback.onEvent(update);
        
        nextStep.invoke(this, update);
    }
    
    @Override
    public void changeOccured(EventType eventType) {
        for (ServiceLocatorListener l : locatorListeners) {
            l.changeOccured(eventType);
        }
    }
    @Override
    public String toString() {
        return String.format("%s [service=%s]", ZkaLeaderSubscriber.class.getSimpleName(), service);
    }
}
