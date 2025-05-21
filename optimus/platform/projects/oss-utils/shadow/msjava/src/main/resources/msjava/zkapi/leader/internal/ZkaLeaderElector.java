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
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.Lease;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import com.google.common.base.Preconditions;
import msjava.base.annotation.Internal;
import msjava.base.slf4j.ContextLogger;
import msjava.base.sr.MapServiceDescription;
import msjava.base.sr.ServiceDescription;
import msjava.zkapi.ZkaAttr;
import msjava.zkapi.ZkaConfig;
import msjava.zkapi.ZkaPathContext;
import msjava.zkapi.internal.AtomicLatch;
import msjava.zkapi.internal.ZkaContext;
import msjava.zkapi.internal.ZkaData;
import msjava.zkapi.internal.ZkaPersistentNode;
import msjava.zkapi.internal.ZkaSemaphore;
import msjava.zkapi.internal.ZkaServiceDescription;
import msjava.zkapi.internal.ZkaSharedCount;
import msjava.zkapi.internal.retry.DelegatingRetryUntilElapsed;
import msjava.zkapi.leader.LeaderElector;
import msjava.zkapi.leader.LeaderElectorListener;
import msjava.zkapi.leader.LeaderSubscriber;
import msjava.zkapi.leader.optimus.ZkaMutex;
import msjava.zkapi.pubsub.ZkaServicePublisher;
public  class ZkaLeaderElector extends AbstractLeaderElector implements ConnectionStateListener, Runnable {
    
    static final String                                      PUBLISH_LEADER_ID_KEY = "__leaderId";
    private static final Logger                              log                   = ContextLogger.safeLogger();
    public static final String                               LEADER_ID_KEY         = "leaderId";
    public static final String                               LEADER_COUNT_PATH     = "/leaderCount";
    public static final String                               LEADER_ID_PATH        = "/leaderId";
    public static final String                               CANDIDATE_PATH        = "/candidates";
    public static final String                               CANDIDATE_PREFIX      = "/le-";
    public static final String                               PUB_SERVICES_PATH     = "/services";
    
    private final ZkaPathContext                             context;
    
    private int                                              id                    = -1;
    
    @GuardedBy("dataLock")
    private final ZkaData                                    data;
    private final Object                                     dataLock              = new Object();
    private final Object                                     stateLock             = new Object();
    
    private final long                                       bufferTime;
    
    private ZkaSemaphore                                     semaphore;
    
    private ZkaMutex mutex;
    
    private ZkaSharedCount                                   maxLeaders;
    
    private ZkaSharedCount                                   sharedLeaderId;
    
    private ZkaPersistentNode                                candidateNode;
    
    @GuardedBy("stateLock")
    private volatile boolean                                 closed;
    
    @GuardedBy("stateLock")
    private volatile boolean                                 started               = false;
    
    private final int                                        leaderCount;
    
    private final Thread                                     thread;
    
    private final AtomicLatch                                leader                = new AtomicLatch();
    
    private final AtomicLatch                                lost                  = new AtomicLatch();
    
    private final AtomicLatch                                lostInFlight          = new AtomicLatch();
    
    private final boolean                                    abdicateOnPartition;
    private long                                             sessionId;
    
    private final boolean                                    updateLeaderCount;
    private final CountDownLatch                             initialized           = new CountDownLatch(1);
    @GuardedBy("publicationHandles")
    private ZkaServicePublisher                              servicePublisher;
    @GuardedBy("publicationHandles")
    private final Map<Object, ServiceDescription>            publicationHandles    = new HashMap<Object, ServiceDescription>();
    
    private final String mutexPath;
    private final boolean usingMutex;
    public interface CuratorBlockingConnect {
        boolean connect(CuratorFramework c) throws InterruptedException;
        CuratorBlockingConnect DEFAULT = curatorFramework -> {
            curatorFramework.blockUntilConnected();
            return true;
        };
    }
    
    private final CuratorBlockingConnect curatorConnectImpl;
    
    public ZkaLeaderElector(ZkaContext context, @Nullable ZkaData data, LeaderElectorListener listener) throws IOException {
        this(context, data, listener, "", "");
    }
    
    @Internal
    public ZkaLeaderElector(ZkaContext context, @Nullable ZkaData data, LeaderElectorListener listener,
            String mutexPathForOptimus, String mutexContentForOptimus)
            throws IOException {
        this(context, data, listener, mutexPathForOptimus, mutexContentForOptimus, null);
    }
    
    @Internal
    public ZkaLeaderElector(ZkaContext context, @Nullable ZkaData data, LeaderElectorListener listener,
                            String mutexPathForOptimus, String mutexContentForOptimus, CuratorBlockingConnect curatorConnectImpl)
            throws IOException {
        super(listener);
        this.context = context;
        adjustRetryPolicy(context);
        this.data = data == null ? new ZkaData() : data;
        this.leaderCount = context.getInteger(ZkaAttr.LEADER_COUNT);
        this.bufferTime = context.getInteger(ZkaAttr.BUFFER_TIME);
        this.abdicateOnPartition = context.getBoolean(ZkaAttr.ABDICATE_ON_PARTITION);
        this.updateLeaderCount = context.getBoolean(ZkaAttr.UPDATE_LEADER_COUNT);
        this.mutexPath = Objects.requireNonNull(mutexPathForOptimus, "Mutex path cannot be null!");
        this.curatorConnectImpl = curatorConnectImpl == null ? CuratorBlockingConnect.DEFAULT : curatorConnectImpl;
        this.usingMutex = !"".equals(mutexPathForOptimus);
        if (usingMutex) {
            log.info("Using Mutex mode for Optimus with path '{}' and content '{}'", mutexPathForOptimus, mutexContentForOptimus);
            this.data.put(ZkaServiceDescription.END_POINT_URL, Objects.requireNonNull(mutexContentForOptimus, "Mutex content cannot be null!"));
        }
        context.register(this);
        thread = new Thread(this, ZkaLeaderElector.class.getName() + "-" + id);
        if (context.getBoolean(ZkaAttr.AUTO_START)) {
            log.info("auto starting");
            start();
        }
    }
    private static void adjustRetryPolicy(@Nonnull ZkaContext context) {
        if (!context.isJailed()) {
            log.warn("ZkaLeaderElector using shared connection! Adjusting retry policy for failover, this affects other users of this client as well!");
        }
        RetryPolicy delegate = context.getCurator().getZookeeperClient().getRetryPolicy();
        context.getCurator().getZookeeperClient().setRetryPolicy(new DelegatingRetryUntilElapsed(context.getConnectionTimeout(), delegate));
        
    }
    
    @Override
    public final void start() {
        synchronized (stateLock) {
            Preconditions.checkState(!started, "LeaderElector already started!");
            thread.start(); 
            started = true;
        }
        boolean initialized = false;
        try {
            initialized = waitForInitialization(2500);
        } catch (InterruptedException e) {
            
        }
        if (!initialized) {
        	log.warn("started but not initialized within 2500 ms");
        }
    }
    
    private void updateLeaderCount() throws Exception {
        if (!updateLeaderCount) {
            return;
        }
        
        final int current = maxLeaders.getCount();
        if (current != leaderCount) { 
            maxLeaders.setCount(leaderCount);
            log.info("{} changed the max lease count to {} from {}", id, leaderCount, current);
        }
    }
    
    @Override
    public final void close() throws IOException {
        synchronized(stateLock) {
            
            
            if (closed)
                return;
            CloseableUtils.closeQuietly(candidateNode);
            CloseableUtils.closeQuietly(maxLeaders);
            CloseableUtils.closeQuietly(sharedLeaderId);
            CloseableUtils.closeQuietly(servicePublisher);
            closed = true;
            abdicate();
            log.info("{} :: going to join thread while closing", id);
            
            
            
            
            
            
            if (thread.isAlive() && thread != Thread.currentThread()) {
                try {
                    
                    
                    
                    
                    
                    
                    
                    thread.join(context.getNegotiatedSessionTimeout());
                } catch (final InterruptedException e) {
                    log.info("failed to join thread while closing");
                    
                    
                }
            }
            CloseableUtils.closeQuietly(semaphore);
            CloseableUtils.closeQuietly(mutex);
            context.unregister(this);
            
            lost.fire();
            leader.fire();
        }
        log.info("{} :: is closed", id);
    }
    
    @GuardedBy("stateLock")
    @Override
    public final void stateChanged(CuratorFramework client, ConnectionState newState) {
        
        
        
        
        
        synchronized(stateLock) {
            if (closed || !started) {
                return;
            }
            final long nid = context.getSessionId();
            
            
            if (nid != sessionId || newState == ConnectionState.LOST) {
                log.info("{} has lost connection or its session. Calling abdicate. Existing session id {}, new session id {}.", id, "0x" + Long.toHexString(sessionId), "0x" + Long.toHexString(nid));
                sessionId = nid;
                abdicate();
                return;
            }
            
            if (abdicateOnPartition && newState == ConnectionState.SUSPENDED) {
                log.info("{} :: abdicating on suspension event", id);
                abdicate();
            }
        }
    }
    
    @Override
    public final int getMaxLeaders() throws IOException {
        ensureStarted();
        return maxLeaders.getCount();
    }
    
    @Override
    public final boolean waitForLeaderLoss() throws InterruptedException {
        return waitForLeaderLoss(0);
    }
    
    @Override
    public final boolean waitForLeaderLoss(long timeout) throws InterruptedException {
        ensureStarted();
        
        
        
        
        
        
        
        final AtomicLatch lif = new AtomicLatch(lostInFlight);
        if (!leader.isFired()) {
            return true; 
        }
        return lif.await(timeout);
    }
    
    @Override
    public final boolean isLeader() {
        if (!leader.isFired()) {
            return false;
        }
        if (lostInFlight.isFired()) {
            return false;
        }
        return true;
    }
    
    @Override
    public final boolean waitForLeadership() throws InterruptedException {
        return waitForLeadership(0);
    }
    
    
    @Override
    public final boolean waitForLeadership(long timeout) throws InterruptedException {
    	ensureStarted();
        return leader.await(timeout);
    }
    
    @Override
    public final void abdicate() {
        ensureStarted();
        log.info("{} :: abdicate called, was leader {}", id, isLeader());
        
        
        
        
        
        
        
        
        
        
        synchronized(stateLock) {
            
            
            AtomicLatch lostAtAbdicate = lost.getAndReplace();
            if (lostAtAbdicate.equals(lostInFlight) && otherThread()) {
                assert _preInterrupt_FOR_TESTING();
                thread.interrupt();
                assert _postInterrupt_FOR_TESTING();
                log.debug("{} :: interrupted by abdicate.", id);
            }
            lostAtAbdicate.fire(); 
        }
    }
    
    protected boolean _preInterrupt_FOR_TESTING() { return true; }
    protected boolean _postInterrupt_FOR_TESTING() { return true; }
    private boolean otherThread() { return thread != Thread.currentThread(); }
    
    private void safeAbdicate() {
        try {
            abdicate();
        } catch (RuntimeException e) {
            log.error("Exception was caught while abdicating", e);
        }
    }
    
    private void ensureStarted() {
         Preconditions.checkState(started, "Leader not started!");
    }
    
    
    private void ensureNotClosed() {
        Preconditions.checkState(!closed, "Leader already closed!");
    }
    
    
    @Override
    public final void run() {
        if (!initializationLoop()) {
            log.warn("Failed to initialize. Exiting loop. Leader election won't run.");
            return;
        }
        try {
            updateHandles(); 
        } catch (Throwable t) {
            log.error("failed to update handles, which means potentially locators may not discover my publications", t);
        }
        electionLoop();
    }
    private boolean initializationLoop() {
        
        while (initialized.getCount() != 0 && !closed) {
            if (Thread.interrupted()) {
                log.info("{} :: Stopping initialization due to interruption.", id);
                return false;
            }
            try {
                Thread.sleep(1000); 
                initialize();       
                                    
                                    
                                    
            } catch (InterruptedException e) {
                log.info("{} :: Stopping initialization due to interruption.", id);
                return false;
            } catch (Throwable e) {
                log.error("failed to initialize, trying again soon", e);
            }
        }
        if (closed) {
            log.info("{} :: closed while initializing.", id);
            return false;
        }
        return true;
    }
    
    private void electionLoop() {
        
        assert initialized.getCount() == 0;
        Lease lease = null;
        do {
            try {
                log.info("{} :: attempting to acquire lock", id);
                
                
                
                
                lostInFlight.replaceWith(lost);
                
                
                
                
                
                if (closed) {
                    log.info("{} :: closed before acquire.", id);
                    break;
                }
                
                
                
                
                
                curatorConnectImpl.connect(context.getCurator());
                
                Preconditions.checkState(context.getCurator().blockUntilConnected(0, TimeUnit.SECONDS),
                        "The zk connection is not connected after the curatorConnectImpl.connect");
                
                
                
                lease = acquire();
                log.info("{} :: lease acquired, going to wait for bufferTime {}", id, bufferTime);
                
                
                leader.fire();
                if (log.isDebugEnabled()) {
                    log.debug("{} :: checking lostInFlight, interruption status {}, bufferTime {}", id, Thread
                            .currentThread().isInterrupted(), bufferTime);
                }
                
                
                
                
                if (bufferTime > 0) {
                    lostInFlight.await(bufferTime);
                }
                elect(); 
            } catch (final InterruptedException ie) {
                log.debug(id + " was interrupted", ie);
                
                
                
                
                Thread.interrupted();
            } catch (final Throwable e) {
                log.error(id + " caught exception", e);
            } finally {
                
                if (leader.isFired()) {
                    leader.replace();
                }
                if (lease != null) {
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    returnLease(lease);
                    lease = null;
                }
            }
        } while (!closed);
        
        assert closed == true;
    }
    
    private void initialize() throws IOException, InterruptedException {
        if (initialized.getCount() == 0) {
            return; 
        }
        if (context.exists("") == null) { 
            log.warn("base path doesn't exist! can't initialize");
            Thread.sleep(1000);           
            return;
        }
        try {
            maxLeaders = new ZkaSharedCount(context, LEADER_COUNT_PATH, leaderCount);
            sharedLeaderId = new ZkaSharedCount(context, LEADER_ID_PATH, leaderCount);
            
            
            
            
            id = ZkaSharedCount.incrementAndGet(sharedLeaderId);
            updateLeaderCount(); 
            synchronized (dataLock) {
                data.getReservedMap().put(LEADER_ID_KEY, id); 
            }
            
            if (!usingMutex) {
                log.debug("Creating candidate node with data {}", data);
                candidateNode = new ZkaPersistentNode(context, CANDIDATE_PATH + CANDIDATE_PREFIX, data.serialize(), true);
                context.create(empty(), PUB_SERVICES_PATH);
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt(); 
            return;
        } catch (final Throwable t) {
            log.error("failed to initialize", t);
            return;
        }
        if (!usingMutex) {
            semaphore = new ZkaSemaphore(context, data.serialize(), maxLeaders);
        } else {
            try {
                
                
                context.create(empty(), CANDIDATE_PATH);
                context.create(empty(), ZkaSemaphore.SEMAPHORE_PATH);
                context.create(empty(), ZkaSemaphore.LEASES_PATH);
                context.create(empty(), ZkaSemaphore.LOCKS_PATH);
                context.create(empty(), PUB_SERVICES_PATH);
            } catch (IOException e) {
                log.warn("Failed to create normal ZkaLeaderElector znodes.", e);
            }
            log.info("Using mutex path {}", mutexPath);
			ZkaConfig cfg = new ZkaConfig().ensemble(context.getEnsemble()).path(mutexPath);
            mutex = new ZkaMutex(new ZkaContext(cfg, context.getCurator()), data);
        }
        sessionId = context.getSessionId();
        initialized.countDown();
        
        thread.setName(ZkaLeaderElector.class.getName() + "-" + id);
        log.info("Initialization finished with id {}", id);
    }
    
    private static byte[] empty() { return new ZkaData().serialize(); }
    private Lease acquire() throws Exception {
        return usingMutex ? mutex.acquireLock() : semaphore.acquire();
    }
    private void returnLease(Lease lease) {
        if (usingMutex) {
            log.info("{} :: going to return mutex lock", id);
            
            CloseableUtils.closeQuietly(lease);
        } else {
            log.info("{} :: going to return lease", id);
            
            
            semaphore.returnLease(lease);
            log.info("returned lease");
        }
    }
    
    private void elect() {
        
        
        if (lostInFlight.isFired()) {
            return;
        }
        log.info("{} :: electing", id);
        try {
            log.debug("{} :: calling startServices()", id);
            super.getListener().startServices(this);
        } catch (InterruptedException e) {
            log.info("{} :: Interrupted while starting, may have only partially started. Abdicating.", id);
            safeAbdicate();
        } catch (final Throwable t) {
            log.error("Caught exception while starting services. Abdicating.", t);
            safeAbdicate();
        }
        try {
            waitForLeaderLoss(); 
        } catch (final InterruptedException e) {
            assert _interruptWasCalled_FOR_TESTING();
            
        }
        log.info("{} :: leader loss detected, calling stopServices().", id);
        
        
        leader.replace();
        Thread.interrupted();
        try {
            super.getListener().stopServices(this);
        } catch (final Throwable t) {
            log.error("Caught exception while stopping services.", t);
        }
    }
    protected boolean _interruptWasCalled_FOR_TESTING() { return true; }
    
    
    @Override
    public final ZkaData getData() {
        return data;
    }
    
    @Override
    public final Set<Map<String, Object>> getLeaders() throws IOException {
        ensureNotClosed();
        ensureStarted();
        return usingMutex ? mutex.getLeaderData() : semaphore.getActiveLeases();
    }
    
    @Override
    public final long getLeaderId() {
        return id;
    }
    
    @Override
    public final Set<Map<String, Object>> getCandidates() throws IOException {
        if (usingMutex) return mutex.getCandidates();
        return new HashSet<>(context.getChildrenData(CANDIDATE_PATH, ZkaData::fromBytes).values());
    }
    
    @Override
    public final void setData(Map<String, Object> data) {
        synchronized(dataLock) {
        	this.data.replaceWith(data);
        	updateData();
        }
    }
    @GuardedBy("this")
    private synchronized void updateData() {
        if (candidateNode == null) {
        	log.warn("not updating data, due candidateNode is null, implies that the leader elector is not yet initialized.");
            return;
        }
        
        try {
            candidateNode.setData(data.serialize());
        } catch (final Exception e) {
            log.error(id + " failed to update data", e);
        }
    }
    
    @Override
    public final Object publish(ServiceDescription service) {
        synchronized (publicationHandles) {
            if (servicePublisher == null) {
                ZkaPathContext subContext = ZkaPathContext.contextForSubPath(this.context, PUB_SERVICES_PATH);
                subContext.setSelfDestructing(true);
                try {
                    servicePublisher = new ZkaServicePublisher(subContext);
                } catch (Exception e) {
                    subContext.close();
                    throw e;
                }
            }
            ServiceDescription psd = MapServiceDescription.asMutable(service);
            psd.put(PUBLISH_LEADER_ID_KEY, id);
            Object handle = servicePublisher.publish(psd);
            publicationHandles.put(handle, psd);
            return handle;
        }
    }
    
    @Override
    public final boolean unpublish(Object publicationHandle) {
    	if (servicePublisher == null) {
            return true;
        }
        synchronized (publicationHandles) {
        	publicationHandles.remove(publicationHandle);
            return servicePublisher.unpublish(publicationHandle);
        }
    }
    @Override
    public final Object update(Object publicationHandle, ServiceDescription desc) {
        if (servicePublisher == null) {
            return null;
        }
        
        synchronized (publicationHandles) {
            return servicePublisher.update(publicationHandle, desc);
        }
    }
    
    private void updateHandles() {
        synchronized (publicationHandles) {
            if (publicationHandles.isEmpty() || servicePublisher == null) {
                return;
            }
            for (Entry<Object, ServiceDescription> e : publicationHandles.entrySet()) {
                updatePublishedLeaderId(e.getKey(), e.getValue());
            }
        }
    }
    
    @GuardedBy("publicationHandles")
    private void updatePublishedLeaderId(final Object handle, final ServiceDescription psd) {
    	log.debug("Updating published leader id for {}", id);
        String publishedKey = psd.get(PUBLISH_LEADER_ID_KEY).toString();
        String sid = "" + id;
        if (sid.equals(publishedKey)) {
            return;
        }
        psd.put(PUBLISH_LEADER_ID_KEY, id);
        try {
            boolean applied = servicePublisher.updateServiceDescription(handle, psd);
            if (applied == false) {
                log.warn("did not update leader id on a publisher handle, which should never happen");
            }
        } catch (Exception e) {
            log.error(
                    "failed to update publishedData with real leader id, which can cause issues for locators to discover elected publications",
                    e);
        }
    }
    
    @Override
    public final boolean waitForInitialization() throws InterruptedException {
        initialized.await();
        return initialized.getCount() == 0;
    }
    
    @Override
    public final boolean waitForInitialization(long timeout) throws InterruptedException {
        return initialized.await(timeout, TimeUnit.MILLISECONDS);
    }
}
