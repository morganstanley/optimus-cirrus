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
import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import msjava.base.annotation.Internal;
import msjava.base.slf4j.ContextLogger;
import msjava.base.slr.internal.ServicePublisher;
import msjava.base.sr.ServiceDescription;
import msjava.zkapi.ZkaPathContext;
import msjava.zkapi.internal.ZkaContext;
import msjava.zkapi.internal.ZkaData;
import msjava.zkapi.internal.ZkaPersistentNode;
import msjava.zkapi.internal.ZkaServiceDescription;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import javax.annotation.concurrent.GuardedBy;
@Internal
public final class ZkaServicePublisher implements ServicePublisher, Closeable {
    private static final Logger log = ContextLogger.safeLogger();
    private static final AtomicLong NEXT_ID = new AtomicLong();
    @GuardedBy("lock")
    private final Map<Long, ZkaPersistentNode> nodes = new HashMap<>();
    private final ZkaPathContext context;
    private final Object lock = new Object();
    @GuardedBy("lock")
    private volatile boolean closed = false;
    private final Function<ServiceDescription, byte[]> sdSerializer;
    
    public ZkaServicePublisher(ZkaContext context) {
        this((ZkaPathContext) context);
    }
    public ZkaServicePublisher(ZkaPathContext context) {
        this(context, serviceDescription -> convert(serviceDescription).serialize());
    }
    public ZkaServicePublisher(ZkaPathContext context, Function<ServiceDescription, byte[]> sdSerializer) {
        this.context = context;
        context.register(this);
        this.sdSerializer = sdSerializer;
    }
    
    @Override
    public Object publish(ServiceDescription service) {
        synchronized (lock) {
            if (closed) {
                log.warn("Cannot publish - publisher already closed.");
                return null;
            }
            
            final String nodePrefix = removeInvalidCharacters(service.getService() + "-");
            try {
                
                ZkaPersistentNode zpn = new ZkaPersistentNode(context, nodePrefix, this.sdSerializer.apply(service), false);
                final long id = NEXT_ID.incrementAndGet();
                nodes.put(id, zpn);
                if (log.isDebugEnabled()) {
                    log.debug("Publication successful for service {} => {} ({}:{}) with handle {}",
                            service.getService(), service.getEndpointUrl(), service.getHost(), service.getPort(), id);
                }
                return id;
            } catch (final Exception e) {
                
                log.error("failed to publish", e);
                return null;
            }
        }
    }
    
    
    private static String removeInvalidCharacters(String nodePrefix) {
        if (!nodePrefix.matches("[a-zA-Z_\\-0-9]*")) {
            log.warn("Service name is invalid for zkapi node name. Invalid characters will be removed.");
            return nodePrefix.replaceAll("[^a-zA-Z_\\-0-9]", "");
        }
        return nodePrefix;
    }
    @VisibleForTesting
    static ZkaData convert(ServiceDescription service) {
        return new ZkaData(ZkaServiceDescription.translateKeysToZkaName(service));
    }
    
    @GuardedBy("lock")
    public boolean updateServiceDescription(Object publishHandle, ServiceDescription sd) {
        synchronized (lock) {
            if (closed) {
                log.warn("Cannot update handle {} - publisher already closed.", publishHandle);
                return false;
            }
            final ZkaPersistentNode node = nodes.get(publishHandle);
            if (node == null) {
                log.error("Node for handle [{}] not found, cannot update", publishHandle);
                return false;
            }
            if (log.isDebugEnabled()) {
                log.debug("Updating node with handle={}, path={}", publishHandle, node.getActualPath());
            }
            try {
                
                node.waitForInitialCreate(1, TimeUnit.SECONDS);
                node.setData(this.sdSerializer.apply(sd));
            } catch (Exception e) {
                log.error("failed to update node data for path {}", node.getActualPath(), e);
                Throwables.throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
            return true;
        }
    }
    
    @Override
    @GuardedBy("lock")
    public boolean unpublish(Object publicationHandle) {
        synchronized (lock) {
            if (closed) {
                log.warn("Cannot unpublish handle {} - publisher already closed.", publicationHandle);
                return false;
            }
            final ZkaPersistentNode zpn = nodes.remove(publicationHandle);
            if (zpn == null) {
                log.debug("Handle {} not found, nothing to unpublish.", publicationHandle);
                return false;
            }
            log.debug("Unpublishing service with handle {}", publicationHandle);
            CloseableUtils.closeQuietly(zpn);
            return true;
        }
    }
    
    @Override
    @GuardedBy("lock")
    public void close() {
        if (closed) {
            
            
            
            log.debug("Cannot close - publisher already closed.");
            return;
        }
        synchronized (lock) {
            if (closed) {
                log.debug("Cannot close - publisher already closed. (2)");
                return;
            }
            log.debug("Closing publisher");
            closed = true;
            for (final ZkaPersistentNode zpn : nodes.values()) {
                CloseableUtils.closeQuietly(zpn);
            }
            nodes.clear();
            context.unregister(this);
        }
    }
    @Override
    @GuardedBy("lock") 
    public Object update(Object handle, ServiceDescription desc) {
        
        
        if (updateServiceDescription(handle, desc))
            return handle;
        else
            return null;
    }
    
    @Deprecated
    public String serviceUri(String serviceName) {
        return context.toServiceURI("zps", serviceName);
    }
    
    @VisibleForTesting
    boolean isPublicationNodeCreated(Object handle) {
        ZkaPersistentNode node = nodes.get(handle);
        return node == null ? false : node.getActualPath() != null;
    }
    @VisibleForTesting
    boolean waitForInitialCreate(Object handle, long timeout, TimeUnit unit) throws InterruptedException {
        ZkaPersistentNode node = nodes.get(handle);
        return node == null ? false : node.waitForInitialCreate(timeout, unit);
    }
}
