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
package msjava.base.slr.internal;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;
import msjava.base.selector.SelectionContext;
import msjava.base.selector.ServiceSelector;
import msjava.base.selector.ServiceSelector.SelectionState;
import msjava.base.selector.ServiceSelectorBuilder;
import msjava.base.slf4j.ContextLogger;
import msjava.base.slr.internal.ServiceLocatorClient.EventType;
import msjava.base.slr.internal.ServiceLocatorClient.ServiceLocatorListener;
import msjava.base.sr.ServiceDescription;
import msjava.base.sr.internal.ServiceUtils;
import msjava.base.sr.registry.ServiceRegistry;
import org.slf4j.Logger;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
public class ServiceLocationSupport implements ServiceLocatorListener, Closeable {
    private final static Logger log = ContextLogger.safeLogger();
    
    private ServiceLocator serviceLocator;
    private ServiceLookup serviceLookup;
    private boolean ownServiceLocator;
    private Predicate<ServiceDescription> filter;
    private ServiceLookupResultProcessor resultProcessor;
    
    private ServiceSelector serviceSelector = new ServiceSelectorBuilder().roundRobinTerminal();
    private SelectionState selectionState = serviceSelector.createSelectionState();
    private volatile ServiceLocatorClient client;
    private final AtomicInteger version = new AtomicInteger();
    private final CopyOnWriteArrayList<ServiceDescriptionSetConsumer> listeners = new CopyOnWriteArrayList<>();
    
    private static final long DEFAULT_TIMEOUT_IN_MILLISECONDS = 60000L;
    
    
    
    private final CountDownLatch updateLatch = new CountDownLatch(1);
    private volatile boolean updateReceived = false;
    
    public ServiceLocationSupport() {
    }
    
    public ServiceLocationSupport(ServiceDescriptionSetConsumer consumer) {
    	this();
    	subscribeServiceDataEvents(consumer);
    }
   
    public synchronized void start() {
        if (client == null) {
            client = createSLRClient();
        }
        forceRefresh();
    }
    private void processRefresh() {
        final ServiceLocatorClient cl = client;
        final Predicate<ServiceDescription>  predicate = this.filter;
        final ServiceLookupResultProcessor rp = resultProcessor;
        final SelectionState state = selectionState;
        Collection<ServiceDescription> data = cl == null ? Collections.<ServiceDescription>emptySet() : cl.getServices();
        
        if (predicate != null) {
            data = FluentIterable.from(data).filter(predicate).toList();
        }
        if (rp != null) {
            data = rp.apply(data);
        }
        data = state.onNewSetOfServiceData(data);
        notifyListeners(data);
        if (!data.isEmpty()) {
        	
        	updateLatch.countDown();
        }
        increment();
    }
    @VisibleForTesting
    protected void notifyListeners(Collection<ServiceDescription> data) {
		for (ServiceDescriptionSetConsumer c: listeners) {
			try {
				c.consume(data);
			}
			catch(RuntimeException e) {
				log.error(e.getMessage(), e);
				
			}
		}
	}
    private final ReentrantLock refreshLock = new ReentrantLock();
    
    public void forceRefresh() {
        refreshLock.lock();
        try {
            processRefresh();
        } finally {
            refreshLock.unlock();
        }
        if (updateReceived) {
        	
        	updateLatch.countDown();
        }
    }
    public void suggestRefresh() {
    	if (refreshLock.tryLock()) {
	        try {
	            processRefresh();
	        } finally {
	            refreshLock.unlock();
	        }
    	}
    }
    
    private ServiceLocatorClient createSLRClient() {
    	if (serviceLocator == null) return null;
        ServiceLocatorClient c = serviceLocator.createClient(serviceLookup, null);
        c.addListener(this);
        return c;
    }
    public synchronized void stop() {
        if (client != null) {
            client.removeListener(this);
            try {
                client.close();
            } catch (final IOException e) {
                log.error("Error while closing service lookup client", e);
            } finally {
                client = null;
            }
        }
    }
    public boolean isRefreshable() {
        return client != null;
    }
    public ServiceLocator getServiceLocator() {
        return serviceLocator;
    }
    public void setServiceLocator(ServiceLocator serviceLocator) {
        if (ownServiceLocator && this.serviceLocator instanceof Closeable) {
            try {
                ((Closeable) this.serviceLocator).close();
            } catch (Exception e) {
                log.warn("Exception while closing service locator", e);
            }
        }
        ownServiceLocator = false;
        this.serviceLocator = serviceLocator;
    }
    public ServiceLookup getServiceLookup() {
        return serviceLookup;
    }
    public void setServiceLookup(ServiceLookup serviceLookup) {
        this.serviceLookup = serviceLookup;
    }
    
    public void setServiceFilter(Predicate<ServiceDescription> predicate) {
        this.filter = predicate;
    }
    
    public Predicate<ServiceDescription> getServiceFilter() {
        return filter;
    }
    
    public void setResultProcessor(ServiceLookupResultProcessor resultProcessor) {
        this.resultProcessor = resultProcessor;
    }
    private void increment() {
        version.incrementAndGet();
    }
    public int version() { return version.intValue(); }
 
    @Override
    public void changeOccured(EventType eventType) {
    	if (eventType == EventType.UPDATE) updateReceived = true;
        suggestRefresh();
    }
    public ServiceSelector getServiceSelector() {
		return serviceSelector;
	}
	
    public void setServiceSelector(@Nullable ServiceSelector serviceSelector) {
    	if (serviceSelector != null) {
    		this.serviceSelector = serviceSelector;
    		this.closeSelectionState();
    		this.selectionState = Preconditions.checkNotNull(this.serviceSelector.createSelectionState(), "ServiceSelector must not return null state!");
    	}
	}
    
    public SelectionContext selectNext(@Nullable Object context, long timeout) {
    	try {
    		if (log.isTraceEnabled()){
    			log.trace("waiting for first UPDATE event (if it havent happened yet)");
    		
				log.trace(updateLatch.await(timeout, TimeUnit.MILLISECONDS) ?
						"proceeding with select - UPDATE event had been received":
						"timed out while waiting for first UPDATE event (with timeout of " + timeout + "milliseconds");
    		}
    		else {
    			updateLatch.await(timeout, TimeUnit.MILLISECONDS);
    		}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		}
    	return serviceSelector.selectNext(context, selectionState);
    }
    public SelectionContext selectNext(@Nullable Object context) {
    	return selectNext(context, DEFAULT_TIMEOUT_IN_MILLISECONDS) ;
    }
    @Override
    public String toString() {
        
        return MoreObjects.toStringHelper(this).omitNullValues()
                .add("client", client)
                .add("predicate", filter)
                .add("resultProcessor", resultProcessor)
                .add("serviceLocator", serviceLocator)
                .add("serviceLookup", serviceLookup)
                .add("serviceSelector", serviceSelector)
                .toString();
        
    }
    
    public void setServiceURI(String uriString) {
        Preconditions.checkArgument(uriString != null, "uriString cannot be null");
        if (!uriString.contains(":
            setServiceLookup(ServiceLookup.forName(uriString));
            return;
        }
        setServiceLocator(ServiceRegistry.buildServiceLocator(uriString));
        setServiceLookup(ServiceUtils.buildServiceLookup(uriString));
        this.ownServiceLocator = true;
    }
    
    @Override
    public void close() {
    	this.stop();
        if (ownServiceLocator && serviceLocator != null && serviceLocator instanceof Closeable) {
            try {
                ((Closeable) serviceLocator).close();
            } catch (final Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        closeSelectionState();
    }
    
    private void closeSelectionState () {
    	if (selectionState instanceof AutoCloseable) {
	    	try {
				((AutoCloseable)this.selectionState).close();
			} catch (Exception e) {
				log.warn("Couldn't close SelectionState", e);
			}
    	}
    }
    
    public void subscribeServiceDataEvents(ServiceDescriptionSetConsumer o) {
    	listeners.add(o);
    }
    
    public void unsubscribeServiceDataEvents(ServiceDescriptionSetConsumer o) {
    	listeners.remove(o);
    }
    
    
    public void markInitialized() {
        updateLatch.countDown();
    }
    
}
