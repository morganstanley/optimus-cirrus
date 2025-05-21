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
import static com.google.common.base.Preconditions.checkNotNull;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import msjava.base.slf4j.ContextLogger;
import msjava.base.sr.ServiceDescription;
import msjava.base.sr.ServiceDescriptionProcessor;
import msjava.base.sr.internal.CanPublish;
import msjava.base.sr.internal.ServicePublisherUtils;
import msjava.base.sr.internal.ServiceUriBuilder;
import org.slf4j.Logger;
import com.google.common.annotations.VisibleForTesting;
public class ServicePublicationSupport {
    private final static Logger log = ContextLogger.safeLogger();
    
    private volatile ServiceDescriptionBuilder descriptionBuilder;
    private final ServicePublisher publisher;
    private final Object lock = new Object();
    @GuardedBy("lock")
    private Object handle;
    private final String serviceName;
    public ServicePublicationSupport(ServiceDescriptionBuilder descriptionBuilder, ServicePublisher p, String serviceName) {
        this.serviceName = serviceName;
        this.descriptionBuilder = checkNotNull(descriptionBuilder, "requires service description builder");
        this.publisher = checkNotNull( p, "requires service publisher" );
    }
    public ServicePublicationSupport(ServiceDescriptionBuilder descriptionBuilder, ServicePublisher p) {
        this(descriptionBuilder, p, null);
    }
    
    public ServicePublicationSupport(ServiceDescription service, ServicePublisher p) {
        this(new ConstantBuilder(service), p, service.getService());
    }
    
    
    public void addProcessor(ServiceDescriptionProcessor processor) {
    	descriptionBuilder = new ChainBuilder(this.descriptionBuilder, processor);
    }
    
    public boolean publish() {
        return publishWith(null);
    }
    
    public boolean publishWith(ServiceDescriptionProcessor processor) {
        if (cannotPublish())
            return false;
        synchronized (lock) {
            if (handle != null) {
                return false;
            }
            ServiceDescription sd = descriptionBuilder.buildServiceDescription();
            if (sd == null) {
                return false;
            }
            if (processor != null) {
                try {
                    sd = processor.apply(sd);
                } catch (Exception e) {
                    log.error("Something happened while applying processor " + processor.getClass().getName() + " to "
                            + publishingState(sd), e);
                    return false;
                }
                if (sd == null)
                    return false;
            }
            doPublish(sd);
            return isPublished();
        }
    }
    private boolean cannotPublish() {
        return publisher instanceof CanPublish && !((CanPublish) publisher).canPublish();
    }
    
    @VisibleForTesting
    public boolean publishOrUpdate() {
        if (cannotPublish())
            return false;
        final ServiceDescription sd = descriptionBuilder.buildServiceDescription();
        return sd != null && publish(sd);
    }
    @VisibleForTesting
    boolean publish(ServiceDescription serviceDescription) {
        checkNotNull(serviceDescription, "nothing to publish: service description is required");
        synchronized (lock) {
            if (handle != null) {
                log.debug("Going to update publication");
                return doUpdate(serviceDescription);
            }
            doPublish(serviceDescription);
            return isPublished();
        }
    }
    
    private void doPublish(ServiceDescription serviceDescription) {
        try {
            Object handle = publisher.publish(serviceDescription);
            if (handle != null) {
                updateHandle(handle);
                log.info("Publication successful for service {} => {} ({}:{})", serviceDescription.getService(),
                        serviceDescription.getEndpointUrl(), serviceDescription.getHost(),
                        serviceDescription.getPort());
            } else {
                log.warn("Cannot publish {}", publishingState(serviceDescription));
            }
        } catch (Exception e) {
            log.error("Something happened while publishing, " + publishingState(serviceDescription), e);
        }
    }
    
    
    boolean update(ServiceDescription serviceDescription) {
        synchronized (lock) {
            return handle != null && doUpdate(serviceDescription);
        }
    }
    
    private boolean doUpdate(ServiceDescription serviceDescription) {
        Object newHandle = publisher.update(handle, serviceDescription);
        if (newHandle == null) {
            log.warn("Cannot update {}", publishingState(serviceDescription));
            return false;
        } else {
            log.debug("Updated successfully {}", publishingState(serviceDescription));
            updateHandle(newHandle);
            return true;
        }
    }
    
    private void updateHandle(Object newHandle) {
        handle = newHandle;
    }
    
    public boolean unpublish() {
        synchronized (lock) {
            if (handle == null) {
                log.info("Cannot unpublish, hasn't been published {}", this);
                return false;
            }
            try {
                if (publisher.unpublish(handle)) {
                    updateHandle(null);
                }
            } catch (Exception e) {
                log.error("Something happened while unpublishing, " + this, e);
            }
            return !isPublished();
        }
    }
    
    
    
    @VisibleForTesting
    public boolean publishOrUpdateWith(@Nullable ServiceDescriptionProcessor processor) {
        if (cannotPublish())
            return false;
        ServiceDescription sd = descriptionBuilder.buildServiceDescription();
        if (sd == null)
            return false;
        
        if (processor != null)
            try {
                sd = processor.apply(sd);
            } catch (Exception e) {
                log.error("Something happened while applying processor " + processor.getClass().getName() + " to "
                        + publishingState(sd), e);
                return false;
            }
        
        if (sd == null)
            return false;
        
        return publish(sd);
    }
    
    private String publishingState(ServiceDescription serviceDescription) {
        return this + " publish " + serviceDescription;
    }
 
    public boolean isPublished() { return handle != null; }
    
    @Override
    public String toString() {
        return "(Service publisher: " + publisher + ", published: " + isPublished() + ")";
    }
    public ServicePublisher getPublisher() {
        return publisher;
    }
    @VisibleForTesting
    Object handle() {
        return handle;
    }
    
    
    @Deprecated
    public Set<String> getServiceURIs() {
        Collection<ServiceUriBuilder> pubs = ServicePublisherUtils.findNodesThat(publisher, ServiceUriBuilder.class);
        if (pubs.isEmpty())
            return Collections.emptySet();
        if (pubs.size() == 1)
            return Collections.singleton( pubs.iterator().next().serviceUri(serviceName));
        Set<String> s = new HashSet<>();
        for (ServiceUriBuilder sp : pubs) {
            s.add( sp.serviceUri(serviceName) );
        }
        return s;
    }
    
    private static class ConstantBuilder implements ServiceDescriptionBuilder {
        private final ServiceDescription service;
        public ConstantBuilder(ServiceDescription service) {
            this.service = service;
        }
        @Override
        public ServiceDescription buildServiceDescription() {
            return service;
        }
    }
    
    
}
