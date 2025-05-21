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
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.annotation.Nullable;
import msjava.base.slf4j.ContextLogger;
import msjava.base.slr.internal.BasePushServiceLocator;
import msjava.base.slr.internal.CompositePublisher;
import msjava.base.slr.internal.NotificationPolicy;
import msjava.base.slr.internal.ServiceEnvironment;
import msjava.base.slr.internal.ServiceLocator;
import msjava.base.slr.internal.ServiceLocatorClient;
import msjava.base.slr.internal.ServiceLookup;
import msjava.base.slr.internal.ServicePublisher;
import msjava.base.sr.internal.CompositeServiceLocatorClient;
import msjava.zkapi.internal.DefaultACLProvider;
import msjava.zkapi.internal.ZkaContext;
import msjava.zkapi.internal.ZkaData;
import msjava.zkapi.internal.ZkaPropertySource;
import msjava.zkapi.internal.ZkaResourceContext;
import msjava.zkapi.leader.LeaderElector;
import msjava.zkapi.leader.LeaderElectorListener;
import msjava.zkapi.leader.LeaderSubscriber;
import msjava.zkapi.leader.LeaderSubscriberListener;
import msjava.zkapi.leader.internal.ZkaLeaderElector;
import msjava.zkapi.leader.internal.ZkaLeaderSubscriber;
import msjava.zkapi.leader.spring.ZkaMBeanRegistrar;
import msjava.zkapi.pubsub.ZkaServicePublisher;
import msjava.zkapi.pubsub.ZkaServiceSubscriber;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.ACLProvider;
import org.slf4j.Logger;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
public class ZkaFactory {
	private static final Logger log = ContextLogger.safeLogger();
	
    public static LeaderSubscriber newLeaderSubscriber(ZkaConfig config) throws IOException {
        return newLeaderSubscriber(config, null);
    }
    
    public static ZkaServicePublisher newServicePublisher(String uriString) throws URISyntaxException {
        final URI uri = new URI(uriString);
        return newServicePublisher(uri);
    }
    public static ZkaServicePublisher newServicePublisher(ZkaConfig config) {
        return new ZkaServicePublisher(ZkaPathContext.selfDestructing(config));
    }
    public static ZkaServicePublisher newServicePublisher(URI uri) {
        final ZkaConfig config = ZkaConfig.fromURI(uri);
        return newServicePublisher(config);
    }
    
    private static final Function<ZkaConfig, ZkaPathContext> ZkaContext_NEW = ZkaPathContext::selfDestructing;
    
    
    private abstract static class CreateComposite<R> {
        R create(ZkaConfig config, Function<ZkaConfig, ZkaPathContext> contextFactory) {
            String fallback = config.getAttributes().get(ZkaAttr.ADDITIONAL);
            if (fallback == null) {
                return createSimple(contextFactory.apply(config));
            } else {
                String[] envRegion = fallback.split("\\.");
                if (envRegion.length != 2) {
                    throw new IllegalArgumentException(fallback + " is not a valid ZK ensemble");
                }
                ZkaConfig config1 = withoutAdditional(config);
                config1.environment(ServiceEnvironment.valueOf(envRegion[0])).region(envRegion[1]);
                R pub1 = createSimple(contextFactory.apply(config1));
                ZkaConfig config0 = withoutAdditional(config);
                R pub0 = createSimple(contextFactory.apply(config0));
                return combine(ImmutableList.of(pub0, pub1));
            }
        }
        protected abstract R createSimple(ZkaPathContext ctx);
        protected abstract R combine(List<R> of);
        
        private static ZkaConfig withoutAdditional(ZkaConfig config) {
            ZkaConfig c = config.copy();
            c.getAttributes().remove(ZkaAttr.ADDITIONAL);
            return c;
        }
    }
    
    private static final CreateComposite<ServicePublisher> fallbackPub = new CreateComposite<ServicePublisher>() {
        @Override
        protected ServicePublisher createSimple(ZkaPathContext ctx) {
            return new ZkaServicePublisher(ctx);
        }
        @Override
        protected ServicePublisher combine(List<ServicePublisher> of) {
            return new CompositePublisher(of);
        }
    };
    
    
    public static ServicePublisher newCompositeServicePublisher(URI uri) {
        final ZkaConfig config = ZkaConfig.fromURI(uri);
        return newCompositeServicePublisher(config);
    }
    
    public static ServicePublisher newCompositeServicePublisher(ZkaConfig config) {
        return newCompositeServicePublisher(config, ZkaContext_NEW);
    }
    
    @VisibleForTesting
    public static ServicePublisher newCompositeServicePublisher(ZkaConfig config, Function<ZkaConfig, ZkaPathContext> contextFactory) {
        return fallbackPub.create(config, contextFactory);
    }
    
    
    public static ServiceLocator newCompositeServiceLocator(URI uri) {
        return newCompositeServiceLocator(uri, ZkaContext_NEW);
    }
    
    @VisibleForTesting
    public static ServiceLocator newCompositeServiceLocator(URI uri, Function<ZkaConfig, ZkaPathContext> contextFactory) {
        return new ZkaCompositeServiceLocator(uri, contextFactory);
    }
    
    private static class AdditionalSub extends CreateComposite<ServiceLocatorClient> {
        private final NotificationPolicy policy;
        private AdditionalSub(NotificationPolicy policy) { this.policy = policy; }
        @Override
        protected ServiceLocatorClient createSimple(ZkaPathContext ctx) {
            ZkaServiceSubscriber client = new ZkaServiceSubscriber(ctx);
            if (policy != null)
                client.setNotificationPolicy(policy);
            return client;
        }
        @Override
        protected ServiceLocatorClient combine(List<ServiceLocatorClient> of) {
            return new CompositeServiceLocatorClient(of);
        }
    }
    private static class ZkaCompositeServiceLocator extends BasePushServiceLocator {
        private final URI uri;
        private final Function<ZkaConfig, ZkaPathContext> contextFactory;
        ZkaCompositeServiceLocator(URI uri,  Function<ZkaConfig, ZkaPathContext> contextFactory) {
            this.uri = uri;
            this.contextFactory = contextFactory;
        }
        
        @Override
        public ServiceLocatorClient createClient(ServiceLookup slu, final NotificationPolicy policy) {
            ZkaConfig config = ZkaConfig.fromURI(uri);
            if (config.getAttributes().get(ZkaAttr.SERVICE) == null) {
                Objects.requireNonNull(slu, "Service name must be provided either in the URI or as ServiceLookup.");
                config.attr(ZkaAttr.SERVICE, slu.getName());
            }
            return new AdditionalSub(policy).create(config, contextFactory);
        }
    }
    
    
    
    public static ZkaServiceSubscriber newServiceSubscriber(String uriString) throws IOException, URISyntaxException {
        final URI uri = new URI(uriString);
        return newServiceSubscriber(uri);
    }
    public static ZkaServiceSubscriber newServiceSubscriber(ZkaConfig config) throws IOException {
        return new ZkaServiceSubscriber(ZkaPathContext.selfDestructing(config));
    }
    public static ZkaServiceSubscriber newServiceSubscriber(URI uri) throws IOException {
        final ZkaConfig config = ZkaConfig.fromURI(uri);
        return newServiceSubscriber(config);
    }
    
    public static LeaderSubscriber newLeaderSubscriber(ZkaConfig config, @Nullable LeaderSubscriberListener listener)
            throws IOException {
        return new ZkaLeaderSubscriber(ZkaPathContext.selfDestructing(config), listener, null);
    }
    
    public static LeaderElector newLeaderElector(ZkaConfig config, @Nullable Map<String, Object> data,
            LeaderElectorListener listener) throws IOException {
    	
        if (!config.getAttributes().containsKey(ZkaAttr.JAIL)) {
            config.attr(ZkaAttr.JAIL, true);
        }
        ZkaLeaderElector le = new ZkaLeaderElector(new ZkaContext(config), new ZkaData(data), listener);
        enableJMX(config, le);
        return le;
    }
    
    private static void enableJMX(ZkaConfig config, ZkaLeaderElector le) {
        try {
	    	if (config.getBoolean(ZkaAttr.JMX)) {
	            ZkaMBeanRegistrar.register(le);
	        }
        } catch (Exception e) {
        	log.warn("Failed to enable JMX on LE", e);
        }
    }
    
    public static LeaderSubscriber newLeaderSubscriber(String uri, @Nullable LeaderSubscriberListener listener)
            throws IOException, URISyntaxException {
        final ZkaConfig config = ZkaConfig.fromURI(uri);
        return new ZkaLeaderSubscriber(ZkaPathContext.selfDestructing(config), listener, null);
    }
    
    public static LeaderElector newLeaderElector(String uri, @Nullable Map<String, Object> data,
            LeaderElectorListener listener) throws IOException, URISyntaxException {
        final ZkaConfig config = ZkaConfig.fromURI(uri);
        return newLeaderElector(config, data, listener);
    }
    
    @Deprecated
    public static ZkaPropertySource newExperimentalPropertySource(String uri) throws IOException, URISyntaxException {
        return newExperimentalPropertySource("", uri);
    }
    
    @Deprecated
    public static ZkaPropertySource newExperimentalPropertySource(@Nullable String namespace, ZkaConfig config)
            throws IOException {
    	Preconditions.checkState(!ZkaPropertySource.isNamespaceInstalled(namespace), "Cannot install the same namespace twice");
        return new ZkaPropertySource(namespace, new ZkaContext(config));
    }
    
    @Deprecated
    public static ZkaPropertySource newExperimentalPropertySource(@Nullable String namespace, String uri)
            throws IOException, URISyntaxException {
        return newExperimentalPropertySource(namespace, ZkaConfig.fromURI(uri));
    }
    
    public static PropertySource newPropertySource(String uri) throws IOException, URISyntaxException {
        return newPropertySource("", uri);
    }
    
    
    public static PropertySource newPropertySource(@Nullable String namespace, ZkaConfig config)
            throws IOException {
        Preconditions.checkState(!ZkaPropertySource.isNamespaceInstalled(namespace), "Cannot install the same namespace twice");
        return new ZkaPropertySource(ZkaPathContext.selfDestructing(config), namespace);
    }
    
    
    public static PropertySource newPropertySource(@Nullable String namespace, String uri)
            throws IOException, URISyntaxException {
        return newPropertySource(namespace, ZkaConfig.fromURI(uri));
    }
    
    
    public static CuratorFramework newCurator(ZkaConfig config) {
        return newCurator(config, new DefaultACLProvider(
            ZkaResourceContext.validatePath(config.getPath()).basePath(),
            config.getInteger(ZkaAttr.CONNECTION_TIMEOUT)));
    }
    
    
    public static CuratorFramework newCurator(ZkaConfig config, ACLProvider aclProvider) {
        return ZkaResourceContext.newCurator(config, aclProvider);
    }
    
    public static ZkaPathContext newCloseableContext(ZkaConfig config) {
        return new ZkaPathContext(config);
    }
    
    public static ZkaPathContext newSelfDestructingContext(ZkaConfig config) {
        return ZkaPathContext.selfDestructing(config);
    }
}
