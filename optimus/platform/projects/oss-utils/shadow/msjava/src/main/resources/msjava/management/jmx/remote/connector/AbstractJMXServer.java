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

package msjava.management.jmx.remote.connector;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.management.MBeanServer;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.MBeanServerForwarder;
import javax.management.remote.jmxmp.JMXMPConnectorServer;
import msjava.base.jmx.MBeanServerAware;
import msjava.base.jmx.MBeanServerReferencingBean;
import msjava.base.kerberos.MSKerberosAuthority;
import msjava.base.slf4j.ContextLogger;
import msjava.base.slr.internal.ServiceDescriptionBuilder;
import msjava.base.slr.internal.ServiceName;
import msjava.base.slr.internal.ServicePublicationSupport;
import msjava.base.slr.internal.ServicePublisher;
import msjava.base.slr.internal.ServicePublisherAware;
import msjava.base.spring.lifecycle.MSComponentLifecyclePhases;
import msjava.base.sr.MapServiceDescription;
import msjava.base.sr.ServiceAttributes.CommonKey;
import msjava.base.sr.ServiceDescription;
import msjava.base.sr.internal.PublicationAttributesSupport;
import msjava.base.util.HostUtils;
import msjava.base.util.internal.SystemPropertyUtils;
import msjava.management.jmx.MSJmxUtils;
import msjava.management.jmx.remote.protocol.jmxmpmskerberos.JMXMPMSKerberosProtocolConstants;
import msjava.tools.util.MSPortUtil;
import org.slf4j.Logger;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.SmartLifecycle;
public abstract class AbstractJMXServer implements SmartLifecycle, MBeanServerAware, MBeanServerReferencingBean,
        ServicePublisherAware {
    static enum Protocol {
        RMI, JMXMP, JMXMPMSKERBEROS;
    }
    private static final Logger LOGGER = ContextLogger.safeLogger();
    public static final int DEFAULT_ORDER = MSComponentLifecyclePhases.ADMIN_MANAGER_PHASE;
    public static final String DEFAULT_SERVICE_NAME = SystemPropertyUtils.getProperty("msjava.JMXServer.serviceName", "JMXServer", LOGGER);
    private final Protocol protocol;
    private final MBeanServerForwarder mbeanServerForwarder;
    public final MBeanServerForwarder getMbeanServerForwarder() {
        return mbeanServerForwarder;
    }
    private MBeanServer mbeanServer;
    private MBeanServer mbeanServerToUse;
    private JMXConnectorServer connectorServer;
    private final List<Consumer<JMXConnectorServer>> connectorServerConfigurers = new LinkedList<>();
    
    private boolean alwaysOn = false;
    private String host = null;
    private int port = 0;
    
    private final AtomicBoolean isInitialized = new AtomicBoolean(false);
    
    private String url;
    private Map<String, Object> jmxConnectorServerEnv;
    private ServicePublicationSupport servicePublicationSupport;
    private final PublicationAttributesSupport publicationAttributes = new PublicationAttributesSupport().mergeWith(DEFAULT_SERVICE_NAME);
    
    AbstractJMXServer(Protocol protocol, MBeanServerForwarder mbeanServerForwarder) {
        this.protocol = protocol;
        this.mbeanServerForwarder = mbeanServerForwarder;
    }
    
    public boolean isAlwaysOn() {
        return alwaysOn;
    }
    
    public void setAlwaysOn(boolean alwaysOn) {
        checkIsNotInitialized();
        this.alwaysOn = alwaysOn;
    }
    
    public String getUrl() {
        if (!isRunning()) {
            throw new IllegalStateException("Server is not running");
        }
        assert url != null;
        return url;
    }
    
    public Protocol getProtocol() {
        return protocol;
    }
    
    public void setHost(String host) {
        checkIsNotInitialized();
        this.host = host;
    }
    
    public String getHost() {
        return host;
    }
    
    public void setPort(String portOrServiceName) {
        checkIsNotInitialized();
        try {
            this.port = MSPortUtil.stringToPort(portOrServiceName);
        } catch (Exception e) {
            throw new IllegalArgumentException("Fail to get port number for " + portOrServiceName, e);
        }
    }
    public void setPort(int port) {
        checkIsNotInitialized();
        this.port = port;
    }
    
    public int getPort() {
        return port;
    }
    
    @Override
    public void setMBeanServer(MBeanServer server) {
        checkIsNotInitialized();
        this.mbeanServer = server;
    }
    
    @Override
    public MBeanServer getMBeanServer() {
        return this.mbeanServer;
    }
    
    @PostConstruct
    public void init() throws Exception {
        if (!isInitialized.compareAndSet(false, true)) {
            throw new IllegalStateException("Already configured, cannot configure again.");
        }
        if (protocol != Protocol.JMXMPMSKERBEROS) {
            MSKerberosAuthority.assertKerberizedServerNotEnforced("msjava.management");
        }
        
        if (mbeanServer == null) {
            try {
                List<MBeanServer> mbeanServers = MSJmxUtils.findMBeanServer(null);
                if (mbeanServers != null && mbeanServers.size() > 0) {
                    mbeanServer = mbeanServers.get(0);
                }
            } catch (SecurityException se1) {
                LOGGER.warn("Do not have permssions to locate a MBeanServer, will try to get PlatformMBeanServer", se1);
            }
        }
        try {
            if (mbeanServer == null) {
                mbeanServer = MSJmxUtils.getPlatformMBeanServer();
            }
        } catch (SecurityException se2) {
            LOGGER.warn("Do not have the permssion to get PlatformMBeanServer, will create a new MBean Server", se2);
            
            mbeanServer = MSJmxUtils.createMBeanServer();
        }
        if (jmxConnectorServerEnv == null) {
            jmxConnectorServerEnv = new HashMap<String, Object>();
            jmxConnectorServerEnv.put(JMXMPConnectorServer.SERVER_ADDRESS_WILDCARD, String.valueOf(isWildcard()));
        }
        
        if (mbeanServerForwarder != null) {
            mbeanServerForwarder.setMBeanServer(mbeanServer);
            mbeanServerToUse = mbeanServerForwarder;
        } else {
            mbeanServerToUse = mbeanServer;
        }
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Initialized JMX connector '{}' with alwaysOn={}", toString(), alwaysOn);
        }
        if (alwaysOn) {
            start();
        }
    }
    @Override
    public boolean isAutoStartup() {
        return true;
    }
    @Override
    public int getPhase() {
        return DEFAULT_ORDER;
    }
    
    @Override
    public void start() {
        if (isRunning()) {
            return;
        } else if (!isInitialized.get()) {
            throw new IllegalStateException("Not initialized yet; consider relying on Spring to do the setup for you, or refer to the docs at initAndStart()");
        }
        try {
            String hostname = (host != null ? host : "localhost");
            JMXServiceURL serviceURL = createUrl(hostname, port, false);
            connectorServer = JMXConnectorServerFactory.newJMXConnectorServer(
                    serviceURL, jmxConnectorServerEnv, mbeanServerToUse);
            notifyConnectorServerConfigurers(connectorServer);
        } catch (IOException e) {
            throw new UncheckedIOException("Could not create JMX connector server", e);
        }
        try {
            try {
                connectorServer.start();
            } catch (IOException e) {
                throw new UncheckedIOException("Could not start JMX connector server", e);
            }
            
            JMXServiceURL address = connectorServer.getAddress();
            if (address != null) {
                port = address.getPort();
            }
            try {
                url = createUrl(host, port, true).toString();
            } catch (IOException e) {
                throw new UncheckedIOException("Could not create JMX service URL", e);
            }
        } catch (Exception e) {
            try {
                connectorServer.stop();
            } catch (Exception suppressed) {
                e.addSuppressed(suppressed);
            }
            connectorServer = null;
            throw e;
        }
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Started JMX connector '{}' on URL: '{}'", toString(), url);
        }
        publish();
    }
    
    public void initAndStart() throws Exception {
        init();
        start();
    }
    
    @Deprecated
    public void afterPropertiesSet() throws Exception {
        initAndStart();
    }
    private void publish() {
        if (servicePublicationSupport != null && publicationAttributes.get() != null) {
            servicePublicationSupport.publish();
        }
    }
    private void unpublish() {
        if (servicePublicationSupport != null) {
            servicePublicationSupport.unpublish();
        }
    }
    private final ServiceDescriptionBuilder descriptionBuilder = new ServiceDescriptionBuilder() {
        @Override
        public ServiceDescription buildServiceDescription() {
            if (publicationAttributes.get() == null)
                return null;
            try {
                String resolvedurl = publicationUrl();
                LOGGER.debug("Publication address: {}", resolvedurl);
                return new MapServiceDescription(publicationAttributes.get()).with(CommonKey.url, resolvedurl.toString());
            } catch (Exception e) {
                LOGGER.warn("Can't build service description: " + e);
                return null;
            }
        }
    };
    protected String publicationUrl() throws Exception {
        return createUrl(HostUtils.toExternalHostName(host), HostUtils.toExternalPort(port), false).toString();
    }
    private JMXServiceURL createUrl(String hostname, int port, boolean sideEffects) throws RemoteException, MalformedURLException {
        if (this.protocol == Protocol.JMXMPMSKERBEROS) {
            
            return new JMXServiceURL(JMXMPMSKerberosProtocolConstants.PROTOCOL, hostname, port);
        } else if (this.protocol == Protocol.JMXMP) {
            
            return new JMXServiceURL("jmxmp", hostname, port);
        } else {
            if (sideEffects) {
                SystemPropertyUtils.setProperty("java.rmi.server.randomIDs", "true", LOGGER);
                
                LocateRegistry.createRegistry(port);
            }
            
            return new JMXServiceURL("service:jmx:rmi:
        }
    }
    
    @Override
    public boolean isRunning() {
        return (connectorServer != null && connectorServer.isActive());
    }
    
    @PreDestroy
    public void forceStop() {
        if (!isRunning()) {
            return;
        }
        unpublish();
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Stopping JMX connector '{}' on URL: '{}'", toString(), url);
        }
        try {
            connectorServer.stop();
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to stop JMX connector", e);
        } finally {
            connectorServer = null;
            url = null;
        }
    }
    
    @Override
    public void stop() {
        if (!alwaysOn) {
            forceStop();
        }
    }
    @Override
    public void stop(Runnable callback) {
        try {
            stop();
        } finally {
            if (callback != null) {
                callback.run();
            }
        }
    }
    
    @Deprecated
    public void destroy() {
        stop();
    }
    protected void checkIsNotInitialized() throws IllegalStateException {
        if (isInitialized.get()) {
            throw new IllegalStateException("This bean is already initialized, you cannot change its settings anymore");
        }
    }
    public Map<String, Object> getJmxConnectorServerEnv() {
        return jmxConnectorServerEnv;
    }
    public void setJmxConnectorServerEnv(Map<String, Object> jmxConnectorServerEnv) {
        this.jmxConnectorServerEnv = jmxConnectorServerEnv;
    }
    boolean isWildcard() {
        if (this.host == null) {
            return true;
        }
        return false;
    }
    @Override
    public ServicePublisher getServicePublisher() {
        return servicePublicationSupport == null ? null : servicePublicationSupport.getPublisher();
    }
    @Override
    public void setServicePublisher(ServicePublisher servicePublisher) {
        if (isRunning()) {
            String alwaysOnMsg = alwaysOn
                    ? ("You have alwaysOn=true; if you are using Leader Election, do not add JMS Servers to the list of managed objects"
                    + " or add AbstractJMXServer to the LeaderElectorLifecycleManager.nonLeaderElectableTypes.")
                    : "";
            throw new IllegalStateException(
                    "This server is already started, you cannot change its settings anymore. " + alwaysOnMsg);
        }
        this.servicePublicationSupport = new ServicePublicationSupport(descriptionBuilder, servicePublisher);
    }
    public String getPublicationServiceName() {
        return ServiceName.forServiceAttributes(publicationAttributes.get());
    }
    
    public void addConnectorServerConfigurer(Consumer<JMXConnectorServer> configurer) {
        checkIsNotInitialized();
        if (!connectorServerConfigurers.contains(configurer)) {
            connectorServerConfigurers.add(configurer);
        }
    }
    private void notifyConnectorServerConfigurers(JMXConnectorServer connectorServer) {
        for (Consumer<JMXConnectorServer> configurer : connectorServerConfigurers) {
            configurer.accept(connectorServer);
        }
    }
    
    public void setPublicationServiceName(String serviceName) {
        publicationAttributes.mergeWith(serviceName);
    }
    public Map<String, Object> getPublicationAttributes() {
        return publicationAttributes.get();
    }
    public void setPublicationAttributes(Map<String, Object> attributes) {
        publicationAttributes.mergeWith(attributes);
    }
    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + protocol + ":
    }
}
