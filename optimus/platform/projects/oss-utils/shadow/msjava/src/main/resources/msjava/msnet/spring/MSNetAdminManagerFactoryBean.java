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
package msjava.msnet.spring;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import msjava.base.admin.MSAdminCommand;
import msjava.base.slr.internal.ServiceName;
import msjava.base.slr.internal.ServicePublisher;
import msjava.base.spring.lifecycle.MSComponentLifecyclePhases;
import msjava.base.spring.sr.LazyInitPublisher;
import msjava.base.sr.ServiceAttributes;
import msjava.base.sr.internal.PublicationAttributesSupport;
import msjava.base.util.internal.SystemPropertyUtils;
import msjava.management.jmx.authorization.FailSafeVoter;
import msjava.msnet.MSNetInetAddress;
import msjava.msnet.MSNetLoop;
import msjava.msnet.MSNetLoopThread;
import msjava.msnet.MSNetServerStartupPolicyEnum;
import msjava.msnet.admin.MSNetAdminManager;
import msjava.msnet.admin.MSNetAdminManagerDefaultImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.PriorityOrdered;
import org.springframework.security.access.AccessDecisionManager;
import org.springframework.security.access.AccessDecisionVoter;
import org.springframework.security.access.vote.AffirmativeBased;
import com.google.common.collect.Lists;
public class MSNetAdminManagerFactoryBean implements FactoryBean<MSNetAdminManager>,
        BeanNameAware, InitializingBean, DisposableBean, BeanPostProcessor,
        PriorityOrdered, ApplicationContextAware {
    
    
    
    
    
    
    private static final Logger LOGGER = LoggerFactory.getLogger(MSNetAdminManagerFactoryBean.class);
    public static final int DEFAULT_ORDER = MSComponentLifecyclePhases.ADMIN_MANAGER_PHASE;
    public static final String DEFAULT_SERVICE_NAME = SystemPropertyUtils.getProperty("msjava.adminManager.serviceName", "netadminServer", LOGGER);
    private String beanName;
    private final MSNetLoop adminNetLoop = makeAdminLoop();
    private MSNetAdminManager adminManager;
    private String hostPort = ":0";
    private boolean kerberos = true;
    protected boolean createDefaultCommands = true;
    private final AtomicBoolean isInitialized = new AtomicBoolean(false);
    private AccessDecisionVoter<?> accessDecisionVoter;
    private AccessDecisionManager accessDecisionManager;
    private MSNetServerStartupPolicyEnum startupFailurePolicy = MSNetServerStartupPolicyEnum.FAIL_ON_EXCEPTION;
    private ServicePublisher publisher;
    private final PublicationAttributesSupport publicationAttributes = new PublicationAttributesSupport().mergeWith(DEFAULT_SERVICE_NAME);
    protected boolean isInitialized() {
        return isInitialized.get();
    }
    protected void checkIsNotInitialized() throws IllegalStateException {
        if (isInitialized()) {
            throw new IllegalStateException("This bean is already initialized, you cannot change its settings anymore");
        }
    }
    @Override
    @SuppressWarnings("deprecation")
    public Object postProcessAfterInitialization(Object bean, String beanName)
            throws BeansException {
        if (bean instanceof MSAdminCommand) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER
                        .trace(
                                "Detected MSAdminCommand bean in Spring called {} to add to MSNetAdminManager",
                                beanName);
            }
            adminManager.addAdminCommand((MSAdminCommand) bean);
        }
        return bean;
    }
    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName)
            throws BeansException {
        if (bean instanceof AdminManagerAware) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER
                        .trace(
                                "Detected AdminManagerAware bean in Spring called {} to be injected with MSNetAdminManager",
                                beanName);
            }
            ((AdminManagerAware) bean).setAdminManager(adminManager);
        }
        return bean;
    }
    protected final MSNetLoop getNetLoop() {
        return adminNetLoop;
    }
    public void setPort(String port) {
        this.hostPort = ":" + port;
    }
    protected String getPort() {
        if (hostPort == null) {
            return null;
        } else {
            return hostPort.substring(hostPort.indexOf(':') + 1);
        }
    }
    @Override
    public void setBeanName(String name) {
        beanName = name;
    }
    protected String getBeanName() {
        return beanName;
    }
    public void setCreateDefaultCommands(boolean createDefaultCommands) {
        this.createDefaultCommands = createDefaultCommands;
    }
    
    protected MSNetAdminManager makeAdminManager() throws Exception {
        if (!kerberos)
        LOGGER.warn("Using a non-kerberized admin manager. The username asserted in the admin request will be used for all"
                + " authorization requests on this admin manager, without verifying actual identity. This allows an "
                + "unsafe use of netAdmin from a security standpoint and requires proper risk signoff.");
        return new MSNetAdminManager(new MSNetAdminManagerDefaultImpl(
                adminNetLoop,
                new MSNetInetAddress(hostPort),
                createDefaultCommands,
                startupFailurePolicy,
                null,
                kerberos,
                publicationAttributes.get(),
                publisher));
    }
    
    protected void validateProperties() {
        if (hostPort == null) {
            throw new IllegalArgumentException(
                    "Must set a 'port' property on admin manager bean '"
                            + beanName + "'");
        }
    }
    
    protected MSNetLoop makeAdminLoop() {
        MSNetLoopThread thread = new MSNetLoopThread("Admin Loop");
        thread.setDaemon(true);
        thread.setPriority(Thread.MAX_PRIORITY);
        return thread.startLoop();
    }
    @Override
    public MSNetAdminManager getObject() throws Exception  {
        afterPropertiesSet();
        return adminManager;
    }
    @Override
    public Class<? extends MSNetAdminManager> getObjectType() {
        return MSNetAdminManager.class;
    }
    @Override
    public boolean isSingleton() {
        return true;
    }
    @Override
    public final void destroy() throws Exception {
        try {
            shutDownAdminManager();
        } finally {
            adminNetLoop.quit();
        }
    }
    @Override
    public final int getOrder() {
        return DEFAULT_ORDER;
    }
    @Override
    public void afterPropertiesSet() throws Exception {
        if (!isInitialized.compareAndSet(false, true)) {
            return;
        }
        validateProperties();
        adminManager = makeAdminManager();
        if (this.accessDecisionManager != null) {
            adminManager
                    .setAccessDecisionManager(this.accessDecisionManager);
        } else if (this.accessDecisionVoter != null) {
            
            AffirmativeBased manager = new AffirmativeBased(Lists.<AccessDecisionVoter<?>>newArrayList(new FailSafeVoter(), this.accessDecisionVoter));
            adminManager.setAccessDecisionManager(manager);
            manager.afterPropertiesSet();
            this.accessDecisionManager = manager;
        } else {
            throw new IllegalStateException(
                    "Neither accessDecisionManager nor accessDecisionVoter is set. Set at least one of them");
        }
    }
    protected void shutDownAdminManager() {
        adminManager.serverShutdown();
    }
    protected final MSNetAdminManager getAdminManager() {
        return adminManager;
    }
    public void setAccessDecisionVoter(AccessDecisionVoter<?> accessDecisionVoter) {
        checkIsNotInitialized();
        this.accessDecisionVoter = accessDecisionVoter;
    }
    public void setAccessDecisionManager(
            AccessDecisionManager accessDecisionManager) {
        checkIsNotInitialized();
        this.accessDecisionManager = accessDecisionManager;
    }
    public void setStartupFailurePolicy(
            MSNetServerStartupPolicyEnum startupFailurePolicy) {
        this.startupFailurePolicy = startupFailurePolicy;
    }
    public MSNetServerStartupPolicyEnum getStartupFailurePolicy() {
        return startupFailurePolicy;
    }
    public void setKerberos(boolean kerberos) {
        this.kerberos = kerberos;
    }
    public boolean isKerberos() {
        return kerberos;
    }
    protected AccessDecisionManager getAccessDecisionManager() {
        return accessDecisionManager;
    }
    public void setHostPort(String hostPort) {
        this.hostPort = hostPort;
    }
    public String getHostPort() {
        return hostPort;
    }
    public void setServicePublisher(ServicePublisher pub) {
        this.publisher = pub;
    }
    public ServicePublisher getServicePublisher() {
        return publisher;
    }
    public String getPublicationServiceName() {
        return ServiceName.forServiceAttributes(publicationAttributes.get());
    }
    
    public void setPublicationServiceName(String serviceName) {
        publicationAttributes.mergeWith(serviceName);
    }
    public ServiceAttributes publicationAttributes() {
        return publicationAttributes.get();
    }
    public void setPublicationAttributes(Map<String, Object> attributes) {
        publicationAttributes.mergeWith(attributes);
    }
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        if (publisher == null)
            this.publisher = new LazyInitPublisher(applicationContext);
    }
}
