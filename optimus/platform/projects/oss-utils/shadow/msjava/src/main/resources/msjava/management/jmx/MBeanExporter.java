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
package msjava.management.jmx;
import java.util.Collection;
import java.util.Set;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotificationEmitter;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.modelmbean.ModelMBean;
import msjava.base.jmx.MBeanServerAware;
import msjava.base.jmx.MBeanServerReferencingBean;
import msjava.management.MonitoringManager;
import msjava.management.annotation.ManagedChildResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.jmx.export.MBeanExportException;
import org.springframework.jmx.export.annotation.AnnotationJmxAttributeSource;
import org.springframework.jmx.export.annotation.AnnotationMBeanExporter;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.jmx.export.naming.SelfNaming;
public class MBeanExporter extends AnnotationMBeanExporter implements MBeanServerAware, MBeanServerReferencingBean, SmartInitializingSingleton {
    static final String LOGBACK_MBEAN_CLASS_NAME = "msjava.logbackutils.jmx.MSJMXConfigurator";
    private static final Logger LOGGER = LoggerFactory.getLogger(MBeanExporter.class);
    
    private boolean exposeManagedResourceClassLoader = true;
    
    private boolean autoexport;
    
    private ListableBeanFactory beanFactory;
    
    private ManagedChildExporter managedChildExporter = new ManagedChildExporter(this);
    
    private MonitoringManager monitoringManager = MonitoringManager.getInstance();
    
    
    private ManagedNotificationEmitterRegistry managedNotificationEmitterRegistry = ManagedNotificationEmitterRegistryFactoryBean.getInstance();
    private final AnnotationJmxAttributeSource annotationSource =
            new AnnotationJmxAttributeSource();
    
    public MBeanExporter() {
        super();
        setAssembler(new ReadOperationMBeanInfoAssembler(annotationSource));
    }
    
    @Override
    public void setBeanFactory(BeanFactory beanFactory) {
        super.setBeanFactory(beanFactory);
        this.beanFactory = (ListableBeanFactory) beanFactory;
        this.annotationSource.setBeanFactory(beanFactory);
    }
    public void setAutoexport(boolean autoexport) {
        this.autoexport = autoexport;
    }
    
    
    
    @Override
    public void setExposeManagedResourceClassLoader(boolean exposeManagedResourceClassLoader) {
        super.setExposeManagedResourceClassLoader(exposeManagedResourceClassLoader);
        this.exposeManagedResourceClassLoader = exposeManagedResourceClassLoader;
    }
    
    @Override
    protected ModelMBean createModelMBean() throws MBeanException {
        return exposeManagedResourceClassLoader ? new ClassLoaderSetterMonitoringModelMBean(getMonitoringManager()) : new MonitoringModelMBean(
                getMonitoringManager());
    }
    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();
        if (autoexport) {
            managedChildExporter.enableAutoExport();
        }
        
    }
    @Override
    public void afterSingletonsInstantiated() {
    	super.afterSingletonsInstantiated();
    	exposeLogbackMBeanIfNotAlreadyExposed();
        exposeMonitoringManagerIfNotAlreadyExposed();
    }
    @Override
    public void destroy() {
        super.destroy();
        managedChildExporter.cleanAutoExport();
    }
    
    MonitoringManager getMonitoringManager() {
        return monitoringManager;
    }
    
    @Override
    protected ObjectName registerBeanNameOrInstance(Object mapValue, String beanKey) throws MBeanExportException {
        ObjectName objectName = super.registerBeanNameOrInstance(mapValue, beanKey);
        if (mapValue instanceof String) {
            String beanName = (String) mapValue;
            
            postRegisterBeanName(objectName, beanName);
            
        } else {
            postRegisterInstance(mapValue, objectName);
        }
        return objectName;
    }
    private void postRegisterBeanName(ObjectName objectName, String beanName) {
        Class<?> beanType = this.beanFactory.getType(beanName);
        if (managedChildExporter.hasManagedComposite(beanType)) {
            
            managedChildExporter.registerAllAccessibleManagedComposites(objectName, this.beanFactory
                    .getBean(beanName));
        }
        
        registerManagedNotificationEmitter(this.beanFactory.getBean(beanName));
    }
    
    
    private void postRegisterInstance(Object mapValue, ObjectName objectName) {
        managedChildExporter.registerAllAccessibleManagedComposites(objectName, mapValue);
        registerManagedNotificationEmitter(mapValue);
    }
    
    @Override
    public void registerManagedResource(Object managedResource, ObjectName objectName) throws MBeanExportException {
        super.registerManagedResource(managedResource, objectName);
        postRegisterInstance(managedResource, objectName);
    }
    
    private void registerManagedNotificationEmitter(Object managedResource) {
        if(managedNotificationEmitterRegistry != null) {
            exposeManagedNotificationEmitters(managedNotificationEmitterRegistry.register(managedResource));
        }
    }
    void registerManagedResourceWithoutLookingAtManagedChildObjects(Object managedResource, ObjectName objectName)
            throws MBeanExportException {
        super.registerManagedResource(managedResource, objectName);
    }
    
    @Override
    protected ObjectName getObjectName(Object bean, String beanKey) throws MalformedObjectNameException {
        return super.getObjectName(bean, beanKey);
    }
    @Override
    public void setMBeanServer(MBeanServer server) {
        this.setServer(server);
    }
    @Override
    public MBeanServer getMBeanServer() {
        return this.getServer();
    }
    boolean exposeMonitoringManagerIfNotAlreadyExposed() {
        try {
            ObjectName objectName = MonitoringManager.getInstance().getObjectName();
            if (!getMBeanServer().isRegistered(objectName)) {
                registerManagedResource(MonitoringManager.getInstance(), objectName);
                return true;
            }
        } catch (Exception e) {
            LOGGER.warn("There was an exception while registering MonitoringManager", e);
        }
        return false;
    }
    private boolean isLogbackJMXOnClasspath() {
        try {
            Class.forName("ch.qos.logback.classic.jmx.JMXConfigurator");
        } catch (ClassNotFoundException ex) {
            return false;
        }
        return true;
    }
    boolean exposeLogbackMBeanIfNotAlreadyExposed() {
        if(!isLogbackJMXOnClasspath()) {
            return false;
        }
        if(isLogbackMBeanRegistered()) {
            return false;
        }
        
        Class<?> logbackMBeanClass = null;
        try {
           logbackMBeanClass = Class.forName(LOGBACK_MBEAN_CLASS_NAME, true, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException cnfe){
            
            return false;
        } catch (Exception e) {
            LOGGER.error("Error initalizing " + LOGBACK_MBEAN_CLASS_NAME, e);
            return false;
        }
                    
        try{
            Object logbackMBean = logbackMBeanClass.newInstance();
            if (logbackMBean instanceof SelfNaming) {
                SelfNaming selfNamed = (SelfNaming) logbackMBean;
                if (!getMBeanServer().isRegistered(selfNamed.getObjectName())) {
                    registerManagedResource(selfNamed, selfNamed.getObjectName());
                    return true;
                } else {
                    LOGGER.debug("Logback MBean already registered");
                }
            } else {
                LOGGER.warn("Logback MBean does not implement self naming, hence not registered");
            }
        } catch (Throwable e) {
            LOGGER.error("There was an exception with registering", e);
        }
        return false;
    }
    private boolean isLogbackMBeanRegistered() {
        if(getMBeanServer() != null) {
            Set<ObjectInstance> mbeans = getMBeanServer().queryMBeans(null, null);
            for(ObjectInstance mbean : mbeans) {
                if(LOGBACK_MBEAN_CLASS_NAME.equals(mbean.getClassName())) {
                    return true;
                }
            }
        }
        return false;
    }
    
    public ManagedNotificationEmitterRegistry getManagedNotificationEmitterRegistry() {
        return managedNotificationEmitterRegistry;
    }
    
    public void setManagedNotificationEmitterRegistry(ManagedNotificationEmitterRegistry registry) {
        this.managedNotificationEmitterRegistry = registry;
    }
    
    private void exposeManagedNotificationEmitters(Collection<NotificationEmitter> emitters) {
            for(NotificationEmitter emitter : emitters) {
                if(isMBean(emitter.getClass()) || isManagedResource(emitter.getClass())) {
                    try {
                        registerManagedResource(emitter);
                    } catch (MBeanExportException ex) {
                        logger.error(ex);
                    }
                } else {
                    logger.info(emitter + " isn't neither a MBean nor a ManagedResource, skipping exposing.");
                }
            }
        
    }
    private boolean isManagedResource(Class<? extends NotificationEmitter> clazz) {
        return clazz.isAnnotationPresent(ManagedResource.class);
    }
}
