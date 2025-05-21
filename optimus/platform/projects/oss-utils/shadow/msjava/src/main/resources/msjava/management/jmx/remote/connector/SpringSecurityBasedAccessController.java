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
import java.io.ObjectInputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.InvalidAttributeValueException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.NotCompliantMBeanException;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.OperationsException;
import javax.management.QueryExp;
import javax.management.ReflectionException;
import javax.management.loading.ClassLoaderRepository;
import javax.management.remote.JMXPrincipal;
import javax.management.remote.MBeanServerForwarder;
import javax.management.remote.generic.JMXMPMSKerberosServerIntermediary;
import javax.security.auth.Subject;
import com.google.common.collect.ImmutableList;
import msjava.base.slf4j.ContextLogger;
import msjava.management.jmx.authorization.JMXAction;
import msjava.management.jmx.authorization.JMXAction.ActionType;
import org.slf4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.security.access.AccessDecisionManager;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;
import com.sun.jmx.mbeanserver.GetPropertyAction;
@SuppressWarnings("restriction")
public class SpringSecurityBasedAccessController implements MBeanServerForwarder, InitializingBean {
    private static final Logger logger = ContextLogger.safeLogger();
    private static final Method SUBJECT_CURRENT_METHOD;
    static {
        if (System.getProperty("java.version", "8").compareTo("18") >= 0) {
            try {
                SUBJECT_CURRENT_METHOD = Subject.class.getMethod("current");
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        } else {
            SUBJECT_CURRENT_METHOD = null;
        }
    }
    private MBeanServer mbs;
    
    private final Collection<JMXOperationExecutionListener> jmxListeners = new ArrayList<>();
    @Override
    public MBeanServer getMBeanServer() {
        return mbs;
    }
    @Override
    public void setMBeanServer(MBeanServer mbs) {
        if (mbs == null) {
            throw new IllegalArgumentException("Null MBeanServer");
        }
        if (this.mbs != null) {
            throw new IllegalArgumentException("MBeanServer object already initialized");
        }
        this.mbs = mbs;
    }
    private AccessDecisionManager accessDecisionManager;
    private final Principal defaultPrincipalToUse;
    
    static final Principal PRINCIPAL_NULL_NAME = new Principal() {
        @Override
        public final String getName() {
            return null;
        }
    };
    
    static final Principal PRINCIPAL_CURRENT_USERNAME = new Principal() {
        private final String userName = System.getProperty("user.name");
        @Override
        public final String getName() {
            return userName;
        }
    };
    private final boolean warnWhenSubjectNull;
    private final AtomicBoolean isConfigured = new AtomicBoolean(false);
    private final void throwIfNotConfigured() {
        if (!isConfigured.get()) {
            throw new IllegalStateException("This object is not fully configured yet, so cannot be used");
        }
    }
    public static final SpringSecurityBasedAccessController DEFAULT_TESTING_CONTROLLER = new SpringSecurityBasedAccessController(
            SpringSecurityBasedAccessController.PRINCIPAL_CURRENT_USERNAME, false);
    public static final SpringSecurityBasedAccessController DEFAULT_PRODUCTION_CONTROLLER = new SpringSecurityBasedAccessController();
    SpringSecurityBasedAccessController() {
        this(SpringSecurityBasedAccessController.PRINCIPAL_NULL_NAME, true);
    }
    SpringSecurityBasedAccessController(Principal defaultPrincipalToUse, boolean warnWhenSubjectNull) {
        if (defaultPrincipalToUse == null) {
            throw new IllegalArgumentException("Default Principal cannot be null");
        }
        this.defaultPrincipalToUse = defaultPrincipalToUse;
        this.warnWhenSubjectNull = warnWhenSubjectNull;
    }
    
    public AccessDecisionManager getAccessDecisionManager() {
        return accessDecisionManager;
    }
    
    public void setAccessDecisionManager(AccessDecisionManager accessDecisionManager) {
        if (isConfigured.get()) {
            throw new IllegalStateException("Cannot change the " + AccessDecisionManager.class.getName()
                    + " after initialization");
        }
        this.accessDecisionManager = accessDecisionManager;
    }
    
    public Collection<JMXOperationExecutionListener> getJMXOperationExecutionListeners() {
        return ImmutableList.copyOf(jmxListeners);
    }
    
    public void addJMXOperationExecutionListener(JMXOperationExecutionListener jmxListener) {
        jmxListeners.add(jmxListener);
    }
    
    public void removeJMXOperationExecutionListener(JMXOperationExecutionListener jmxListener) {
        jmxListeners.remove(jmxListener);
    }
    
    public void removeAllJMXOperationExecutionListeners() {
        jmxListeners.clear();
    }
    
    
    
    
    void throwIfNotAllowed(String action, ActionType actionType, ObjectName objectName, String jmxActionName) {
        JMXAction jmxAction = SpringJMXSecurityUtils.createJMXAction(action, actionType, objectName, jmxActionName);
        Principal jmxPrincipal = getJMXPrincipal();
        Authentication auth = new PreAuthenticatedAuthenticationToken(jmxPrincipal, null);
        boolean allowed = SpringJMXSecurityUtils.authorizeAction(jmxAction, objectName, auth, accessDecisionManager);
        if (!allowed) {
            throw new SecurityException("Access denied for subject: " + jmxPrincipal);
        }
    }
    
    void throwIfNotAllowed(String action, ActionType actionType) {
        throwIfNotAllowed(action, actionType, null, null);
    }
    private final Principal getJMXPrincipal() {
        AccessControlContext acc = AccessController.getContext();
        Subject subject;
        if (SUBJECT_CURRENT_METHOD != null) {
            try {
                subject = (Subject) SUBJECT_CURRENT_METHOD.invoke(null);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        } else {
            subject = Subject.getSubject(acc);
        }
        return getJMXPrincipal(subject);
    }
    
    private final Principal getJMXPrincipal(Subject subject) {
        
        
        
        if (subject == null) {
            
            subject = JMXMPMSKerberosServerIntermediary.currentSubject.get();
        }
        if (subject == null) {
            if (warnWhenSubjectNull && logger.isWarnEnabled()) {
                logger.warn("Given subject is null, this should never happen.");
            }
            return defaultPrincipalToUse;
        } else {
            Set<Principal> set = subject.getPrincipals();
            if (set != null) {
                for (Principal principal : set) {
                    if (principal != null && principal instanceof JMXPrincipal) {
                        return principal;
                    }
                }
            }
            if (logger.isWarnEnabled()) {
                logger.warn("No JMXPrincipal found for the given subject: '{}'. "
                        + "This should not have happened, at least 1 JMXPrincipal should exist", subject);
            }
            return defaultPrincipalToUse;
        }
    }
    private void clearSecurityContextHolder() {
        SecurityContextHolder.clearContext();
    }
    private void setupSecurityContextHolder() {
        SecurityContextHolder.getContext().setAuthentication(
                new PreAuthenticatedAuthenticationToken(this.getJMXPrincipal(), null));
    }
    
    
    
    
    @Override
    public void afterPropertiesSet() {
        if (isConfigured.compareAndSet(false, true)) {
            if (accessDecisionManager == null) {
                throw new IllegalStateException(AccessDecisionManager.class.getSimpleName() + " should be set");
            }
            
            
            
            
        } else {
            throw new IllegalStateException(SpringSecurityBasedAccessController.class.getSimpleName() + "'"
                    + System.identityHashCode(this) + "' already configured");
        }
    }
    
    
    
    
    @Override
    public Object invoke(ObjectName objectName, String operationName, Object[] params, String[] signature)
            throws InstanceNotFoundException, MBeanException, ReflectionException {
        
        Collection<JMXOperationExecutionListener> jmxListenersCopy = getJMXOperationExecutionListeners();
        long time = System.currentTimeMillis();
        
        String userName = getJMXPrincipal().getName();
        String connectionId = null;
        String address = null;
        int port = 0;
        JMXMPMSKerberosServerIntermediary.ConnectionInfo connectionInfo = JMXMPMSKerberosServerIntermediary.getCurrentConnectionInfo();
        if(connectionInfo != null) {
            connectionId = connectionInfo.getConnectionId();
            address = connectionInfo.getAddress();
            port = connectionInfo.getPort();
        }
        
        JMXOperation jmxOperation = new JMXOperation(time, userName, connectionId, address, port, operationName,
                params);
        JMXOperationState jmxOperationState = JMXOperationState.ABOUT_TO_EXECUTE;
        try {
            boolean executeJMXOperation;
            try {
                executeJMXOperation = executeBeforeInvocationCallback(jmxOperation, jmxListenersCopy);
            } catch (Throwable th) {
                
                jmxOperationState = JMXOperationState.EXECUTION_ABORTED;
                throw th;
            }
			if(!executeJMXOperation) {
				
                
                jmxOperationState = JMXOperationState.EXECUTION_ABORTED;
                throw new RuntimeException(
                        "Execution of JMX operation aborted because one (or more) beforeInvocation callback(s) returned false");
			}
			
			try {
		        throwIfNotConfigured();
		        checkOperationAccess(objectName, operationName, signature);
		        checkMLetMethods(objectName, operationName);
		        try {
		            setupSecurityContextHolder();
		            Object retValue = getMBeanServer().invoke(objectName, operationName, params, signature);
                    jmxOperationState = JMXOperationState.EXECUTION_SUCCESSFUL;
		            return retValue;
		        } finally {
		            clearSecurityContextHolder();
		        }
			} catch(Throwable th) {
                jmxOperationState = JMXOperationState.EXECUTION_FAILED;
	        	
	        	
	        	throw th;
	        }
    	} finally {
    		executeAfterInvocationCallback(jmxListenersCopy, jmxOperation, jmxOperationState);
    	}
    }
	
	private boolean executeBeforeInvocationCallback(JMXOperation jmxOperation, Collection<JMXOperationExecutionListener> jmxListenersCopy) {
		boolean executeJMXOperation = true;
        for (final JMXOperationExecutionListener currentJmxListener : jmxListenersCopy) {
            boolean execute;
            try {
                execute = currentJmxListener.beforeInvocation(jmxOperation);
            } catch(Throwable th) {
                
                logger.debug("Executing the beforeInvocation callback threw exception", th);
                execute = false;
            }
            if (!execute) {
                executeJMXOperation = false;
            }
        }
		return executeJMXOperation;
	}
    private void executeAfterInvocationCallback(Collection<JMXOperationExecutionListener> jmxListenersCopy,
                                                JMXOperation jmxOperation, JMXOperationState jmxOperationState) {
        for (final JMXOperationExecutionListener currentJmxListener : jmxListenersCopy) {
            currentJmxListener.afterInvocation(jmxOperation, jmxOperationState);
        }
    }
    private void checkOperationAccess(ObjectName objectName, String operationName, String[] signature) {
        throwIfNotAllowed(asCamelCase("invoke"), getOperationActionType(objectName, operationName, signature),
                objectName, operationName);
    }
    private ActionType getOperationActionType(ObjectName objectName, String operationName, String[] signature) {
        try {
            
            MBeanInfo mbi = getMBeanServer().getMBeanInfo(objectName);
            if (mbi != null) {
                
                
                
                
                for (MBeanOperationInfo op : mbi.getOperations()) {
                    if (op.getName().equals(operationName) && equalSignatures(op.getSignature(), signature)) {
                        return op.getImpact() == MBeanOperationInfo.INFO ? ActionType.READONLY : ActionType.READWRITE;
                    }
                }
            }
        } catch (InstanceNotFoundException | IntrospectionException | ReflectionException e) {
            if (logger.isDebugEnabled())
                logger.warn("Error looking up MBeanInfo for [" + objectName + "]", e);
            else
                logger.warn("Error looking up MBeanInfo for [" + objectName + "], " + e);
        }
        return ActionType.READWRITE;
    }
    private boolean equalSignatures(MBeanParameterInfo[] opSig, String[] requestedSig) {
        if (requestedSig == null)
            return opSig.length==0;
        if (requestedSig.length != opSig.length)
            return false;
        for (int i = 0; i < requestedSig.length; i++) {
            if (!opSig[i].getType().equals(requestedSig[i]))
                return false;
        }
        return true;
    }
    
    @Override
    public Object getAttribute(ObjectName objectName, String attribute) throws MBeanException,
            AttributeNotFoundException, InstanceNotFoundException, ReflectionException {
        throwIfNotConfigured();
        throwIfNotAllowed(asCamelCase("getAttribute"), ActionType.READONLY, objectName, "get" + attribute);
        try {
            setupSecurityContextHolder();
            return getMBeanServer().getAttribute(objectName, attribute);
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    @Override
    public AttributeList getAttributes(ObjectName objectName, String[] attributes) throws InstanceNotFoundException,
            ReflectionException {
        throwIfNotConfigured();
        for (String attribute : attributes) {
            throwIfNotAllowed(asCamelCase("getAttributes"), ActionType.READONLY, objectName, "get" + attribute);
        }
        try {
            setupSecurityContextHolder();
            return getMBeanServer().getAttributes(objectName, attributes);
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    @Override
    public void setAttribute(ObjectName objectName, Attribute attribute) throws InstanceNotFoundException,
            AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
        throwIfNotConfigured();
        throwIfNotAllowed(asCamelCase("setAttribute"), ActionType.READWRITE, objectName, "set" + attribute.getName());
        try {
            setupSecurityContextHolder();
            getMBeanServer().setAttribute(objectName, attribute);
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    @Override
    public AttributeList setAttributes(ObjectName objectName, AttributeList attributes)
            throws InstanceNotFoundException, ReflectionException {
        throwIfNotConfigured();
        for (Attribute attribute : attributes.asList()) {
            throwIfNotAllowed(asCamelCase("setAttributes"), ActionType.READWRITE, objectName, "set"
                    + attribute.getName());
        }
        try {
            setupSecurityContextHolder();
            return getMBeanServer().setAttributes(objectName, attributes);
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    @Override
    public void addNotificationListener(ObjectName name, NotificationListener listener, NotificationFilter filter,
                                        Object handback) throws InstanceNotFoundException {
        throwIfNotConfigured();
        throwIfNotAllowed(asCamelCase("addNotificationListener"), ActionType.READONLY);
        try {
            setupSecurityContextHolder();
            getMBeanServer().addNotificationListener(name, listener, filter, handback);
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    @Override
    public void addNotificationListener(ObjectName name, ObjectName listener, NotificationFilter filter, Object handback)
            throws InstanceNotFoundException {
        throwIfNotConfigured();
        throwIfNotAllowed(asCamelCase("addNotificationListener"), ActionType.READONLY);
        try {
            setupSecurityContextHolder();
            getMBeanServer().addNotificationListener(name, listener, filter, handback);
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    @Override
    public ObjectInstance createMBean(String className, ObjectName name) throws ReflectionException,
            InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException, NotCompliantMBeanException {
        throwIfNotConfigured();
        throwIfNotAllowed(asCamelCase("createMBean"), ActionType.READWRITE);
        try {
            setupSecurityContextHolder();
            SecurityManager sm = System.getSecurityManager();
            if (sm == null) {
                Object object = getMBeanServer().instantiate(className);
                checkClassLoader(object);
                return getMBeanServer().registerMBean(object, name);
            } else {
                return getMBeanServer().createMBean(className, name);
            }
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    @Override
    public ObjectInstance createMBean(String className, ObjectName name, Object params[], String signature[])
            throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException,
            NotCompliantMBeanException {
        throwIfNotConfigured();
        throwIfNotAllowed(asCamelCase("createMBean"), ActionType.READWRITE);
        try {
            setupSecurityContextHolder();
            SecurityManager sm = System.getSecurityManager();
            if (sm == null) {
                Object object = getMBeanServer().instantiate(className, params, signature);
                checkClassLoader(object);
                return getMBeanServer().registerMBean(object, name);
            } else {
                return getMBeanServer().createMBean(className, name, params, signature);
            }
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    @Override
    public ObjectInstance createMBean(String className, ObjectName name, ObjectName loaderName)
            throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException,
            NotCompliantMBeanException, InstanceNotFoundException {
        throwIfNotConfigured();
        throwIfNotAllowed(asCamelCase("createMBean"), ActionType.READWRITE);
        try {
            setupSecurityContextHolder();
            SecurityManager sm = System.getSecurityManager();
            if (sm == null) {
                Object object = getMBeanServer().instantiate(className, loaderName);
                checkClassLoader(object);
                return getMBeanServer().registerMBean(object, name);
            } else {
                return getMBeanServer().createMBean(className, name, loaderName);
            }
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    @Override
    public ObjectInstance createMBean(String className, ObjectName name, ObjectName loaderName, Object params[],
                                      String signature[]) throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException,
            MBeanException, NotCompliantMBeanException, InstanceNotFoundException {
        throwIfNotConfigured();
        throwIfNotAllowed(asCamelCase("createMBean"), ActionType.READWRITE);
        try {
            setupSecurityContextHolder();
            SecurityManager sm = System.getSecurityManager();
            if (sm == null) {
                Object object = getMBeanServer().instantiate(className, loaderName, params, signature);
                checkClassLoader(object);
                return getMBeanServer().registerMBean(object, name);
            } else {
                return getMBeanServer().createMBean(className, name, loaderName, params, signature);
            }
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    @Override
    @SuppressWarnings("deprecation")
    public ObjectInputStream deserialize(ObjectName name, byte[] data) throws InstanceNotFoundException,
            OperationsException {
        throwIfNotConfigured();
        throwIfNotAllowed(asCamelCase("deserialize"), ActionType.READONLY);
        try {
            setupSecurityContextHolder();
            return getMBeanServer().deserialize(name, data);
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    @Override
    @SuppressWarnings("deprecation")
    public ObjectInputStream deserialize(String className, byte[] data) throws OperationsException, ReflectionException {
        throwIfNotConfigured();
        throwIfNotAllowed(asCamelCase("deserialize"), ActionType.READONLY);
        try {
            setupSecurityContextHolder();
            return getMBeanServer().deserialize(className, data);
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    @Override
    @SuppressWarnings("deprecation")
    public ObjectInputStream deserialize(String className, ObjectName loaderName, byte[] data)
            throws InstanceNotFoundException, OperationsException, ReflectionException {
        throwIfNotConfigured();
        throwIfNotAllowed(asCamelCase("deserialize"), ActionType.READONLY);
        try {
            setupSecurityContextHolder();
            return getMBeanServer().deserialize(className, loaderName, data);
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    @Override
    public ClassLoader getClassLoader(ObjectName loaderName) throws InstanceNotFoundException {
        throwIfNotConfigured();
        throwIfNotAllowed(asCamelCase("getClassLoader"), ActionType.READONLY);
        try {
            setupSecurityContextHolder();
            return getMBeanServer().getClassLoader(loaderName);
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    @Override
    public ClassLoader getClassLoaderFor(ObjectName mbeanName) throws InstanceNotFoundException {
        throwIfNotConfigured();
        throwIfNotAllowed(asCamelCase("getClassLoaderFor"), ActionType.READONLY);
        try {
            setupSecurityContextHolder();
            return getMBeanServer().getClassLoaderFor(mbeanName);
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    @Override
    public ClassLoaderRepository getClassLoaderRepository() {
        throwIfNotConfigured();
        throwIfNotAllowed(asCamelCase("getClassLoaderRepository"), ActionType.READONLY);
        try {
            setupSecurityContextHolder();
            return getMBeanServer().getClassLoaderRepository();
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    @Override
    public String getDefaultDomain() {
        throwIfNotConfigured();
        throwIfNotAllowed(asCamelCase("getDefaultDomain"), ActionType.READONLY);
        try {
            setupSecurityContextHolder();
            return getMBeanServer().getDefaultDomain();
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    @Override
    public String[] getDomains() {
        throwIfNotConfigured();
        throwIfNotAllowed(asCamelCase("getDomains"), ActionType.READONLY);
        try {
            setupSecurityContextHolder();
            return getMBeanServer().getDomains();
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    @Override
    public Integer getMBeanCount() {
        throwIfNotConfigured();
        throwIfNotAllowed(asCamelCase("getMBeanCount"), ActionType.READONLY);
        try {
            setupSecurityContextHolder();
            return getMBeanServer().getMBeanCount();
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    @Override
    public MBeanInfo getMBeanInfo(ObjectName name) throws InstanceNotFoundException, IntrospectionException,
            ReflectionException {
        throwIfNotConfigured();
        throwIfNotAllowed(asCamelCase("getMBeanInfo"), ActionType.READONLY);
        try {
            setupSecurityContextHolder();
            return getMBeanServer().getMBeanInfo(name);
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    @Override
    public ObjectInstance getObjectInstance(ObjectName name) throws InstanceNotFoundException {
        throwIfNotConfigured();
        throwIfNotAllowed(asCamelCase("getObjectInstance"), ActionType.READONLY);
        try {
            setupSecurityContextHolder();
            return getMBeanServer().getObjectInstance(name);
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    @Override
    public Object instantiate(String className) throws ReflectionException, MBeanException {
        throwIfNotConfigured();
        throwIfNotAllowed(asCamelCase("instantiate"), ActionType.READWRITE);
        try {
            setupSecurityContextHolder();
            return getMBeanServer().instantiate(className);
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    @Override
    public Object instantiate(String className, Object params[], String signature[]) throws ReflectionException,
            MBeanException {
        throwIfNotConfigured();
        throwIfNotAllowed(asCamelCase("instantiate"), ActionType.READWRITE);
        try {
            setupSecurityContextHolder();
            return getMBeanServer().instantiate(className, params, signature);
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    @Override
    public Object instantiate(String className, ObjectName loaderName) throws ReflectionException, MBeanException,
            InstanceNotFoundException {
        throwIfNotConfigured();
        throwIfNotAllowed(asCamelCase("instantiate"), ActionType.READWRITE);
        try {
            setupSecurityContextHolder();
            return getMBeanServer().instantiate(className, loaderName);
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    @Override
    public Object instantiate(String className, ObjectName loaderName, Object params[], String signature[])
            throws ReflectionException, MBeanException, InstanceNotFoundException {
        throwIfNotConfigured();
        throwIfNotAllowed(asCamelCase("instantiate"), ActionType.READWRITE);
        try {
            setupSecurityContextHolder();
            return getMBeanServer().instantiate(className, loaderName, params, signature);
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    @Override
    public boolean isInstanceOf(ObjectName name, String className) throws InstanceNotFoundException {
        throwIfNotConfigured();
        throwIfNotAllowed(asCamelCase("isInstanceOf"), ActionType.READONLY);
        try {
            setupSecurityContextHolder();
            return getMBeanServer().isInstanceOf(name, className);
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    @Override
    public boolean isRegistered(ObjectName name) {
        throwIfNotConfigured();
        throwIfNotAllowed(asCamelCase("isRegistered"), ActionType.READONLY);
        try {
            setupSecurityContextHolder();
            return getMBeanServer().isRegistered(name);
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    @Override
    public Set<ObjectInstance> queryMBeans(ObjectName name, QueryExp query) {
        throwIfNotConfigured();
        throwIfNotAllowed(asCamelCase("queryMBeans"), ActionType.READONLY);
        try {
            setupSecurityContextHolder();
            return getMBeanServer().queryMBeans(name, query);
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    @Override
    public Set<ObjectName> queryNames(ObjectName name, QueryExp query) {
        throwIfNotConfigured();
        throwIfNotAllowed(asCamelCase("queryNames"), ActionType.READONLY);
        try {
            setupSecurityContextHolder();
            return getMBeanServer().queryNames(name, query);
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    @Override
    public ObjectInstance registerMBean(Object object, ObjectName name) throws InstanceAlreadyExistsException,
            MBeanRegistrationException, NotCompliantMBeanException {
        throwIfNotConfigured();
        throwIfNotAllowed(asCamelCase("registerMBean"), ActionType.READWRITE);
        try {
            setupSecurityContextHolder();
            return getMBeanServer().registerMBean(object, name);
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    @Override
    public void removeNotificationListener(ObjectName name, NotificationListener listener)
            throws InstanceNotFoundException, ListenerNotFoundException {
        throwIfNotConfigured();
        throwIfNotAllowed(asCamelCase("removeNotificationListener"), ActionType.READONLY);
        try {
            setupSecurityContextHolder();
            getMBeanServer().removeNotificationListener(name, listener);
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    @Override
    public void removeNotificationListener(ObjectName name, NotificationListener listener, NotificationFilter filter,
                                           Object handback) throws InstanceNotFoundException, ListenerNotFoundException {
        throwIfNotConfigured();
        throwIfNotAllowed(asCamelCase("removeNotificationListener"), ActionType.READONLY);
        try {
            setupSecurityContextHolder();
            getMBeanServer().removeNotificationListener(name, listener, filter, handback);
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    @Override
    public void removeNotificationListener(ObjectName name, ObjectName listener) throws InstanceNotFoundException,
            ListenerNotFoundException {
        throwIfNotConfigured();
        throwIfNotAllowed(asCamelCase("removeNotificationListener"), ActionType.READONLY);
        try {
            setupSecurityContextHolder();
            getMBeanServer().removeNotificationListener(name, listener);
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    @Override
    public void removeNotificationListener(ObjectName name, ObjectName listener, NotificationFilter filter,
                                           Object handback) throws InstanceNotFoundException, ListenerNotFoundException {
        throwIfNotConfigured();
        throwIfNotAllowed(asCamelCase("removeNotificationListener"), ActionType.READONLY);
        try {
            setupSecurityContextHolder();
            getMBeanServer().removeNotificationListener(name, listener, filter, handback);
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    @Override
    public void unregisterMBean(ObjectName name) throws InstanceNotFoundException, MBeanRegistrationException {
        throwIfNotConfigured();
        throwIfNotAllowed(asCamelCase("unregisterMBean"), ActionType.READWRITE);
        try {
            setupSecurityContextHolder();
            getMBeanServer().unregisterMBean(name);
        } finally {
            clearSecurityContextHolder();
        }
    }
    
    
    
    private void checkClassLoader(Object object) {
        if (object instanceof ClassLoader)
            throw new SecurityException("Access denied! Creating an " + "MBean that is a ClassLoader "
                    + "is forbidden unless a security " + "manager is installed.");
    }
    private void checkMLetMethods(ObjectName name, String operation) throws InstanceNotFoundException {
        
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            return;
        }
        
        if (!operation.equals("addURL") && !operation.equals("getMBeansFromURL")) {
            return;
        }
        
        if (!getMBeanServer().isInstanceOf(name, "javax.management.loading.MLet")) {
            return;
        }
        
        if (operation.equals("addURL")) { 
            throw new SecurityException("Access denied! MLet method addURL "
                    + "cannot be invoked unless a security manager is installed.");
        } else { 
            
            
            
            
            
            final String propName = "jmx.remote.x.mlet.allow.getMBeansFromURL";
            GetPropertyAction propAction = new GetPropertyAction(propName);
            String propValue = AccessController.doPrivileged(propAction);
            boolean allowGetMBeansFromURL = "true".equalsIgnoreCase(propValue);
            if (!allowGetMBeansFromURL) {
                throw new SecurityException("Access denied! MLet method "
                        + "getMBeansFromURL cannot be invoked unless a "
                        + "security manager is installed or the system property "
                        + "-Djmx.remote.x.mlet.allow.getMBeansFromURL=true " + "is specified.");
            }
        }
    }
    private static String asCamelCase(String methodName) {
        return "JMX" + (methodName.charAt(0) + "").toUpperCase() + methodName.substring(1);
    }
}
