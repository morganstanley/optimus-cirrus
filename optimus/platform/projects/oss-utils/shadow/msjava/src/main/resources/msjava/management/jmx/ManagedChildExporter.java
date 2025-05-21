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
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.aspectj.lang.Aspects;
import org.aspectj.lang.NoAspectBoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.MBeanExportException;
import msjava.management.annotation.ManagedChildResource;
import msjava.management.jmx.internal.ManagedChildResourceObjectNameResolver;
class ManagedChildExporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(ManagedChildExporter.class);
    
    private static class CyclicDependencyDetector {
        private Map<Object, Object> registeredObjects = Collections
                .synchronizedMap(new IdentityHashMap<Object, Object>());
        public CyclicDependencyDetector(Object root) {
            register(root);
        }
        public void register(Object object) {
            registeredObjects.put(object, object);
        }
        public boolean hasRegistered(Object object) {
            return registeredObjects.containsKey(object);
        }
    }
    
    private final MBeanExporter exporter;
    
    private ManagedChildResourceObjectNameResolver managedCompositeObjectNameResolver = new ManagedChildResourceObjectNameResolver();
    
    
    private AutoExporter autoExporter;
    
    public ManagedChildExporter(MBeanExporter exporter){
        this.exporter=exporter;
    }
    
    
    public void enableAutoExport() {
        try {
            autoExporter = Aspects.aspectOf(AutoExporter.class);
            autoExporter.setExporter(exporter);
        } catch (NoAspectBoundException ex) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER
                        .warn(
                                "Unable to load "
                                        + AutoExporter.class.getName()
                                        + " aspect.\n Please make sure you've used this aspect in compile time weaving or put it into aop.xml for load time weaving.",
                                ex);
            }
        }
    }
    
    
    public void cleanAutoExport() {
        if(autoExporter!=null){        
            autoExporter.reset();
        }
    }
    
    
    public void registerAllAccessibleManagedComposites(ObjectName objectName, Object root) {
        CyclicDependencyDetector detector = new CyclicDependencyDetector(root);
        registerManagedComposite(objectName, root, detector);
    }
    
    @SuppressWarnings("unchecked")
    private void registerManagedComposite(ObjectName parentObjectName, Object parent, CyclicDependencyDetector detector) {
        List<Field> annotatedField = getFieldsAnnotatedWithManagedComposite(parent.getClass(), new ArrayList<Field>());
        for (Field field : annotatedField) {
            try {
                Object child = field.get(parent);
                if (child != null) {
                    ManagedChildResource managedResource = field.getAnnotation(ManagedChildResource.class);
                    if (Collection.class.isInstance(child)) {
                        Collection childrenToBeRegistered = (Collection) child;
                        for (Object childToBeRegistered : childrenToBeRegistered) {
                            
                            try {
                                doRegisterManagedComposite(managedResource, parentObjectName, parent,
                                        childToBeRegistered, detector);
                            } catch (MBeanExportException ex) {
                                if (LOGGER.isWarnEnabled()) {
                                    LOGGER.warn(ex.getMessage(), ex);
                                }
                            }
                        }
                    } else {
                        try {
                            doRegisterManagedComposite(managedResource, parentObjectName, parent, child, detector);
                        } catch (MBeanExportException ex) {
                            if (LOGGER.isWarnEnabled()) {
                                LOGGER.warn(ex.getMessage(), ex);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("Failed to expose field : " + field.getName(), e);
                }
            }
        }
    }
    
    private void doRegisterManagedComposite(ManagedChildResource managedComposite, ObjectName parentObjectName,
            Object parent, Object child, CyclicDependencyDetector detector) throws MBeanExportException {
        if (!detector.hasRegistered(child)) {
            detector.register(child);
            ObjectName childObjectName = managedCompositeObjectNameResolver.resolve(managedComposite.value(),
                    parentObjectName, parent, child);
            if (childObjectName == null) {
                try {
                    childObjectName = exporter.getObjectName(child, null);
                } catch (MalformedObjectNameException e) {
                    throw new MBeanExportException("Unable to get a valid ObjectName for " + child, e);
                }
            }
            
            if(childObjectName == null ) {
                this.LOGGER.warn("ManagedChildResource {} has not been registerd due to null ObjectName!", child);
                return;
            }
            
            try {
                exporter.registerManagedResourceWithoutLookingAtManagedChildObjects(child, childObjectName);
                registerManagedComposite(childObjectName, child, detector);
            } catch (MBeanExportException e) {
                throw new MBeanExportException("Unable to get a valid ObjectName for " + child, e);
            }
        } else {
            throw new MBeanExportException("Unable to register " + child + " because of cyclic dependency");
        }
    }
    private List<Field> getFieldsAnnotatedWithManagedComposite(Class<?> clazz, List<Field> annotatedField) {
        if (clazz == null) {
            return annotatedField;
        }
        for (Field f : clazz.getDeclaredFields()) {
            if (f.isAnnotationPresent(ManagedChildResource.class)) {
                try {
                    f.setAccessible(true);
                    annotatedField.add(f);
                } catch (SecurityException se) {
                    LOGGER.warn("Failed to expose field: {} because the security manager does not allow accessing it.",
                            f.getName());
                }
            }
        }
        return getFieldsAnnotatedWithManagedComposite(clazz.getSuperclass(), annotatedField);
    }
    public boolean hasManagedComposite(Class<?> clazz) {
        if (getFieldsAnnotatedWithManagedComposite(clazz, new ArrayList<Field>()).size() > 0) {
            return true;
        } else {
            return false;
        }
    }
}
