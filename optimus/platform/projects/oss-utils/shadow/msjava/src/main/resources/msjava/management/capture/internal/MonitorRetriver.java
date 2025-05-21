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

package msjava.management.capture.internal;
import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import msjava.management.Monitor;
import msjava.management.Monitorable;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.reflect.FieldSignature;
import org.aspectj.lang.reflect.MethodSignature;
public class MonitorRetriver {
    
    private CopyOnWriterMonitorHolder monitorHolderArray = new CopyOnWriterMonitorHolder();
    
    private final Class<? extends Monitor> monitorClass;
    
    private final Class<? extends Annotation> annotationClass;
    public MonitorRetriver(Class<? extends Monitor> monitorClass, Class<? extends Annotation> annotationClass) {
        this.monitorClass = monitorClass;
        this.annotationClass = annotationClass;
    }
    
    public Monitor getMonitorForJoinPoint(JoinPoint joinPoint) {
        Monitor result = (Monitor) monitorHolderArray.get(joinPoint.getStaticPart());
        if (result == null) {
            
            result = getMonitorForJoinPointFromMonitoredObject(joinPoint);
            monitorHolderArray.set(joinPoint.getStaticPart(), result);
        }
        return result;
    }
    
    private Monitor getMonitorForJoinPointFromMonitoredObject(JoinPoint joinPoint) {
        Monitorable monitorable = getMonitorableFromJoinPoint(joinPoint);
        String monitorID = getMonitorIdFromJoinPoint(joinPoint);
        return monitorable.getMonitorAgent().getMonitor(monitorID, monitorClass);
    }
    
    private String getMonitorIdFromJoinPoint(JoinPoint joinPoint) {
        Signature sig = joinPoint.getSignature();
        AnnotatedElement annotated = null;
        if (sig instanceof FieldSignature) {
            annotated = ((FieldSignature) sig).getField();
        } else if (sig instanceof MethodSignature) {
            annotated = ((MethodSignature) sig).getMethod();
        } else {
            throw new IllegalStateException("The executing joinpoint is neither a FieldSignature nor a MethodSignature");
        }
        if (!annotated.isAnnotationPresent(annotationClass)) {
            throw new IllegalStateException("The executing joinpoint is not annotated with "
                    + annotationClass.getClass().getName());
        }
        Annotation annotation = annotated.getAnnotation(annotationClass);
        try {
            Method monitorIDGetter = annotation.annotationType().getMethod("monitorID");
            return (String) monitorIDGetter.invoke(annotation);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
    
    protected Monitorable getMonitorableFromJoinPoint(JoinPoint joinPoint) {
        return (Monitorable) joinPoint.getThis();
    }
}
