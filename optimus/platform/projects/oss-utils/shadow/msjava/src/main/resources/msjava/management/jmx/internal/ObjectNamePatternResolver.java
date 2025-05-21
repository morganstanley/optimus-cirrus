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

package msjava.management.jmx.internal;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.WordUtils;
import org.springframework.jmx.export.MBeanExportException;
import org.springframework.util.ReflectionUtils;
public class ObjectNamePatternResolver {
    
    public String resolve(String pattern, Object parent, Object child) throws MBeanExportException {
        if (StringUtils.isBlank(pattern)) {
            throw new MBeanExportException("Pattern is blank.");
        }
        StringBuilder objectName = new StringBuilder();
        StringBuilder variable = new StringBuilder();
        int state = 0;
        for (int i = 0; i < pattern.length(); ++i) {
            char c = pattern.charAt(i);
            switch (state) {
            case 0:
                if (c == '#') {
                    state = 1;
                } else {
                    objectName.append(c);
                }
                break;
            case 1:
                if (c == '{') {
                    state = 2;
                } else {
                    objectName.append('#');
                    objectName.append(c);
                    state = 0;
                }
                break;
            case 2:
                if (c == '}') {
                    String variableName = variable.toString().trim();
                    if (variableName.startsWith("PARENT.")) {
                        objectName.append(resolveVariable(variableName.substring(7), parent));
                    } else if (variableName.startsWith("CHILD.")) {
                        objectName.append(resolveVariable(variableName.substring(6), child));
                    } else {
                        objectName.append(resolveVariable(variableName, parent));
                    }
                    variable = new StringBuilder();
                    state = 0;
                } else {
                    variable.append(c);
                }
                break;
            }
        }
        if (state == 1) {
            
            objectName.append('#');
        } else if (state == 2) {
            throw new MBeanExportException("Bad Pattern: #{ is missing } to close the variable.");
        }
        return objectName.toString();
    }
    private String resolveVariable(String ognl, Object root) throws MBeanExportException {
        String[] accessSegments = ognl.split("\\.");
        Object currentObject = root;
        StringBuilder currentResolving = new StringBuilder();
        for (String propName : accessSegments) {
            currentResolving.append(propName);
            if (currentObject == null) {
                throw new MBeanExportException("target object is null when resolving " + currentResolving.toString());
            }
            try {
                currentObject = resolveProperty(propName, currentObject);
            } catch (MBeanExportException ex) {
                throw new MBeanExportException("exception happens when resolving " + currentResolving.toString(), ex);
            }
            currentResolving.append(".");
        }
        return String.valueOf(currentObject);
    }
    
    private Object resolveProperty(String varName, Object target) throws MBeanExportException {
        
        String getterName = "get" + WordUtils.capitalize(varName);
        Method accessMethod = ReflectionUtils.findMethod(target.getClass(), getterName);
        if (accessMethod == null) {
            
            accessMethod = ReflectionUtils.findMethod(target.getClass(), "is" + WordUtils.capitalize(varName));
        }
        if (accessMethod != null) {
            try {
                ReflectionUtils.makeAccessible(accessMethod);
                return accessMethod.invoke(target);
            } catch (Exception e) {
                throw new MBeanExportException("Unable to get property by invoking method " + accessMethod.getName(), e);
            }
        } else {
            
            Field accessField = ReflectionUtils.findField(target.getClass(), varName);
            if (accessField != null) {
                try {
                    ReflectionUtils.makeAccessible(accessField);
                    return accessField.get(target);
                } catch (Exception e) {
                    throw new MBeanExportException("Unable to get value of field " + accessField.getName(), e);
                }
            } else {
                throw new MBeanExportException("Property " + varName + " is not found in " + target);
            }
        }
    }
}
