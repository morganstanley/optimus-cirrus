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
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jmx.export.MBeanExportException;
import org.springframework.jmx.export.naming.SelfNaming;
public class ManagedChildResourceObjectNameResolver {
    private ObjectNamePatternResolver patternResolver = new ObjectNamePatternResolver();
    
    
    public ObjectName resolve(String pattern, ObjectName ownerName, Object parent, Object child)
            throws MBeanExportException {
        
        if (child instanceof SelfNaming) {
            try {
                return ((SelfNaming) child).getObjectName();
            } catch (MalformedObjectNameException e) {
                throw new MBeanExportException("ObjecName is not valid by SelfNaming ", e);
            }
        } else if (StringUtils.isNotBlank(pattern)) {
            String name = patternResolver.resolve(pattern, parent, child);
            try {
                if (name.indexOf(":") != -1) {
                    
                    return new ObjectName(name);
                } else {
                    
                    return new ObjectName(ownerName.getDomain() + ":" + name);
                }
            } catch (MalformedObjectNameException e) {
                throw new MBeanExportException("Unable to resolve pattern " + pattern, e);
            }
        }  else {
            return null;
        }
    }
}
