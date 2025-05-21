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
import java.util.WeakHashMap;
import org.aspectj.lang.Aspects;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.MBeanExporter;
import msjava.management.annotation.AutoExport;
@Aspect
public class AutoExporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(AutoExporter.class);
    private static final int MAX_PENDING_ENTRIES = 256;
    public static void registerForExport(Object managedResource) {
        Aspects.aspectOf(AutoExporter.class).export(managedResource);
    }
    
    private WeakHashMap<Object, Boolean> pendingRegistration = new WeakHashMap<>();
    private MBeanExporter exporter;
    
    @After("initialization((!msjava.management.jmx.AutoExporter && @msjava.management.annotation.AutoExport *)+.new(..)) && target(managedResource)")
    public void exporting(Object managedResource) {
        export(managedResource);
    }
    private synchronized void export(Object managedResource) {
        if (exporter != null) {
            exporter.registerManagedResource(managedResource);
            return;
        }
        if (pendingRegistration.size() > MAX_PENDING_ENTRIES) {
            LOGGER.warn("Pending registration entries exceeds quota! ({} > {}) You might be leaking memory",
                    pendingRegistration.size(), MAX_PENDING_ENTRIES);
        }
        pendingRegistration.put(managedResource, Boolean.TRUE);
    }
    synchronized void reset() {
        exporter = null;
        pendingRegistration.clear();
    }
    synchronized void setExporter(MBeanExporter exporter) {
        if (exporter == null)
            throw new IllegalArgumentException();
        this.exporter = exporter;
        for (Object managedResource : pendingRegistration.keySet()) {
            export(managedResource);
        }
        pendingRegistration.clear();
    }
}
