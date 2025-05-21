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
import java.util.Arrays;
import msjava.management.Monitor;
import org.aspectj.lang.JoinPoint.StaticPart;
class CopyOnWriterMonitorHolder {
    
    private static class MonitorHolder {
        public MonitorHolder(StaticPart staticPart, Monitor monitor) {
            this.staticPart = staticPart;
            this.monitor = monitor;
        }
        
        final StaticPart staticPart;
        
        final Monitor monitor;
        
        volatile MonitorHolder next = null;
    }
    
    private volatile MonitorHolder[] array = new MonitorHolder[0];
    
    private final MonitorHolder[] getArray() {
        return array;
    }
    
    private final void setArray(MonitorHolder[] array) {
        this.array = array;
    }
    
    public Monitor get(StaticPart staticPart) {
        int i = staticPart.getId();
        MonitorHolder[] currentArray = getArray();
        if (currentArray.length <= i) {
            return null;
        } else {
            return getMonitorFromHolderChain(currentArray[i], staticPart);
        }
    }
    
    private Monitor getMonitorFromHolderChain(MonitorHolder holder, StaticPart staticPart) {
        while (holder != null) {
            if (holder.staticPart == staticPart) {
                return holder.monitor;
            }
            holder = holder.next;
        }
        return null;
    }
    
    public synchronized void set(StaticPart staticPart, Monitor monitor) {
        int i = staticPart.getId();
        MonitorHolder[] currentArray = getArray();
        if (currentArray.length > i) {
            if (currentArray[i] == null) {
                currentArray[i] = new MonitorHolder(staticPart, monitor);
            } else {
                addMonitorToHolderChain(currentArray[i], staticPart, monitor);
            }
        } else {
            MonitorHolder[] newArray = Arrays.copyOf(currentArray, i + 1);
            newArray[i] = new MonitorHolder(staticPart, monitor);
            setArray(newArray);
        }
    }
    
    private void addMonitorToHolderChain(MonitorHolder monitorHolder, StaticPart staticPart, Monitor monitor) {
        do {
            if (monitorHolder.staticPart == staticPart && monitorHolder.monitor == monitor) {
                return;
            }
        } while (monitorHolder.next != null);
        monitorHolder.next = new MonitorHolder(staticPart, monitor);
    }
}
