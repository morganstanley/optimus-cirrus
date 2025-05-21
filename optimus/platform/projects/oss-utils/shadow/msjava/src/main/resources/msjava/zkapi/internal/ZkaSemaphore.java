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

package msjava.zkapi.internal;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import msjava.base.slf4j.ContextLogger;
import msjava.zkapi.ZkaPathContext;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2;
import org.apache.curator.framework.recipes.shared.SharedCountReader;
import org.slf4j.Logger;
public class ZkaSemaphore extends InterProcessSemaphoreV2 implements Closeable {
    
    private static final Logger                    logger         = ContextLogger.safeLogger();
    public static final String                     SEMAPHORE_PATH = "/semaphore";
    public static final String                     LEASES_PATH    = "/semaphore/leases";
    public static final String                     LOCKS_PATH     = "/semaphore/locks";
    
    public static final Comparator<String> leaseSorter = Comparator.comparing(s -> s.split("-lease-")[1]);
    private final ZkaPathContext context;
    
    private final SharedCountReader count;
    
    
    public ZkaSemaphore(ZkaContext context, ZkaData data, SharedCountReader count) {
        this(context, data.serialize(), count);
    }
        
    public ZkaSemaphore(ZkaPathContext context, byte[] data, SharedCountReader count) {
        super(context.getCurator(), context.resolve(SEMAPHORE_PATH), count);
        this.count = count;
        setNodeData(data);
        context.register(this);
        this.context = context;
    }
    
    
    public Set<Map<String,Object>> getActiveLeases() throws IOException {
        Set<Map<String,Object>> data = new HashSet<Map<String,Object>>();
        try {
            
            if(context.exists(LEASES_PATH) == null) {
                return data;
            }
            
            List<String> leases = context.getChildren(LEASES_PATH);
            Collections.sort(leases, ZkaSemaphore.leaseSorter);
            
            
            
            int count = this.count.getCount() >= leases.size() ? leases.size() : leases.size() -1;
            for(String lease : leases) {
                
                if(count-- == 0) {
                    break;
                }
                try {
                    data.add(ZkaData.fromBytes(context.getNodeData(LEASES_PATH + "/" + lease)));
                } catch(Throwable t) {
                    logger.error("failed to find lease",t);
                }
            }
            
        } catch(Exception e) {
            logger.error("failed to find participating nodes",e);
            throw new IOException(e);
        }
        return data;
    }
    
    @Override
    public void close() throws IOException {
        context.unregister(this);
    }
}
