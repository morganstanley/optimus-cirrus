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
import java.nio.BufferUnderflowException;
import java.util.concurrent.Executor;
import msjava.base.slf4j.ContextLogger;
import msjava.zkapi.ZkaPathContext;
import msjava.zkapi.annotation.ZkaSyncOperation;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.SharedCountListener;
import org.apache.curator.framework.recipes.shared.SharedCountReader;
import org.apache.curator.framework.recipes.shared.VersionedValue;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
public final class ZkaSharedCount implements SharedCountReader, Closeable {
    private static final Logger logger = ContextLogger.safeLogger();
    private final SharedCount sharedCount;
    private final ZkaPathContext context;
    
    @ZkaSyncOperation
    public ZkaSharedCount(ZkaContext context, String path, int seedValue) throws Exception {
        this((ZkaPathContext) context, path, seedValue);
    }
    
    
    @ZkaSyncOperation
    public ZkaSharedCount(ZkaPathContext context, String path, int seedValue) throws Exception {
        final String countPath = ZKPaths.makePath(context.getBasePath(), path);
        logger.debug("Creating a shared count at {}", countPath);
        sharedCount = new SharedCount(context.getCurator(), countPath, seedValue);
        this.context = context;
        context.register(this);
        sharedCount.start();
        
        
        try {
            getCount();
        } catch (BufferUnderflowException | IndexOutOfBoundsException e) {
            
            
            
            logger.warn("Broken shared count node fount at " + countPath + ", overwriting with default value " + seedValue, e);
            setCount(seedValue);
        }
    }
    
    @ZkaSyncOperation
    public void setCount(int newCount) throws Exception {
        sharedCount.setCount(newCount);
    }
    
    @Override
    public int getCount() {
        return sharedCount.getCount();
    }
    @Override
    public VersionedValue<Integer> getVersionedValue() {
        return sharedCount.getVersionedValue();
    }
    
    @Override
    public void addListener(SharedCountListener listener) {
        sharedCount.addListener(listener);
    }
    @Override
    public void addListener(SharedCountListener listener, Executor executor) {
        sharedCount.addListener(listener, executor);
    }
    @Override
    public void removeListener(SharedCountListener listener) {
        sharedCount.removeListener(listener);
    }
    @Override
    public void close() throws IOException {
        try {
            sharedCount.close();
        } finally {
            context.unregister(this);
        }
    }
    
    @ZkaSyncOperation
    public static int incrementAndGet(ZkaSharedCount count) throws Exception {
        VersionedValue<Integer> versionedId = count.sharedCount.getVersionedValue();
        while(!count.sharedCount.trySetCount(versionedId, versionedId.getValue() + 1)) {
            versionedId = count.sharedCount.getVersionedValue();
        }
        return versionedId.getValue();
    }
}
