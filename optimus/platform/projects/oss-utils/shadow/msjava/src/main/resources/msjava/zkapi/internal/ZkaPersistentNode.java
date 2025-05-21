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
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import msjava.base.slf4j.ContextLogger;
import msjava.zkapi.ZkaAttr;
import msjava.zkapi.ZkaPathContext;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import com.google.common.base.Throwables;
public final class ZkaPersistentNode extends PersistentNode {
	private static final Logger log = ContextLogger.safeLogger();
    private final ZkaPathContext context;
    
    public ZkaPersistentNode(ZkaContext context, String path, ZkaData data) throws IOException, InterruptedException {
        this(context, path, data.serialize(), true);
    }
    
    public ZkaPersistentNode(ZkaContext context, String path, ZkaData data, boolean ensureCreate) throws IOException,
            InterruptedException {
        this(context, path, data.serialize(), ensureCreate);
    }
    public ZkaPersistentNode(ZkaPathContext context, String path, byte[] data, boolean ensureCreate) throws IOException,
        InterruptedException {
        super(context.getCurator(), CreateMode.EPHEMERAL_SEQUENTIAL, false, context.resolve(path), data);
        try {
            context.register(this);
            this.context = context;
            
            start();
            if (ensureCreate
                    && !waitForInitialCreate(context.getInteger(ZkaAttr.SESSION_TIMEOUT), TimeUnit.MILLISECONDS)) {
                throw new IOException("couldn't create persistent node in time");
            }
        } catch (InterruptedException | IOException | RuntimeException e) {
            log.error("Exception occured, while creating ephemeral node.", e);
            context.unregister(this);
            throw e;
        } catch (Exception e) {
            log.error("Exception occured, while creating ephemeral node.", e);
            context.unregister(this);
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            context.unregister(this);
        }
    }
    
    
    @Deprecated
    public long getId() {
        return 1L;
    }
    
}
