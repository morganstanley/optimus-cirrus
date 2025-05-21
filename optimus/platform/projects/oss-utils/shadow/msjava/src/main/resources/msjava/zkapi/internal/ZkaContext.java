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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import msjava.base.slf4j.ContextLogger;
import msjava.zkapi.ZkaConfig;
import msjava.zkapi.ZkaPathContext;
import msjava.zkapi.annotation.ZkaAsyncOperation;
import msjava.zkapi.annotation.ZkaSyncOperation;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import com.google.common.annotations.VisibleForTesting;
public class ZkaContext extends ZkaPathContext {
    
    private final static Logger log = ContextLogger.safeLogger();
    public ZkaContext(ZkaConfig config) {
        super(config,null);
        setSelfDestructing(true);
    }
    public ZkaContext(ZkaConfig config, CuratorFramework curator) {
        super(config, curator);
        setSelfDestructing(true);
    }
    
    public static ZkaContext contextForSubPath(ZkaContext context, String subPath) {
        return new ZkaContext(context, ZKPaths.makePath(context.getBasePath(), subPath), null);
    }
    @ZkaAsyncOperation
    
    @Deprecated
    public ZkaContext(ZkaContext context, String subPath) {
        this(context, ZKPaths.makePath(context.getBasePath(), subPath), null);
        setSelfDestructing(true);
        context.createInBackground(new ZkaData(), true, subPath);
    }
    
    protected ZkaContext(ZkaContext context, String basePath, Object ignored) {
        super(context, basePath);
        setSelfDestructing(true);
    }
    
    
    
    @VisibleForTesting
    @ZkaSyncOperation
    public final List<ACL> getDefaultAcl() {
        return getSharedDefaultAcl().orElseGet(() -> {
            try {
                return getACL("");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    } 
    
    public final void setDefaultAcl(List<ACL> defaultAcl) {
        if (!setSharedDefaultAcl(defaultAcl)) {
            ZkaResourceContext rctx = zkaResourceContext();
            if (rctx == null) {
                log.warn(
                        "#{}: Context is created from a Curator instance and does not support setting default ACL [{}]",
                        this, defaultAcl);
            } else if (!(rctx.getAclProvider() instanceof DefaultACLProvider)) {
                log.warn("#{}: ACL provider [{}] does not support setting default ACL [{}]", this,
                        rctx.getAclProvider(), defaultAcl);
            }
        }
    }
    @VisibleForTesting
    public final boolean waitForDefaultACLResolved(long timeoutMs) throws InterruptedException {
        ZkaResourceContext rc = zkaResourceContext();
        if (rc != null && rc.getAclProvider() instanceof DefaultACLProvider) {
            return true;
        } else {
            return true;
        }
    }
    
    
    private static String joinPath(String[] paths) {
        if (paths.length == 0) {
            return "/";
        }
        
        if (paths.length == 1) {
            return ZKPaths.makePath(paths[0], "");
        }
        if (paths.length == 2) {
            return ZKPaths.makePath(paths[0], paths[1]);
        }
        
        
        return ZKPaths.makePath(paths[0], paths[1], Arrays.copyOfRange(paths, 2, paths.length));
    }
    
    @ZkaSyncOperation
    public final @Nullable Stat exists(String... paths) throws IOException {
        return exists(joinPath(paths));
    }
    
    @ZkaSyncOperation
    public final void create(ZkaData data, String... paths) throws IOException {
        create(data.serialize(), false, joinPath(paths));
    }
    @ZkaSyncOperation
    public final void create(ZkaData data, String childPath) throws IOException {
        create(data.serialize(), false, childPath);
    }
    
    @ZkaSyncOperation
    public final String create(ZkaData data, boolean createParents, String... paths) throws IOException {
        create(data.serialize(), createParents, joinPath(paths));
        return resolve(joinPath(paths));
    }
    
    @ZkaSyncOperation
    public final String create(ZkaData data, boolean createParents, String childPath) throws IOException {
        create(data.serialize(), createParents, childPath);
        return resolve(childPath);
    }
    
    @ZkaAsyncOperation
    public final void createInBackground(ZkaData data, boolean createParents, String childPath) {
        createInBackground(data.serialize(), createParents, childPath);
    }
    
    
    @ZkaAsyncOperation
    public final void createInBackground(ZkaData data, boolean createParents, String... paths) {
        createInBackground(data.serialize(), createParents, joinPath(paths));
    }
    
    @ZkaSyncOperation
    @Deprecated
    public final ZkaData read(String... paths) throws IOException {
        
        return getData(paths);
    }
    @ZkaSyncOperation
    public final Stat update(ZkaData data, String... paths) throws IOException {
        return update(data.serialize(), joinPath(paths));
    }
    @ZkaSyncOperation
    public final void delete(String... paths) throws IOException {
        delete(joinPath(paths));
    }
    @ZkaAsyncOperation
    public final void deleteInBackground(String... paths) throws IOException {
        deleteInBackground(joinPath(paths));
    }
    
    
    @ZkaSyncOperation
    public final ZkaData getData(String childPath) throws IOException {
        return ZkaData.fromBytes(getNodeData(childPath));
    }
    
    
    public final ZkaData getData(String... paths) throws IOException {
        return ZkaData.fromBytes(getNodeData(joinPath(paths)));
    }
    
    @ZkaSyncOperation
    public final List<ACL> getACL(String... paths) throws IOException {
        return getACL(joinPath(paths));
    }
    
    public final void setACL(List<ACL> acls, String... paths) throws IOException {
        setACL(acls, joinPath(paths));
    }
    
    @ZkaSyncOperation
    public final List<String> getChildren(String... paths) throws IOException {
        return getChildren(joinPath(paths));
    }
    @ZkaSyncOperation
    public final Map<String, ZkaData> getChildrenData(String... paths) throws IOException {
        return getChildrenData(joinPath(paths), ZkaData::fromBytes);
    }
}
