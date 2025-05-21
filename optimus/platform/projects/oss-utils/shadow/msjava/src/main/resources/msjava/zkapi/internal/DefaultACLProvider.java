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
import com.google.common.collect.ImmutableSet;
import com.ms.infra.zookeeper.utils.curator.CopyACLProvider;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class DefaultACLProvider extends CopyACLProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultACLProvider.class);
    private final AtomicReference<List<ACL>> overrideAclRef = new AtomicReference<>();
    public DefaultACLProvider(String path, int maxWaitMs) {
        super("/".equals(path) ? "" : path, maxWaitMs);
    }
    @Override
    public List<ACL> getDefaultAcl() {
        List<ACL> overrideAcl = overrideAclRef.get();
        return overrideAcl != null ? overrideAcl : super.getDefaultAcl();
    }
    
    public void setDefaultAcl(List<ACL> acl) {
        overrideAclRef.set(acl);
        LOGGER.debug("{} now has its override ACL set to {}", this, acl);
        if (acl != null && !ACLUtils.doesUserHasAdminPerm(acl)) {
            LOGGER.warn("{} warning: none of the user groups has ADMIN permission. This means you will not be able to "
                + "modify the ACL list of the created nodes. For more info see "
                + "http:
        }
    }
    @Override
    public String toString() {
        return getClass().getSimpleName() + "{"
            + "super=" + super.toString() + ", "
            + "overrideAcl=" + overrideAclRef.get() + "}";
    }
    
    private static class ACLUtils {
        private static final String KERBEROS = "kerberos";
        
        private static final Set<Id> DEFAULT_ZOOKEEPER_IDS = ImmutableSet.<Id>builder()
            .add(new Id(KERBEROS, "user:"))
            .add(new Id(KERBEROS, "user:"))
            .add(new Id(KERBEROS, "ldap:"))
            .add(new Id(KERBEROS, "ldap:"))
            .build();
        private static boolean hasPermission(ACL acl, int perms) {
            return (acl.getPerms() & perms) == perms;
        }
        
        private static boolean doesUserHasAdminPerm(List<ACL> acls) {
            for (ACL acl : acls) {
                if (!DEFAULT_ZOOKEEPER_IDS.contains(acl.getId()) && hasPermission(acl, ZooDefs.Perms.ADMIN)) {
                    return true;
                }
            }
            return false;
        }
    }
}
