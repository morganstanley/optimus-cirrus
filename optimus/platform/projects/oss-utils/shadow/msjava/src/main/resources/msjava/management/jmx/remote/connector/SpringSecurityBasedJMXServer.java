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

package msjava.management.jmx.remote.connector;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import msjava.management.jmx.authorization.FailSafeVoter;
import org.springframework.security.access.AccessDecisionManager;
import org.springframework.security.access.AccessDecisionVoter;
import org.springframework.security.access.vote.AffirmativeBased;
public abstract class SpringSecurityBasedJMXServer extends AbstractJMXServer {
    private AccessDecisionVoter<?> accessDecisionVoter;
    private final AtomicBoolean managerOrVoterSet = new AtomicBoolean(false);
    private boolean isFailsafeVoteFirst = true;
    SpringSecurityBasedJMXServer(Protocol protocol, SpringSecurityBasedAccessController mbeanServerForwarder) {
        super(protocol, mbeanServerForwarder);
    }
    public void setAccessDecisionManager(AccessDecisionManager accessDecisionManager) {
        throwIfSomethingAlreadySet();
        getAccessController().setAccessDecisionManager(accessDecisionManager);
    }
    public AccessDecisionManager getAccessDecisionManager() {
        return getAccessController().getAccessDecisionManager();
    }
    public void setAccessDecisionVoter(AccessDecisionVoter<?> accessDecisionVoter) {
        throwIfSomethingAlreadySet();
        this.accessDecisionVoter = accessDecisionVoter;
    }
    @Override
    public void init() throws Exception {
        throwIfNothingSet();
        super.init();
        if (accessDecisionVoter != null) {
            
            List<AccessDecisionVoter<?>> list = new ArrayList<AccessDecisionVoter<?>>();
            if (isFailsafeVoteFirst) {
                list.add(new FailSafeVoter());
                list.add(accessDecisionVoter);
            } else {
                list.add(accessDecisionVoter);
                list.add(new FailSafeVoter());
            }
            AffirmativeBased manager = new AffirmativeBased(list);
            getAccessController().setAccessDecisionManager(manager);
            manager.afterPropertiesSet();
        }
        
        getAccessController().afterPropertiesSet();
        
        accessDecisionVoter = null;
    }
    private final SpringSecurityBasedAccessController getAccessController() {
        return (SpringSecurityBasedAccessController) this.getMbeanServerForwarder();
    }
    private void throwIfSomethingAlreadySet() {
        if (!managerOrVoterSet.compareAndSet(false, true)) {
            throw new IllegalStateException(
                    "You should either set the accessDecisionVoter or the accessDecisionManager, but not both; and you should do the setting once");
        }
    }
    private void throwIfNothingSet() {
        if (!managerOrVoterSet.get()) {
            throw new IllegalStateException("You should set one of accessDecisionVoter or accessDecisionManager");
        }
    }
    public boolean isFailsafeVoteFirst() {
        return isFailsafeVoteFirst;
    }
    public void setFailsafeVoteFirst(boolean isFailsafeVoteFirst) {
        this.isFailsafeVoteFirst = isFailsafeVoteFirst;
    }
}
