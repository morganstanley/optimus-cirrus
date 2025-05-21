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

package msjava.management.jmx.authorization;
import java.util.Collection;
import msjava.base.util.internal.SystemPropertyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.AccessDecisionVoter;
import org.springframework.security.access.ConfigAttribute;
import org.springframework.security.core.Authentication;
public class FailSafeVoter implements AccessDecisionVoter<Object> {
    
    private static final Logger auditLogger = LoggerFactory
            .getLogger(FailSafeVoter.class.getCanonicalName() + ".Audit");
    
    private static final String userid = SystemPropertyUtils.getProperty("user.name", auditLogger);
    
    @Override
    public boolean supports(ConfigAttribute attribute) {
        return true;
    }
    
    @Override
    public boolean supports(Class<?> clazz) {
        return true;
    }
    
    @Override
    public int vote(Authentication authentication, Object object, Collection<ConfigAttribute> config) {
        String user = authentication.getName();
        if (userid.equals(user)) {
            if (auditLogger.isInfoEnabled()) {
                auditLogger.info("Granting (fail-safe) access to user '{}' for action '{}'", user, config);
            }
            return ACCESS_GRANTED;
        } else {
            
            return ACCESS_DENIED;
        }
    }
}