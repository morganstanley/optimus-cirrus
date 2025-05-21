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
package msjava.msnet.authorization;
import msjava.msnet.MSNetEstablishException;
import msjava.msnet.auth.MSNetAuthContext;
import msjava.msnet.authorization.data.AuthorizationDataSource;
import msjava.msnet.authorization.internal.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
public class IPCAuthorizer implements MSNetAuthorizer {
    private static final Logger LOG = LoggerFactory.getLogger(IPCAuthorizer.class);
    protected final AuthorizationDataSource dataSource;
    private final boolean preview;
    protected Set<String> acceptedProids = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    protected Set<String> rejectedProids = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    protected Set<String> previewProids = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    public IPCAuthorizer(AuthorizationDataSource dataSource) {
        this(false, dataSource);
    }
    public IPCAuthorizer(boolean preview, AuthorizationDataSource authzDataSource) {
        this.preview = preview;
        this.dataSource = authzDataSource;
        if (LOG.isDebugEnabled())
            LOG.debug("IPC Authorizer created with {}", dataSource);
    }
    public String getAuthorizationDataLocation() {
        return dataSource.getAuthorizationDataLocation();
    }
    public boolean isPreview() {
        return preview;
    }
    public void validate(IPCAuthorizationContext context) throws MSNetEstablishException {
        if (context == null) {
            throw new MSNetEstablishException("cannot perform authorization on null IPCAuthorizationContext");
        }
        AuthorizationOutcome outcome = checkWithAuthorizationData(context);
        processValidateOutcome(outcome, context);
    }
    private AuthorizationOutcome checkWithAuthorizationData(IPCAuthorizationContext context) {
        AuthorizationData authzData = dataSource.getAuthorizationData();
        if (!context.isKerberized()) {
            return AuthorizationOutcome.NOT_KERBERIZED;
        } else if (authzData == null) {
            return AuthorizationOutcome.NOT_LOADED;
        } else if (!hasMatchedAuthorizationDataEntry(context, authzData)) {
            return AuthorizationOutcome.NOT_LISTED;
        } else {
            return AuthorizationOutcome.AUTHORIZED;
        }
    }
    boolean hasMatchedAuthorizationDataEntry(IPCAuthorizationContext context, AuthorizationData authzData) {
        if (authzData.size() > 0) {
            for (AuthorizationPrefix prefix : AuthorizationPrefix.values()) {
                AuthorizationDataEntry entry = createDummyAuthorizationDataEntryForComparison(prefix, context);
                if (entry != null && authzData.contains(entry)) {
                    return true;
                }
            }
        }
        return false;
    }
    static AuthorizationDataEntry createDummyAuthorizationDataEntryForComparison(AuthorizationPrefix prefix,
                                                                                 IPCAuthorizationContext context) {
        AuthorizationDataEntry entry = null;
        switch (prefix) {
            
            case PROID:
                MSNetAuthContext authContext;
                String authID;
                if (((authContext = context.getAuthContext()) != null) && ((authID = authContext.getAuthID()) != null)) {
                    entry = new AuthorizationDataEntry(authID);
                }
                break;
            
            default:
                break;
        }
        return entry;
    }
    void processValidateOutcome(AuthorizationOutcome outcome, IPCAuthorizationContext context) throws MSNetEstablishException {
        MSNetAuthContext authContext = context.getAuthContext();
        String proid = authContext.getAuthID();
        if (isPreview()) {
            
            LOG.info(outcome.getAuthorizationMessage());
            tryRegisterProid(proid, previewProids);
        } else {
            
            if (outcome.isAuthorized()) {
                tryRegisterProid(proid, acceptedProids);
                LOG.info(outcome.getAuthorizationMessage());
            } else {
                tryRegisterProid(proid, rejectedProids);
                String failMessage = "Proid(" + proid + ") is not authorized. Reason: " + outcome.getAuthorizationMessage();
                outcome.getAuthorizationMessage();
                LOG.error(failMessage);
                throw new MSNetEstablishException(failMessage);
            }
        }
    }
    private void tryRegisterProid(String proid, Set<String> proids){
        if(proid != null){
            proids.add(proid);
        }
    }
}
