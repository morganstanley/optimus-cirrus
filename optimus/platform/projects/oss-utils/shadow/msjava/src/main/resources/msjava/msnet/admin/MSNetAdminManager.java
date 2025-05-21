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

package msjava.msnet.admin;
import msjava.base.admin.MSAdminCommand;
import msjava.base.admin.MSAdminCommandAlias;
import msjava.base.admin.MSAdminCommandStore;
import msjava.msnet.MSNetID;
import msjava.msnet.MSNetInetAddress;
import msjava.msnet.MSNetLoop;
import msjava.msnet.MSNetServerStartupPolicyEnum;
import org.springframework.security.access.AccessDecisionManager;
public class MSNetAdminManager {
    protected MSNetAdminManagerImpl _impl;
    
    protected MSNetAdminManager() {
    }
    
    public MSNetAdminManager(MSNetLoop loop_, MSNetInetAddress address_) {
        this(loop_, address_, true);
    }
    
    public MSNetAdminManager(MSNetLoop loop_, MSNetInetAddress address_, boolean createDefaultCommands_) {
        this(loop_, address_, createDefaultCommands_, MSNetServerStartupPolicyEnum.FAIL_ON_EXCEPTION, false);
    }
    
    
    protected MSNetAdminManager(MSNetLoop loop_, MSNetInetAddress address_, boolean createDefaultCommands_, String usernameToUseForAuthorization) {
        this(new MSNetAdminManagerDefaultImpl(loop_, address_, createDefaultCommands_, usernameToUseForAuthorization));
    }
    public MSNetAdminManager(MSNetAdminManagerImpl impl_) {
        setImpl(impl_);
    }
    
    public MSNetAdminManager(MSNetLoop loop_, MSNetInetAddress address_, boolean createDefaultCommands_,
            MSNetServerStartupPolicyEnum startupFailurePolicy) {
        this(loop_, address_, createDefaultCommands_, startupFailurePolicy, null);
    }
    public MSNetAdminManager(MSNetLoop loop_, MSNetInetAddress address_, boolean createDefaultCommands_,
            MSNetServerStartupPolicyEnum startupFailurePolicy, boolean kerberos) {
        
        this(loop_, address_, createDefaultCommands_, startupFailurePolicy, null, kerberos);
    }
    
    protected MSNetAdminManager(MSNetLoop loop_, MSNetInetAddress address_, boolean createDefaultCommands_, 
            MSNetServerStartupPolicyEnum startupFailurePolicy, String usernameToUseForAuthorization) {
        this(loop_, address_, createDefaultCommands_, startupFailurePolicy, usernameToUseForAuthorization, false);
    }
     
    protected MSNetAdminManager(MSNetLoop loop_, MSNetInetAddress address_, boolean createDefaultCommands_,
            MSNetServerStartupPolicyEnum startupFailurePolicy, String usernameToUserForAuthorization, boolean kerberos) {
        this(new MSNetAdminManagerDefaultImpl(loop_, address_, createDefaultCommands_, startupFailurePolicy, usernameToUserForAuthorization, kerberos));        
    }
    
    
    public synchronized MSNetAdminManagerImpl getImpl() {
        return _impl;
    }
    
    public synchronized void setImpl(MSNetAdminManagerImpl impl_) {
        if (impl_ != null) {
            _impl = impl_;
        } else {
            throw new IllegalArgumentException("Unable to initialize MSNetAdminManager: impl_ must not be null.");
        }
    }
    
    public void serverShutdown() {
        _impl.serverShutdown();
    }
    public void setInterceptListener(MSNetAdminInterceptListener interceptListener_) {
        _impl.setInterceptListener(interceptListener_);
    }
    public MSNetAdminInterceptListener getInterceptListener() {
        return _impl.getInterceptListener();
    }
    
    public boolean isEntityRegistered(MSNetID entityID_) {
        return _impl.isEntityRegistered(entityID_.toString());
    }
    
    public boolean isServerListening() {
        return _impl.isServerListening();
    }
    
    public void addListener(MSNetAdminManagerListener listener_) {
        _impl.addListener(listener_);
    }
    
    public void removeListener(MSNetAdminManagerListener listener_) {
        _impl.removeListener(listener_);
    }
    
    public MSAdminCommand getAdminCommand(String ns_, String command_) {
        return _impl.getAdminCommand(ns_, command_);
    }
    
    public MSAdminCommandStore getAdminCommandStore() {
        return _impl.getAdminCommandStore();
    }
    
    public void addAlias(MSAdminCommandAlias alias_) {
        _impl.addAlias(alias_);
    }
    
    public void addAdminCommand(MSAdminCommand command_) {
        _impl.addAdminCommand(command_);
    }
    
    public void addAdminCommand(MSAdminCommand command_, String[] constArgs_) {
        _impl.addAdminCommand(command_, constArgs_);
    }
    
    public void removeAdminCommand(MSAdminCommand command_) {
        _impl.removeAdminCommand(command_);
    }
    
    public void removeAdminCommands(String commandNS_, String commandName_) {
        _impl.removeAdminCommands(commandNS_, commandName_);
    }
    
    public void registerEntity(MSNetAdminableEntity entity_) {
        _impl.registerEntity(entity_);
    }
    
    public void unregisterEntity(MSNetID entityName_) {
        if (entityName_ != null) {
            _impl.unregisterEntity(entityName_.toString());
        }
    }
    @Override
    public String toString() {
        return _impl.toString();
    }
    
    public String getAddressInfo() {
        return _impl.getAddressInfo();
    }
    
    
    public void setAccessDecisionManager(AccessDecisionManager accessDecisionManager) {
        if (this._impl != null) {
            this._impl.setAccessDecisionManager(accessDecisionManager);
        } else {
            throw new IllegalStateException(
                    "You cannot set an AccessDecisionManager without setting an MSNetAdminManagerImpl");
        }
    }
}