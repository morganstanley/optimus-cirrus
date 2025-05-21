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
import msjava.base.admin.MSAdminException;
import msjava.base.admin.MSAdminRequest;
import org.springframework.security.access.AccessDecisionManager;
public interface MSNetAdminManagerImpl {
    
    public void addAdminCommand(MSAdminCommand command_);
    
    public void addAdminCommand(MSAdminCommand command_, String[] constArgs_);
    
    public void addAlias(MSAdminCommandAlias alias_);
    
    public MSAdminCommand getAdminCommand(String ns_, String command_);
    
    public void removeAdminCommand(MSAdminCommand command_);
    
    public void removeAdminCommands(String commandNS_, String commandName_);
    
    public void addListener(MSNetAdminManagerListener listener_);
    
    public void removeListener(MSNetAdminManagerListener listener_);
    public MSNetAdminInterceptListener getInterceptListener();
    public void setInterceptListener(MSNetAdminInterceptListener interceptListener_);
    
    public void registerEntity(MSNetAdminableEntity entity_);
    
    public void unregisterEntity(String entityName_);
    
    public boolean isEntityRegistered(String entityID_);
    
    public MSNetAdminableEntity getEntity(String entityID_);
    
    public boolean isServerListening();
    
    public void serverShutdown();
    
    public int getConnectionCount();
    
    public MSNetAdminableEntity[] listEntities();
    
    public MSAdminCommandStore getAdminCommandStore();
    
    public void handleTapRequest(MSAdminRequest req_) throws MSAdminException;
    
    public void handleSubscribeRequest(MSAdminRequest req_) throws MSAdminException;
    
    public void handleUnsubscribeRequest(MSAdminRequest req_) throws MSAdminException;
    
    public String getAddressInfo();
    
    
    public void setAccessDecisionManager(AccessDecisionManager accessDecisionManager);
}
