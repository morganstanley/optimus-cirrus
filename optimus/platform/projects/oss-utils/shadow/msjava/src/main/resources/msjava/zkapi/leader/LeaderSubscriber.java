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

package msjava.zkapi.leader;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import msjava.base.slr.internal.NotificationPolicy;
import msjava.base.slr.internal.ServiceLocatorClient;
import msjava.base.sr.ServiceDescription;
public interface LeaderSubscriber extends ServiceLocatorClient {
    
    
    @Override
    public void close() throws IOException;
    
    
    public Set<Map<String,Object>> getCandidates();
    
    
    public Set<Map<String,Object>> getLeaders();
    
    
    public Set<Map<String,Object>> getNonLeaders();
    
    
    public int getMaxLeaders();
    
    
    public void subscribe(LeaderSubscriberListener listener);
    
    
    public boolean waitForInitialization();
    
    
    public boolean waitForInitialization(long timeout);
    
    
    public boolean isInitialized();
    
    public void setNotificationPolicy(NotificationPolicy policy);
    public Set<ServiceDescription> getServices(String service);
    @Override
    public Set<ServiceDescription> getServices();
}
