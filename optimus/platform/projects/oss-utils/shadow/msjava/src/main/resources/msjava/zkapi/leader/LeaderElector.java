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
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import msjava.base.slr.internal.ServicePublisher;
public interface LeaderElector extends Closeable, ServicePublisher {
    
    
    @Override
    public void close() throws IOException;
    
    
    public int getMaxLeaders() throws IOException;
    
    
    
    public boolean waitForLeaderLoss() throws InterruptedException;
    
    
    
    public boolean waitForLeaderLoss(long timeout) throws InterruptedException;
  
    
    
    public boolean isLeader();
    
    
    public Map<String,Object> getData();
    
    
    public void setData(Map<String,Object> data);
    
    
    public LeaderElectorListener getListener();
    
    
    public void setListener(LeaderElectorListener listener);
    
    public boolean waitForLeadership() throws InterruptedException;
    
    
    public boolean waitForLeadership(long timeout) throws InterruptedException;
    
    public void abdicate();
    
    
    public Set<Map<String,Object>> getLeaders() throws IOException;
    
    
    public long getLeaderId();
    
    
    public Set<Map<String,Object>> getCandidates() throws IOException;
    
    
    public void start() throws IOException;
    
    
    public boolean waitForInitialization() throws InterruptedException;
    
    public boolean waitForInitialization(long timeout) throws InterruptedException;
}
