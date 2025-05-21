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

package msjava.msnet;
import java.util.Set;
import java.util.SortedSet;
public abstract class MSNetEstablisher implements Comparable {
    protected int priority;
    private boolean enabled = true;
    private boolean optional = false;
    private int timeoutInSecs;
    public MSNetEstablisher(int priority) {
        
        this(priority, 20);
    }
    public MSNetEstablisher(int priority, int timeoutInSecs) {
        this.priority = priority;
        this.timeoutInSecs = timeoutInSecs;
    }
    
    public abstract void init(boolean isServerSide, MSNetTCPConnection conn);
    
    public abstract MSNetEstablishStatus establish(MSNetTCPSocketBuffer readBuf) throws MSNetEstablishException,
            MSNetProtocolException;
    
    
    final MSNetEstablishStatus establishAndCheck(MSNetTCPSocketBuffer readBuf) throws MSNetEstablishException,
        MSNetProtocolException {
        MSNetEstablishStatus status = establish(readBuf);
        if (status.isUnknown()) {
            throw new MSNetEstablishException(getEstablisherName()
                    + " establisher returned unknown status after establish called");
        }
        if (status.isFailure()) {
            throw new MSNetEstablishException(getEstablisherName() + " establisher failed: "
                    + status.toString());
        }
        return status;
    }
    
    
    
    public abstract void cleanup();
    public abstract MSNetEstablishStatus getStatus();
    public abstract String getEstablisherName();
    
    public abstract MSNetTCPSocketBuffer getOutputBuffer();
    
    @Deprecated
    public void setPriority(int priority) {
        this.priority = priority;
    }
    
    public int getPriority() {
        return priority;
    }
    
    public int getSyncTimeout(){
        return timeoutInSecs;
    }
    
    public void setSyncTimeout(int seconds){
        timeoutInSecs = seconds;
    }
    @Override
    public int compareTo(Object o) {
        MSNetEstablisher obj = (MSNetEstablisher) o;
        return Long.signum(0L + getPriority() - obj.getPriority());
    }
    final public boolean isEnabled() {
        return enabled;
    }
    final public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
    @Override
    public String toString() {
        return "MSNetEstablisher [name=" + getEstablisherName() + " priority=" + priority + " optional=" + optional + " enabled=" + enabled + "]";
    }
    final public boolean isOptional() {
        return optional;
    }
    final public void setOptional(boolean optional) {
        this.optional = optional; 
    }
    
    
    protected void onDisable(boolean backCompat) throws MSNetEstablishException {
        
    }
    
    boolean onDisconnect() {
        return false;
    }
}
