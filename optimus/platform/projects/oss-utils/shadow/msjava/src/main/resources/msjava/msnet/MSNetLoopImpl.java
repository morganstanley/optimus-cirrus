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
import msjava.msnet.MSNetLoop.MSNetLoopErrorHandler;
import msjava.msnet.MSNetLoop.MaxLoopRestartsErrorHandler;
public abstract interface MSNetLoopImpl {
    
    public boolean getContinueLoop();
    
    public void setContinueLoop(boolean continueLoop);
    public void quit();
    
    public void wakeup();
    
    public boolean isLooping();
    
    public void loop();
    
    public void addLoopListener(MSNetLoopListener listener);
    
    public void removeLoopListener(MSNetLoopListener listener);
    
    public void registerChannel(MSNetChannel channel);
    
    public void unregisterChannel(MSNetChannel channel);
    
    public Object callbackAt(long timeToRunAt, MSNetEventListener listener);
    
    public Object callbackAt(long timeToRunAt, MSNetEventListener listener, String tag);
    
    public Object callbackAfterDelay(long delayInMillis, MSNetEventListener listener);
    
    public Object callbackAfterDelay(long delayInMillis, MSNetEventListener listener, String tag);
    
    public Object callbackPeriodically(long periodInMillis, MSNetEventListener listener);
    
    public Object callbackPeriodically(long periodInMillis, MSNetEventListener listener, String tag);
    
    public void setCallbackTag(Object id, String tag);
    
    public void cancelCallback(Object id);
    
    MSNetLoopErrorHandler getLoopErrorHandler();
    
    void setLoopErrorHandler(MSNetLoopErrorHandler loopErrorHandler);
}
