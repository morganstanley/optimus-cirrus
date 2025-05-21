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
public class MSNetLoopNullImpl implements MSNetLoopImpl {
    
    public MSNetLoopNullImpl() {
        
        
        
        
    }
    public void addLoopListener(MSNetLoopListener listener) {
    }
    public Object callbackAfterDelay(long delayInMillis, MSNetEventListener listener, String tag) {
        return null;
    }
    public Object callbackAfterDelay(long delayInMillis, MSNetEventListener listener) {
        return null;
    }
    public Object callbackAt(long timeToRunAt, MSNetEventListener listener, String tag) {
        return null;
    }
    public Object callbackAt(long timeToRunAt, MSNetEventListener listener) {
        return null;
    }
    public Object callbackPeriodically(long periodInMillis, MSNetEventListener listener, String tag) {
        return null;
    }
    public Object callbackPeriodically(long periodInMillis, MSNetEventListener listener) {
        return null;
    }
    public void cancelCallback(Object id) {
    }
    public boolean getContinueLoop() {
        return false;
    }
    public boolean isLooping() {
        return false;
    }
    public void loop() {
    }
    public void quit() {
    }
    public void registerChannel(MSNetChannel channel) {
    }
    public void removeLoopListener(MSNetLoopListener listener) {
    }
    public void setCallbackTag(Object id, String tag) {
    }
    public void setContinueLoop(boolean continueLoop) {
    }
    public void unregisterChannel(MSNetChannel channel) {
    }
    public void wakeup() {
    }
    @Override
    public MSNetLoopErrorHandler getLoopErrorHandler() {
        return null;
    }
    @Override
    public void setLoopErrorHandler(MSNetLoopErrorHandler loopErrorHandler) {
    }
}
