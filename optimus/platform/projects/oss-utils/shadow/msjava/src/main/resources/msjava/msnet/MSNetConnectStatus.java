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
public class MSNetConnectStatus {
    protected MSNetException _exception = null;
    protected int _rc = -1;
    private MSNetConnectionStateEnum _state;
    
    public MSNetConnectStatus() {
    }
    public MSNetConnectStatus(MSNetException exception_) {
        _exception = exception_;
    }
    public MSNetConnectStatus(MSNetConnectionStateEnum state) {
        this._state = state;
    }
    
    public boolean isOk() {
        return (_exception == null);
    }
    
    public boolean inError() {
        return (_exception != null);
    }
    
    public MSNetException getException() {
        return _exception;
    }
    
    public void setException(MSNetException exception_) {
        _exception = exception_;
    }
    
    public void raise() throws MSNetException {
        if (inError())
            throw _exception;
    }
    public String toString() {
        return "MSNetConnectStatus: rc=" + _rc + ", exception=" + _exception;
    }
    public MSNetConnectionStateEnum getState() {
        return _state;
    }
}
