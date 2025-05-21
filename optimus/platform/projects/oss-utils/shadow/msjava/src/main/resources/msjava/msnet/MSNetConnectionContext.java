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
public class MSNetConnectionContext {
    protected MSNetTCPServerUserContext _userContext;
    protected MSNetLoop _loop;
    protected final MSNetConnectionContextTypeEnum _type;
    
    public MSNetConnectionContext(MSNetLoop loop_, MSNetTCPConnection connection_, MSNetConnectionContextTypeEnum type_) {
        this(loop_, type_, new MSNetTCPServerUserContext(connection_));
    }
    
    public MSNetConnectionContext(MSNetLoop loop_, MSNetConnectionContextTypeEnum type_,
            MSNetTCPServerUserContext context_) {
        _type = type_;
        _loop = loop_;
        _userContext = context_;
    }
    public MSNetTCPServerUserContext getUserContext() {
        return _userContext;
    }
    public MSNetTCPConnection getConnection() {
        return _userContext.getConnection();
    }
    public MSNetLoop getLoop() {
        return _loop;
    }
    public MSNetConnectionContextTypeEnum getType() {
        return _type;
    }
    public void reparent(MSNetLoop loop_) {
        _loop = loop_;
        getConnection().reparent(_loop);
    }
    public MSNetID getName() {
        return getConnection().getName();
    }
}
