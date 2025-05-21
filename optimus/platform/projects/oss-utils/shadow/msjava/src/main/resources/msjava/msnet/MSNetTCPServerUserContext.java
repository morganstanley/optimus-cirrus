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
import msjava.msnet.auth.MSNetAuthContext;
public class MSNetTCPServerUserContext {
    final protected MSNetTCPConnection connection;
    private volatile MSNetAuthContext authContext;
    
    public MSNetTCPServerUserContext(MSNetTCPConnection connection) {
        this.connection = connection;
    }
    
    public MSNetTCPConnection getConnection() {
        return connection;
    }
    
    public MSNetAuthContext getAuthContext() {
        return authContext;
    }
    @Override
    public String toString() {
        return "MSNetTCPServerUserContext [connection=" + connection + "]";
    }
    
    void copyAuthContext() {
        this.authContext = connection.getAuthContext();
    }
    
}
