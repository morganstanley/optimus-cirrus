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
public interface ConnectionLimitPolicy {
    
    
    final ConnectionLimitPolicy NONE = new ConnectionLimitPolicy() {
        @Override
        public Object connectionGroup(MSNetTCPServerUserContext context) {
            return null;
        }
        public String toString() { return "NONE"; };
    };
    
    
    final ConnectionLimitPolicy HOST = new ConnectionLimitPolicy() {
        @Override
        public Object connectionGroup(MSNetTCPServerUserContext context) {
            return ((MSNetInetAddress) context.getConnection().getAddress()).getHost();
        }
        public String toString() { return "HOST"; };
    };
    
    
    final ConnectionLimitPolicy USER = new ConnectionLimitPolicy() {
        @Override
        public Object connectionGroup(MSNetTCPServerUserContext context) {
            MSNetAuthContext auth = context.getAuthContext();
            return auth == null ? "" : auth.getAuthID();
        }
        public String toString() { return "USER"; };
    };
    
    Object connectionGroup(MSNetTCPServerUserContext context);
}