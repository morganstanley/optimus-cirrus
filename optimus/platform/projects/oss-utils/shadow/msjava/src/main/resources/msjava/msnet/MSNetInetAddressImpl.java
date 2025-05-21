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
import java.net.InetAddress;
import java.net.InetSocketAddress;
public interface MSNetInetAddressImpl {
    
    public boolean isValid();
    
    public String getHost();
    
    public int getPort();
    
    public InetAddress getInetAddress();
    
    public InetSocketAddress getInetSocketAddress();
    
    public int hashCode();
    
    public boolean equals(Object o);
    
    public void resolve() throws MSNetAddressResolutionException;
    
    public String getAddressString();
}
