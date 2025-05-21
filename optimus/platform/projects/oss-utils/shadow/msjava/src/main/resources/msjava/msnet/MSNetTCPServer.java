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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import msjava.msnet.tcp.server.TCPServerMessageHandler;
import msjava.msnet.tcp.server.TCPServerStateListener;
import msjava.msnet.tcp.server.TCPServerStreamHandler;
public abstract interface MSNetTCPServer {
	
	public void addListener(TCPServerStateListener listener);
	
	public void removeStateListener(TCPServerStateListener listener);
	
	public MSNetID getName();
	
	public void setName(MSNetID serverName);
	
	public MSNetInetAddress getAddress();
	
	public void setAddress(MSNetInetAddress address);
	
	public MSNetTCPServerUserContext getUserContext(MSNetID connectionName);
	
	public List<MSNetTCPServerUserContext> getUserContexts();
	
	public boolean hasConnection(MSNetID connectionName);
	
	public void start();
	
	public void start(MSNetInetAddress address);
	
	public void stop();
	
	public boolean isListening();
	
	public boolean manageConnection(MSNetTCPConnection connection, boolean forwardEvents);
	
	public void openConnection(MSNetInetAddress address, MSNetID connectionName, boolean forwardEvents);
	
	public boolean reparentConnection(MSNetID connectionName, MSNetLoop newLoop);
	
	public boolean removeConnection(MSNetID connectionName);
	
	public void setAdminManager(msjava.msnet.admin.MSNetAdminManager adminManager);
	
	public MSNetTCPConnection getConnection(MSNetID connectionName);
	
	public void addEstablisherFactory(MSNetEstablisherFactory establisher);
	
	public void removeEstablisherFactory(MSNetEstablisherFactory establisher);
	
	public Iterator<MSNetEstablisherFactory> establisherFactoryIterator();
	
	public Collection<TCPServerStateListener> getStateListeners();
	public void setStreamHandler(TCPServerStreamHandler streamServiceHandler);
	public void setMessageHandler(TCPServerMessageHandler messageServiceHandler);
	
	public void setStartupFailurePolicy(MSNetServerStartupPolicyEnum startupFailurePolicy);
}
