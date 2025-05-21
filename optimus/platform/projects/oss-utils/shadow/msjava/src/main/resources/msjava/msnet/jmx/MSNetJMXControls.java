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

package msjava.msnet.jmx;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import msjava.management.annotation.ManagedChildResource;
import msjava.msnet.MSNetAddress;
import msjava.msnet.MSNetException;
import msjava.msnet.MSNetInetAddress;
import msjava.msnet.MSNetTCPAcceptor;
import msjava.msnet.MSNetTCPConnection;
import msjava.msnet.admin.MSNetRowFormatter;
import msjava.msnet.admin.MSNetRowFormatterRow;
import msjava.msnet.admin.MSNetRowFormatterRowSet;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedOperationParameter;
import org.springframework.jmx.export.annotation.ManagedOperationParameters;
import org.springframework.jmx.export.annotation.ManagedResource;
import com.google.common.collect.ImmutableList;
@ManagedResource(description="Provides commands to open/close connections and listeners", objectName="net:type=conn")
public class MSNetJMXControls {
    @ManagedResource(description="miscellaneous commands", objectName="net:type=misc")
    public static class InnerControls {
        private MSNetJMXControls controls;
        private InnerControls(MSNetJMXControls outer) {
            controls = outer;
        }
        
        @ManagedOperation(description = "Open the named listener using the specified address.\n If address is not specified the listener\'s\n default address is used")
        @ManagedOperationParameters(value = { 
                @ManagedOperationParameter(name = "NAME", description = "The ID of the listener to close"),
                @ManagedOperationParameter(name="ADDRESS", description="A host:port to listen on") } )
        public String open_listener(String name, String hostPort) throws MSNetException {
            MSNetTCPAcceptor listener = controls.listeners.get(name);
            
            if (listener == null) {
                throw new MSNetException("Cannot find listener with name " + name);
            }
            
            if (listener.isListening()) {
                return "Notice: Entity \"" + name + "\" is already open.";
            } else {
                if (hostPort == null || hostPort.equals("") || "null".equals(hostPort.toLowerCase())) {
                    listener.listen();
                    return "Info: Entity opened; address = " + listener.getAddress();
                }
                
                MSNetInetAddress addr = null;
                try {
                    addr = new MSNetInetAddress(hostPort);
                } catch (Exception e) {                
                }
                if (addr == null) {
                    return "Address '" + hostPort + "' is invalid!";
                }
                controls.unregisterAcceptor(listener);
                listener.listen(addr);
                controls.registerAcceptor(listener);
                return "Opened " + addr;
            }
        }
     
        @ManagedOperation(description = "Close the listener identified by the given NAME.")
        @ManagedOperationParameters(value = { @ManagedOperationParameter(name = "NAME", description = "The ID of the listener to close") })
        public String close_listener(String name) throws MSNetException {
            MSNetTCPAcceptor listener = controls.listeners.get(name);
            
            if (listener == null) {
                throw new MSNetException("Cannot find listener with name " + name);
            }
            
            if (listener.isListening()) {
                listener.close();
                return "Closed " + name;
            }
            return "Notice: Entity \"" + name + "\" was not open.";
        }
        @ManagedOperation(description = "List all the adminable entities registered with this\n admin manager.")
        public String list() {
            
            
            ArrayList<String> row = new ArrayList<String>();
            MSNetRowFormatterRowSet rowset = new MSNetRowFormatterRowSet(5);
            
            
            
            
            DateFormat dateFormatter = DateFormat.getDateTimeInstance();
            String headers[] = { "Name", "Address", "State", "OpenTime", "AuthID" };
            rowset.setHeader(new MSNetRowFormatterRow(headers));
            for (Map.Entry<String, MSNetTCPConnection> entry : controls.connections.entrySet()) {
                row.add(entry.getKey());
                
                MSNetTCPConnection conn = entry.getValue();
                MSNetAddress addr = conn.getAddress();
                row.add(addr.toString());
                row.add(conn.getState().toString());
                Date ot = conn.connectTime();
                if (ot == null) {
                    row.add("Never opened.");
                } else {
                    row.add(dateFormatter.format(ot));
                }
                String authID = (conn.getAuthContext() != null) ? conn.getAuthContext().getAuthID() : "";
                row.add(authID == null ? "" : authID);
                rowset.addRow(new MSNetRowFormatterRow(row));
                row.clear();
            }
            
            for (Map.Entry<String, MSNetTCPAcceptor> entry : controls.listeners.entrySet()) {
                row.add(entry.getKey());
                
                MSNetTCPAcceptor listener = entry.getValue();
                MSNetAddress addr = listener.getAddress();
                row.add(addr.toString());
                row.add(listener.getState().toString());
                Date ot = listener.listenStartTime();
                if (ot == null) {
                    row.add("Never opened.");
                } else {
                    row.add(dateFormatter.format(ot));
                }
                row.add(""); 
                rowset.addRow(new MSNetRowFormatterRow(row));
                row.clear();
            }
            
            return MSNetRowFormatter.formatRowSet(rowset, 0);        
        }
    }
    
    @ManagedChildResource
    public InnerControls inner = new InnerControls(this);    
    
    private Map<String, MSNetTCPConnection> connections = new HashMap<String, MSNetTCPConnection>();
    private Map<String, MSNetTCPAcceptor> listeners = new HashMap<String, MSNetTCPAcceptor>();
    
    public synchronized void registerConnection(MSNetTCPConnection connection) {
        connections.put(connection.getName().getString(), connection);
    }
    
    public synchronized void registerAcceptor(MSNetTCPAcceptor acceptor) {
        MSNetInetAddress addr = acceptor.getAddress();
        listeners.put(addr.getHost() + ":" + addr.getPort(), acceptor);
    }
    
    public synchronized void unregisterConnection(MSNetTCPConnection connection) {
        connections.remove(connection.getName().getString());
    }
    
    public synchronized void unregisterAcceptor(MSNetTCPAcceptor acceptor) {
        MSNetInetAddress addr = acceptor.getAddress();
        listeners.remove(addr.getHost() + ":" + addr.getPort());
    }
    
    public synchronized Collection<MSNetTCPAcceptor> getListeners() {
    	return ImmutableList.copyOf(listeners.values());
    }
    
    public synchronized Collection<MSNetTCPConnection> getConnections() {
    	return ImmutableList.copyOf(connections.values());
    }
    
    @ManagedOperation(description = "Open the named connection using the specified address.\n If address is not specified the connection\'s default address is used")
    @ManagedOperationParameters(value = { 
            @ManagedOperationParameter(name = "NAME", description = "ID for the connection"),
            @ManagedOperationParameter(name="ADDRESS", description="The address to open") } )    
    public String open(String name, String hostPort) throws MSNetException {
        MSNetTCPConnection conn = connections.get(name);
        
        if (conn == null) {
            
            MSNetTCPAcceptor listener = listeners.get(name);
            if (listener != null) {
                return inner.open_listener(name, hostPort);
            }
            throw new MSNetException("Cannot find connection with name " + name);
        }
        if (conn.isConnected()) {
            return "Notice: Entity \"" + name + "\" is already open.";
        } 
        if (hostPort == null || hostPort.equals("")) {
            conn.connect();
            return "Info: Entity opened; address = " + conn.getAddress();
        }
        
        MSNetInetAddress addr = null;
        try {
            addr = new MSNetInetAddress(hostPort);
        } catch (Exception e) {
            return "Address '" + hostPort + "' is invalid!";
        }
        conn.connect(addr);
        return "Info: Entity opened; address = " + addr;
    }
    
    @ManagedOperation(description = "Close the connection identified by the given NAME.")
    @ManagedOperationParameters(value = {@ManagedOperationParameter(name = "NAME", description = "The ID of the connection to close")} )    
    public String close(String name) throws MSNetException {
        MSNetTCPConnection conn = connections.get(name);
        if (conn == null) {
            
            MSNetTCPAcceptor listener = listeners.get(name);
            if (listener != null) {
                listener.close();
                return "Closed " + name;
            }
            throw new MSNetException("Cannot find connection with name " + name);
        } 
        
        if (!conn.isConnected()) {
            return "Notice: Entity \"" + name + "\" is already closed.";
        }
        
        conn.close();
        return "Closed " + name;
    }
    
    @ManagedOperation(description = "Show detailed state information for the connection identified by the given NAME")
    @ManagedOperationParameters(value = { @ManagedOperationParameter(name = "NAME", description = "A connection ID") })
    public String info(String name) throws MSNetException {
        MSNetTCPConnection conn = connections.get(name);
        if (conn == null) {
            
            MSNetTCPAcceptor listener = listeners.get(name);
            if (listener != null) {
                return listener.getInfoMessage();
            }
            throw new MSNetException("Cannot find connection with name " + name);
        }
        
        return conn.getInfoMessage();
    }
    
    @ManagedOperation(description = "Show a count of the number of connections currently registered")
    public String count() throws MSNetException {
        return Integer.toString(connections.size());
    }
    
    @ManagedOperation(description = "Shows how much data (in bytes and number of buffers) is being buffered by connections registered with the admin manager. If NAME is specified, then the sizes for that connection will be returned; otherwise, the sizes for all registered connections will be returned.")
    @ManagedOperationParameters(value = { @ManagedOperationParameter(name = "NAME", description = "The connection to examine") })
    public String view_buffers(String name) throws MSNetException {
        ArrayList<String> row = new ArrayList<String>();
        MSNetRowFormatterRowSet rowset = new MSNetRowFormatterRowSet(4);
        String headers[] = { "Name", "BufferSize", "BufferCount" };
        rowset.setHeader(new MSNetRowFormatterRow(headers));
        if (name != null && !"".equals(name)) {
            MSNetTCPConnection connection = connections.get(name);
            if (connection == null) {
                throw new MSNetException("Cannot find connection \"" + name + "\"");
            }
            row.add(name);
            row.add(String.valueOf(connection.bufferSize()));
            row.add(String.valueOf(connection.bufferCount()));
            rowset.addRow(new MSNetRowFormatterRow(row));
            row.clear();
        } else {
            for (Map.Entry<String,MSNetTCPConnection> entry : connections.entrySet()) {
                String connName = entry.getKey();
                MSNetTCPConnection conn = entry.getValue();
                row.add(connName);
                row.add(String.valueOf(conn.bufferSize()));
                row.add(String.valueOf(conn.bufferCount()));
                rowset.addRow(new MSNetRowFormatterRow(row));
                row.clear();
            } 
        }
        
        String result = MSNetRowFormatter.formatRowSet(rowset, 0);
        return result;
    }
    @ManagedOperation(description = "Close the named connection and try to reconnect it to the specific ADDRESS.  The tokens OVERRIDE and PRIMARY may be used to indicate that the connection should failback to its preset override or primary address, respectively.or the connection's primary or override address. If an address is specified, it will be set to the connections override address.")
    @ManagedOperationParameters(value = { 
            @ManagedOperationParameter(name = "NAME", description = "The connection to override"),
            @ManagedOperationParameter(name="ADDRESS", description="The new address to use") } )
    public String failback(String name, String hostPort) throws MSNetException {
        MSNetTCPConnection conn = connections.get(name);
        if (conn == null) {
            throw new MSNetException("Connection '" + name + "' not found.");
        }
        
        if (hostPort == null || hostPort.trim().equals("")) {
            throw new MSNetException("Missing address");
        }        
        hostPort = hostPort.trim();
        if ("primary".equals(hostPort.toLowerCase())) {
            conn.failback(false);
        } else if ("override".equals(hostPort.toLowerCase())) {
            conn.failback(true);
        } else {
            try {
                MSNetInetAddress addr = new MSNetInetAddress(hostPort);
                conn.failback(addr);
            } catch (Exception e) {
                throw new MSNetException("Address '" + hostPort + "' is invalid.");
            }
        }
        
        return "Success";
    }
}
