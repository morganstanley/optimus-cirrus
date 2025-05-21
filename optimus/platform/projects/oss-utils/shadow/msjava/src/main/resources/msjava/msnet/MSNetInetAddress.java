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
import java.net.UnknownHostException;
import msjava.msnet.utils.MSNetConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class MSNetInetAddress implements MSNetAddress {
    private static final Logger LOGGER = LoggerFactory.getLogger(MSNetInetAddress.class);
    final MSNetInetAddressImpl impl;
    private boolean canonical = MSNetConfiguration.DEFAULT_USE_CANONICAL_NAME;
    
    public MSNetInetAddress(String host, int port) throws UnknownHostException, IllegalArgumentException {
        this(getJavaInetAddressFromString(host), port);
    }
    
    public MSNetInetAddress(String host, String servNameOrPort) throws UnknownHostException,
            MSNetServiceNotFoundException, IllegalArgumentException {
        this(host, MSNetPort.stringToPort(servNameOrPort));
    }
    
    public MSNetInetAddress(String addr) throws IllegalArgumentException, UnknownHostException,
            MSNetServiceNotFoundException {
        
        
        AddressString a = new AddressString(addr);
        int port = getPortFromString(a.portString());
        InetAddress ia = getJavaInetAddressFromString(a.hostString());
        if (!portBoundsCheck(port)) {
            throw new IllegalArgumentException("Port " + port + " is out of range!");
        }
        impl = new MSNetInetAddressNIOImpl(ia, port);
    }
    
    public MSNetInetAddress(int port) throws UnknownHostException, IllegalArgumentException {
        this((String) null, port);
    }
    
    public MSNetInetAddress(InetAddress inetaddr, int port) throws IllegalArgumentException {
        if (portBoundsCheck(port) == false) {
            throw new IllegalArgumentException("Port " + port + " is out of range!");
        }
        impl = new MSNetInetAddressNIOImpl(inetaddr, port);
    }
    
    public MSNetInetAddress(MSNetInetAddress address_) {
        if (address_ == null) {
            throw new IllegalArgumentException("Can't copy null address");
        }
        
        
        
        impl = address_.impl;
    }
    
    public MSNetInetAddress(MSNetInetAddressImpl impl_) {
        this.impl = impl_;
    }
    
    public MSNetInetAddressImpl getImpl() {
        return impl;
    }
    
    public boolean isValid() {
        return impl.isValid();
    }
    
    public String getHost() {
        if (!canonical || !getInetAddress().isAnyLocalAddress()) {
            return impl.getHost();
        }
        try {
            return InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(e.getMessage());
            }
        }
        return null;
    }
    
    public int getPort() {
        return impl.getPort();
    }
    
    public InetSocketAddress getInetSocketAddress() {
        return impl.getInetSocketAddress();
    }
    
    public InetAddress getInetAddress() {
        return impl.getInetAddress();
    }
    @Override
    public String toString() {
        return impl.toString();
    }
    
    @Override
    public int hashCode() {
        return impl.hashCode();
    }
    
    @Override
    public boolean equals(Object o) {
        return impl.equals(o);
    }
    
    private static boolean portBoundsCheck(int port) {
        return (port & ~0xFFFF) == 0;
    }
    public void resolve() {
        impl.resolve();
    }
    
    private static int getPortFromString(String servNameOrPortString) throws MSNetServiceNotFoundException {
        return MSNetPort.stringToPort(servNameOrPortString);
    }
    
    private static InetAddress getJavaInetAddressFromString(String host) throws UnknownHostException {
        if ((host == null) || ("".equals(host) || ("ANY".equals(host)))) {
            
            return InetAddress.getByName("0.0.0.0");
        }
        if ("localhost".equals(host)) {
            return InetAddress.getByName("127.0.0.1");
        }
        return InetAddress.getByName(host);
    }
    
    public String getAddressString() {
        return impl.getAddressString();
    }
    
    private static final class AddressString {
        private final String hostStr, portStr;
        
        AddressString(String hostport) {
            int colon = hostport.lastIndexOf(':');
            if ((colon == -1) || (colon == hostport.length() - 1)) {
                throw new IllegalArgumentException("MSNetInetAddress.<ctor>(): '" + hostport
                        + "' is not a legal hostport string.");
            }
            hostStr = hostport.substring(0, colon);
            portStr = hostport.substring(colon + 1);
        }
        String hostString() {
            return hostStr;
        }
        String portString() {
            return portStr;
        }
    }
    
    public boolean isCanonical() {
        return canonical;
    }
    
    public void setCanonical(boolean canonical) {
        this.canonical = canonical;
    }
}
