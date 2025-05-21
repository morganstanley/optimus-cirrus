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
import msjava.msnet.ssl.SSLEngineBuilder;
import msjava.msnet.ssl.SSLEngineConfig;
import msjava.msnet.ssl.SSLEstablisher;
import javax.net.ssl.SSLEngine;
public class MSNetSSLSocketFactory extends MSNetTCPSocketFactoryNIOImpl {
    protected final SSLEngineBuilder sslEngineBuilder;
    public MSNetSSLSocketFactory() {
        this.sslEngineBuilder = new SSLEngineBuilder(new SSLEngineConfig());
    }
    public MSNetSSLSocketFactory(SSLEngineConfig sslEngineConfig) {
        this.sslEngineBuilder = new SSLEngineBuilder(sslEngineConfig);
    }
    @Override
    public MSNetTCPSocket createMSNetTCPSocket(boolean isServer) throws MSNetIOException {
        SSLEngine sslEngine = sslEngineBuilder.build(isServer);
        return new MSNetSSLSocket(createMSNetTCPSocketImpl(isServer), this, sslEngine);
    }
    @Override
    public MSNetTCPSocket acceptMSNetTCPSocket(MSNetTCPSocketImpl serverSocketImpl) throws MSNetIOException {
        SSLEngine sslEngine = sslEngineBuilder.build(true);
        return new MSNetSSLSocket(acceptMSNetTCPSocketImpl(serverSocketImpl), this, sslEngine);
    }
}