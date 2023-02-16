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

import java.io.IOException;
import java.util.Objects;

import javax.annotation.Nullable;
import javax.net.ssl.SSLEngine;

import msjava.msnet.ssl.SSLEngineBuilder;
import msjava.msnet.ssl.SSLEngineConfig;
import msjava.msnet.ssl.SSLEngineFactory;
import msjava.msnet.ssl.SSLEstablisher;

/**
 * For general library overview and code examples refer to the {@link SSLEstablisher} documentation.
 */
public class MSNetSSLSocketFactory extends MSNetTCPSocketFactoryNIOImpl {

  private final boolean slicingBuffers;

  @Nullable protected final SSLEngineBuilder sslEngineBuilder;
  @Nullable private volatile SSLEngineFactory sslEngineFactory;

  public MSNetSSLSocketFactory() {
    slicingBuffers = false;
    this.sslEngineBuilder = new SSLEngineBuilder(new SSLEngineConfig());
    sslEngineFactory = null;
  }

  public MSNetSSLSocketFactory(SSLEngineConfig sslEngineConfig) {
    slicingBuffers = false;
    this.sslEngineBuilder = new SSLEngineBuilder(sslEngineConfig);
    sslEngineFactory = null;
  }

  public MSNetSSLSocketFactory(SSLEngineFactory sslEngineFactory, boolean slicingBuffers) {
    this.slicingBuffers = slicingBuffers;
    this.sslEngineBuilder = null;
    this.sslEngineFactory = Objects.requireNonNull(sslEngineFactory);
  }

  @Override
  public MSNetTCPSocket createMSNetTCPSocket(boolean isServer) throws MSNetIOException {
    SSLEngine sslEngine = makeSSLEngine(isServer);
    if (sslEngineFactory == null) {
      return new MSNetSSLSocket(
          createMSNetTCPSocketImpl(isServer), this, sslEngine, slicingBuffers);
    } else {
      final SSLEngineFactory sslEngineFactory = this.sslEngineFactory;
      return new MSNetSSLSocket(
          createMSNetTCPSocketImpl(isServer), this, sslEngine, slicingBuffers) {
        @Override
        public void close() throws IOException {
          super.close();
          sslEngineFactory.dispose(sslEngine);
        }
      };
    }
  }

  @Override
  public MSNetTCPSocket acceptMSNetTCPSocket(MSNetTCPSocketImpl serverSocketImpl)
      throws MSNetIOException {
    SSLEngine sslEngine = makeSSLEngine(true);
    if (sslEngineFactory == null) {
      return new MSNetSSLSocket(
          acceptMSNetTCPSocketImpl(serverSocketImpl), this, sslEngine, slicingBuffers);
    } else {
      final SSLEngineFactory sslEngineFactory = this.sslEngineFactory;
      return new MSNetSSLSocket(
          acceptMSNetTCPSocketImpl(serverSocketImpl), this, sslEngine, slicingBuffers) {
        @Override
        public void close() throws IOException {
          super.close();
          sslEngineFactory.dispose(sslEngine);
        }
      };
    }
  }

  private SSLEngine makeSSLEngine(boolean isServer) {
    if (sslEngineBuilder != null) {
      return sslEngineBuilder.build(isServer);
    } else {
      if (isServer) {
        return Objects.requireNonNull(sslEngineFactory).createServerEngine();
      } else {
        return Objects.requireNonNull(sslEngineFactory).createClientEngine();
      }
    }
  }

  public void setSslEngineFactory(SSLEngineFactory sslEngineFactory) {
    Objects.requireNonNull(sslEngineFactory);
    Objects.requireNonNull(this.sslEngineFactory);
    this.sslEngineFactory = sslEngineFactory;
  }
}
