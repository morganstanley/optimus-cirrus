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
package optimus.platform.nameservice;

import msjava.base.dns.AsyncNameService;
import msjava.base.dns.CachingNameService;
import msjava.base.dns.MSNameService;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.spi.InetAddressResolver;
import java.net.spi.InetAddressResolver.LookupPolicy;
import java.net.spi.InetAddressResolverProvider;
import java.util.stream.Stream;

/**
 * Builds an InetAddressResolver which uses the MSJava AsyncNameService and CachingNameService to
 * wrap the underlying builtinResolver. This provides "asynchronous" (well, on a different thread,
 * and with a timeout) and cached lookups.
 *
 * <p>This gets loaded automatically because it's specified in
 * META-INF/services/java.net.spi.InetAddressResolverProvider, but only on Java 18 or higher (since
 * this SPI didn't exist until Java 18). On Java 17 and lower we instead call
 * MSNameServiceRegistrar.registerDefault() which does some reflective hackery.
 */

// Do not use any scala types in this class. It's important that this class does not cause scala
// classes to be loaded as we need to be able to intercept class loading which happens in
// InstrumentationCmds.java
// Also do not do any eager initialization here such as logback etc. These slow down app strat-up
// and we really don't need to be logging in this class.
public class MsJavaInetAddressResolverProvider extends InetAddressResolverProvider {
  // we end up with resolver = MSNameServiceToInetAddressResolverAdapter --> CachingNameService -->
  //    AsyncNameService --> InetAddressResolverToMSNameServiceAdapter -->
  // configuration.builtinResolver
  @Override
  public InetAddressResolver get(Configuration configuration) {
    var adaptedBuiltinResolver =
        new InetAddressResolverToMSNameServiceAdapter(configuration.builtinResolver());
    long timeout = Long.getLong(AsyncNameService.TIMEOUT_MS_PROPERTY, 60000L);
    AsyncNameService asyncNameService =
        new AsyncNameService.Builder().delegate(adaptedBuiltinResolver).timeoutMs(timeout).build();
    var cachingNameService = new CachingNameService.Builder().delegate(asyncNameService).build();
    return new MSNameServiceToInetAddressResolverAdapter(cachingNameService);
  }

  @Override
  public String name() {
    return getClass().getName();
  }

  /** Adapts a MSNameService to the Java InetAddressResolver API */
  private static class MSNameServiceToInetAddressResolverAdapter implements InetAddressResolver {
    private final MSNameService underlying;

    public MSNameServiceToInetAddressResolverAdapter(MSNameService underlying) {
      this.underlying = underlying;
    }

    @Override
    public Stream<InetAddress> lookupByName(String host, LookupPolicy lookupPolicy)
        throws UnknownHostException {
      return Stream.of(underlying.lookupAllHostAddr(host));
    }

    @Override
    public String lookupByAddress(byte[] addr) throws UnknownHostException {
      return underlying.getHostByAddr(addr);
    }
  }

  /** Adapts a Java InetAddressResolver to the MSNameService API */
  private static class InetAddressResolverToMSNameServiceAdapter implements MSNameService {
    private final InetAddressResolver underlying;
    private final LookupPolicy policy =
        LookupPolicy.of(LookupPolicy.IPV4 | LookupPolicy.IPV6 | LookupPolicy.IPV4_FIRST);

    public InetAddressResolverToMSNameServiceAdapter(InetAddressResolver underlying) {
      this.underlying = underlying;
    }

    @Override
    public InetAddress[] lookupAllHostAddr(String hostname) throws UnknownHostException {
      return underlying.lookupByName(hostname, policy).toArray(InetAddress[]::new);
    }

    @Override
    public String getHostByAddr(byte[] addr) throws UnknownHostException {
      return underlying.lookupByAddress(addr);
    }
  }
}
