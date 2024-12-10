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
package optimus.platform.nameservice

/* import msjava.base.dns.AsyncNameService
import msjava.base.dns.CachingNameService
import msjava.base.dns.MSNameService */
import msjava.slf4jutils.scalalog.getLogger

import java.net.InetAddress
import java.net.spi.InetAddressResolver
import java.net.spi.InetAddressResolver.LookupPolicy
import java.net.spi.InetAddressResolverProvider
import java.util.stream.Stream

/**
 * Builds an InetAddressResolver which uses the MSJava AsyncNameService and CachingNameService to wrap the underlying
 * builtinResolver. This provides "asynchronous" (well, on a different thread, and with a timeout) and cached lookups.
 *
 * This gets loaded automatically because it's specified in META-INF/services/java.net.spi.InetAddressResolverProvider,
 * but only on Java 18 or higher (since this SPI didn't exist until Java 18). On Java 17 and lower we instead call
 * MSNameServiceRegistrar.registerDefault() which does some reflective hackery.
 */
class MsJavaInetAddressResolverProvider extends InetAddressResolverProvider {
  private val log = getLogger(this)

  // we end up with resolver = MSNameServiceToInetAddressResolverAdapter --> CachingNameService -->
  //    AsyncNameService --> InetAddressResolverToMSNameServiceAdapter --> configuration.builtinResolver
  override def get(configuration: InetAddressResolverProvider.Configuration): InetAddressResolver = ??? /* {
    log.info(s"Constructing MsJavaInetAddressResolver with async and cached resolution")
    val adaptedBuiltinResolver = new InetAddressResolverToMSNameServiceAdapter(configuration.builtinResolver())
    val timeout: Long = sys.props.get(AsyncNameService.TIMEOUT_MS_PROPERTY).map(_.toLong).getOrElse(60000L)
    val asyncNameService = new AsyncNameService.Builder().delegate(adaptedBuiltinResolver).timeoutMs(timeout).build
    val cachingNameService = new CachingNameService.Builder().delegate(asyncNameService).build
    new MSNameServiceToInetAddressResolverAdapter(cachingNameService)
  } */
  override def name(): String = getClass.getName
}

/** adapts a MSNameService to the Java InetAddressResolver API */
/* private class MSNameServiceToInetAddressResolverAdapter(underlying: MSNameService) extends InetAddressResolver {
  override def lookupByName(host: String, lookupPolicy: InetAddressResolver.LookupPolicy): Stream[InetAddress] =
    Stream.of(underlying.lookupAllHostAddr(host): _*)

  override def lookupByAddress(addr: Array[Byte]): String =
    underlying.getHostByAddr(addr)

}

/** adapts a  Java InetAddressResolverto the MSNameService API */
private class InetAddressResolverToMSNameServiceAdapter(underlying: InetAddressResolver) extends MSNameService {
  private val policy = LookupPolicy.of(LookupPolicy.IPV4 | LookupPolicy.IPV6 | LookupPolicy.IPV4_FIRST)

  override def lookupAllHostAddr(hostname: String): Array[InetAddress] =
    underlying.lookupByName(hostname, policy).toArray(new Array[InetAddress](_))

  override def getHostByAddr(addr: Array[Byte]): String = underlying.lookupByAddress(addr)
} */
