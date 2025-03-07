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
package optimus.core.utils

import msjava.kerberos.auth.MSKerberosConfiguration
import org.apache.http.HttpRequest
import org.apache.http.HttpRequestInterceptor
import org.apache.http.HttpResponse
import org.apache.http.auth.AuthScope
import org.apache.http.auth.Credentials
import org.apache.http.client.config.CookieSpecs
import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.message.BasicHeader
import org.apache.http.protocol.HttpContext
import org.apache.http.HttpHeaders
import org.apache.http.config.ConnectionConfig

import java.util.UUID

object HttpClientOIDC {
  {
    MSKerberosConfiguration.setClientConfiguration()
  }
  def create(connectionTimeout: Int = 30000, nThreads: Int = 1): CloseableHttpClient = {
    val credentialsProvider = new BasicCredentialsProvider
    credentialsProvider.setCredentials(
      new AuthScope(null, -1, null),
      new Credentials {
        override def getUserPrincipal = null
        override def getPassword = null
      })

    val requestConfig =
      RequestConfig
        .custom()
        .setExpectContinueEnabled(true)
        .setCookieSpec(CookieSpecs.STANDARD)
        .setAuthenticationEnabled(true)
        .setConnectTimeout(connectionTimeout)
        .setConnectionRequestTimeout(connectionTimeout)
        .build

    val httpClientBuilder =
      HttpClients.custom
        .setDefaultConnectionConfig(ConnectionConfig.custom().setBufferSize(10 * 1000 * 1000).build())
        .setDefaultRequestConfig(requestConfig)
        .addInterceptorFirst(httpRequestInterceptor)
        .setDefaultCredentialsProvider(credentialsProvider)
        .setKeepAliveStrategy(keepAliveStrategy)
    if (nThreads > 1) {
      val poolingManager = new PoolingHttpClientConnectionManager
      poolingManager.setDefaultMaxPerRoute(nThreads * 2)
      httpClientBuilder.setConnectionManager(poolingManager)
    }
    httpClientBuilder.build()
  }

  val keepAliveStrategy = new DefaultConnectionKeepAliveStrategy() {
    override def getKeepAliveDuration(response: HttpResponse, context: HttpContext): Long = {
      var aliveDuration = super.getKeepAliveDuration(response, context)
      if (aliveDuration == -1L) {
        aliveDuration = 10000
      }
      aliveDuration
    }
  }
  private object httpRequestInterceptor extends HttpRequestInterceptor {
    override def process(req: HttpRequest, httpContext: HttpContext) = {
      req.setHeader(new BasicHeader(HttpHeaders.ACCEPT, "*/*")) // */* is MimeType ALL
      req.setHeader(new BasicHeader("UUID", UUID.randomUUID.toString))
    }
  }
}
