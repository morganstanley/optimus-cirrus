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
package optimus.dht.common.internal.transport;

import java.security.Principal;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.stream.Collectors;

import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.net.ssl.SSLSession;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SslHandshakeCompletedInboundChannelHandler extends ChannelInboundHandlerAdapter {

  private static final Logger logger =
      LoggerFactory.getLogger(SslHandshakeCompletedInboundChannelHandler.class);

  private final SslHandler sslHandler;
  private final ChannelSharedState channelSharedState;

  public SslHandshakeCompletedInboundChannelHandler(
      SslHandler sslHandler, ChannelSharedState channelSharedState) {
    this.sslHandler = sslHandler;
    this.channelSharedState = channelSharedState;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    // we do not fire channelActive upstream, until handshake has completed
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {

    if (evt instanceof SslHandshakeCompletionEvent) {
      SslHandshakeCompletionEvent event = (SslHandshakeCompletionEvent) evt;
      if (event.isSuccess()) {
        SSLSession sslSession = sslHandler.engine().getSession();
        X509Certificate[] peerCertificates = convertToX509(sslSession.getPeerCertificates());
        Principal subjectDN = peerCertificates[0].getSubjectDN();
        String[] extractedCN = extractCN(subjectDN);

        String authUsername = extractedCN[0];
        String userDomain = extractedCN[1];

        if (logger.isDebugEnabled()) {
          if (logger.isTraceEnabled()) {
            // only log full certificates at trace level, otherwise output is way too long
            logger.trace(
                "SSL handshake completed, authUsername="
                    + authUsername
                    + ", domain="
                    + userDomain
                    + ", peerCertificates=["
                    + Arrays.stream(peerCertificates)
                        .map(Certificate::toString)
                        .collect(Collectors.joining(","))
                    + "]");
          } else {
            // at the debug level, only log certificates DN
            logger.debug(
                "SSL handshake completed, authUsername="
                    + authUsername
                    + ", domain="
                    + userDomain
                    + ", peerCertificatesDNs=["
                    + Arrays.stream(peerCertificates)
                        .map(c -> "\"" + c.getSubjectDN().getName() + "\"")
                        .collect(Collectors.joining(","))
                    + "]");
          }
        }

        channelSharedState.authenticatedConnection(
            new SslAuthenticatedConnection(
                peerCertificates,
                sslSession.getProtocol(),
                sslSession.getCipherSuite(),
                authUsername,
                userDomain));
        ctx.pipeline().remove(this);
        ctx.fireChannelActive();
      }
    } else {
      ctx.fireUserEventTriggered(evt);
    }
  }

  private static X509Certificate[] convertToX509(Certificate[] certificates) {
    X509Certificate[] x509Certificates = new X509Certificate[certificates.length];
    for (int i = 0; i < certificates.length; ++i) {
      x509Certificates[i] = (X509Certificate) certificates[i];
    }
    return x509Certificates;
  }

  private static String[] extractCN(Principal principal) {
    String dnName = principal.getName();
    try {
      LdapName ldapName = new LdapName(dnName);
      String cn = null;
      for (Rdn rdn : ldapName.getRdns()) {
        if ("CN".equalsIgnoreCase(rdn.getType())) {
          cn = String.valueOf(rdn.getValue());
          break;
        }
      }
      if (cn != null) {
        var atPosition = cn.indexOf('@');
        if (atPosition >= 0) {
          return new String[] {cn.substring(0, atPosition), cn.substring(atPosition + 1)};
        } else {
          return new String[] {cn, ""};
        }
      } else {
        logger.warn("Unable to find CN in " + dnName + ", will use DN instead");
        return new String[] {dnName, ""};
      }
    } catch (Exception e) {
      logger.warn("Unable to extract CN from X509 certificate, will use DN instead", e);
      return new String[] {dnName, ""};
    }
  }
}
