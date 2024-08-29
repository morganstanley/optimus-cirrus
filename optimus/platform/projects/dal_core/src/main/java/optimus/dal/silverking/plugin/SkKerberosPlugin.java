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
package optimus.dal.silverking.plugin;

import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Set;

import com.google.common.collect.Sets;
import com.ms.silverking.net.security.AuthenticationFailedAction;
import com.ms.silverking.net.security.AuthenticationResult;
import com.ms.silverking.net.security.Authenticator;
import com.ms.silverking.text.ObjectDefParser2;
import msjava.msnet.AbstractEstablisherHandler;
import msjava.msnet.MSNetEstablishException;
import optimus.dal.ZookeeperLogSuppression;
import org.slf4j.Logger;

public class SkKerberosPlugin extends Authenticator {
  public static final String AccessAllowListProp = "optimus.dal.silverking.userAccessAllowList";

  private static String userName;
  private static Set<String> userAccessAllowList;
  private static String wildcardUser;
  private static Logger log;

  private AuthenticationFailedAction clientActionForAccessDeclined;
  private AuthenticationFailedAction clientActionForOtherFailure;

  private AuthenticationFailedAction serverActionForAccessDeclined;
  private AuthenticationFailedAction serverActionForOtherFailure;

  static {
    ObjectDefParser2.addParser(
        new SkKerberosPlugin(
            AuthenticationFailedAction.THROW_NON_RETRYABLE,
            AuthenticationFailedAction.THROW_RETRYABLE,
            AuthenticationFailedAction.THROW_RETRYABLE,
            AuthenticationFailedAction.THROW_RETRYABLE));
    userName = System.getProperty("user.name");
    wildcardUser = "*";
    if (System.getProperty(AccessAllowListProp) == null) {
      userAccessAllowList = Sets.newHashSet();
    } else {
      userAccessAllowList = Sets.newHashSet(System.getProperty(AccessAllowListProp).split(","));
    }
    log = getLogger(SkKerberosPlugin.class);
  }

  public static void setUserAccessAllowList(Set<String> newAccessAllowList) {
    userAccessAllowList = newAccessAllowList;
  }

  public SkKerberosPlugin(
      AuthenticationFailedAction clientActionForAccessDeclined,
      AuthenticationFailedAction clientActionForOtherFailure,
      AuthenticationFailedAction serverActionForAccessDeclined,
      AuthenticationFailedAction serverActionForOtherFailure) {
    this.clientActionForAccessDeclined = clientActionForAccessDeclined;
    this.clientActionForOtherFailure = clientActionForOtherFailure;
    this.serverActionForAccessDeclined = serverActionForAccessDeclined;
    this.serverActionForOtherFailure = serverActionForOtherFailure;
    // N.b. this suppresses some annoying ZK lines that churn the SK logs
    // logback and log4j setups don't correctly suppress these lines
    // it can however be done at runtime; the Kerberos plugin provides a handy place to do so.
    // Note that this isn't done statically! this is so the call is definitely later than static
    // logging setups
    // which means we definitely don't get overwritten by whatever ZK does to avoid our
    // logback/log4j
    ZookeeperLogSuppression.suppress();
  }

  private boolean isPermittedUser(String user) {
    Set<String> allowList = getUserAccessAllowList();
    return user != null
        && (user.equals(userName) || allowList.contains(user) || allowList.contains(wildcardUser));
  }

  public String getUserName() {
    return userName;
  }

  public Set<String> getUserAccessAllowList() {
    return userAccessAllowList;
  }

  @Override
  public String getName() {
    return "[MSInternalPlugin]" + SkKerberosPlugin.class.getCanonicalName();
  }

  @Override
  public Authenticator createLocalCopy() {
    return this; // Safe to be shared (assume Socket will be different)
  }

  @Override
  public AuthenticationFailedAction onAuthTimeout(boolean serverside) {
    if (serverside) {
      return serverActionForOtherFailure;
    } else {
      return clientActionForOtherFailure;
    }
  }

  protected String doKerberosAuth(Socket unauthNetwork, boolean serverside, int timeoutInMillis)
      throws MSNetEstablishException {
    return AbstractEstablisherHandler.kerberosAuth(unauthNetwork, serverside, timeoutInMillis);
  }

  private static final byte authorizationSuccessByte = 1;

  private void tryCleanupSocket(AuthenticationResult authRes, Socket sock, int originalTimeout) {
    try {
      if ((authRes == null || authRes.isFailed()) && !sock.isClosed()) {
        // no need to reset socket timeout back if we're going to close it anyway
        sock.close();
      } else {
        sock.setSoTimeout(originalTimeout);
      }
    } catch (IOException ioe) {
      log.warn("Failed to cleanup socket", ioe);
    }
  }

  private AuthenticationResult authenticateAuthorizeServerSide(
      Socket serverSock, int timeoutMillis) {
    AuthenticationResult result = null;
    int originalTimeout = 0;
    try {
      // Stage 1: Authentication handshake between client and server
      String clientAuthId = doKerberosAuth(serverSock, true, timeoutMillis);
      // Post-condition: client and server both succeed or fail (MSNetEstablishException)

      // Stage 2: Authorization and send result byte to client
      // Pre-condition: serverSock is already authenticated (otherwise move to catch
      // MSNetEstablishException)
      if (isPermittedUser(clientAuthId)) {
        originalTimeout = serverSock.getSoTimeout();
        serverSock.setSoTimeout(timeoutMillis);
        serverSock.getOutputStream().write(authorizationSuccessByte);
        result = Authenticator.createAuthSuccessResult(clientAuthId);
      } else {
        String msg =
            "Authenticator fails to authorize: user "
                + clientAuthId
                + " has not been allowlisted. The "
                + "allowlist "
                + "contains "
                + getUserAccessAllowList();
        log.info(msg);
        result =
            Authenticator.createAuthFailResult(serverActionForAccessDeclined, new Exception(msg));
      }
    } catch (MSNetEstablishException mse) {
      // come from State 1
      log.error(
          "Authenticator fails to authenticate on server side to remote client: "
              + serverSock.getRemoteSocketAddress(),
          mse);
      result = Authenticator.createAuthFailResult(serverActionForOtherFailure, mse);
    } catch (SocketTimeoutException ste) {
      // come from Stage 2 (authorization timeout)
      log.error(
          "Authenticator fails to authorize, timeout in server writing authorization result to remote client: "
              + serverSock.getRemoteSocketAddress(),
          ste);
      result = Authenticator.createAuthFailResult(serverActionForOtherFailure, ste);
    } catch (IOException ioe) {
      // come from Stage 2 (some other exceptions like network issue, get/setSoTimeout)
      log.error(
          "Authenticator fails to authorize on server side to remote client: "
              + serverSock.getRemoteSocketAddress(),
          ioe);
      result = Authenticator.createAuthFailResult(serverActionForOtherFailure, ioe);
    } finally {
      tryCleanupSocket(result, serverSock, originalTimeout);
    }
    return result;
  }

  private AuthenticationResult authenticateAuthorizeClientSide(
      Socket clientSock, int timeoutMillis) {
    AuthenticationResult result = null;
    int originalTimeout = 0;
    try {
      // Stage 1: Authentication handshake between client and server
      String serverAuthId = doKerberosAuth(clientSock, false, timeoutMillis);
      // Post-condition: client and server both succeed or fail (MSNetEstablishException)

      // Stage 2: Wait for server's authorization result byte
      // Pre-condition: clientSock is already authenticated (otherwise move to catch
      // MSNetEstablishException)
      originalTimeout = clientSock.getSoTimeout();
      clientSock.setSoTimeout(timeoutMillis);
      int signal = clientSock.getInputStream().read();
      if (signal == -1) {
        String msg =
            "Authenticator fails to receive authorization result, since server socket is already closed: "
                + clientSock.getRemoteSocketAddress();
        log.error(msg);
        result =
            Authenticator.createAuthFailResult(clientActionForAccessDeclined, new Exception(msg));
      } else {
        // receive 1 byte from server: authorization succeeds
        result = Authenticator.createAuthSuccessResult(serverAuthId);
      }
    } catch (MSNetEstablishException mse) {
      // come from Stage 1
      log.error(
          "Authenticator fails to authenticate on client side to remote server: "
              + clientSock.getRemoteSocketAddress(),
          mse);
      result = Authenticator.createAuthFailResult(clientActionForOtherFailure, mse);
    } catch (SocketTimeoutException ste) {
      // come from Stage 2 (authorization timeout)
      log.error(
          "Authenticator fails to authorize, timeout in client waiting for authorization result from remote server: "
              + clientSock.getRemoteSocketAddress(),
          ste);
      result = Authenticator.createAuthFailResult(clientActionForOtherFailure, ste);
    } catch (IOException ioe) {
      // come from Stage 2: (some other exceptions like network issue, get/setSoTimeout)
      log.error(
          "Authenticator fails to authorize on client side to remote server: "
              + clientSock.getRemoteSocketAddress(),
          ioe);
      result = Authenticator.createAuthFailResult(clientActionForOtherFailure, ioe);
    } finally {
      tryCleanupSocket(result, clientSock, originalTimeout);
    }
    return result;
  }

  @Override
  public AuthenticationResult syncAuthenticate(
      Socket unauthNetwork, boolean serverside, int timeoutMillis) {
    return serverside
        ? authenticateAuthorizeServerSide(unauthNetwork, timeoutMillis)
        : authenticateAuthorizeClientSide(unauthNetwork, timeoutMillis);
  }
}
