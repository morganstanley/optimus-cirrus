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
package com.ms.silverking.net.security;

import java.net.Socket;

import com.ms.silverking.text.FieldsRequirement;
import com.ms.silverking.text.ObjectDefParser2;
import com.ms.silverking.util.PropertiesHelper;

public abstract class Authenticator {
  public static final String authImplProperty = Authenticator.class.getPackage().getName() + ".AuthImplSKDef";
  private static Authenticator singleton;
  private static final String emptyDef = "";

  static {
    ObjectDefParser2.addParserWithExclusions(Authenticator.class, null, FieldsRequirement.ALLOW_INCOMPLETE, null);
    String def = PropertiesHelper.systemHelper.getString(authImplProperty, "");
    setAuthenticator(def);
  }

  public String toSKDef() {
    return ObjectDefParser2.toClassAndDefString(this);
  }

  static Authenticator parseSKDef(String skDef) {
    return ObjectDefParser2.parse(skDef, Authenticator.class.getPackage());
  }

  public static void setAuthenticator(String def) {
    if (def == null || def.equals(emptyDef)) {
      singleton = new NoopAuthenticatorImpl();
    } else {
      singleton = parseSKDef(def);
    }
  }

  public static Authenticator getAuthenticator() {
    return singleton;
  }

  public static boolean isNoopAuthenticator() {
    return singleton instanceof NoopAuthenticatorImpl;
  }

  private static AuthenticationFailedAction defAction = AuthenticationFailedAction.THROW_NON_RETRYABLE;

  // Factory constructor for AUTH_SUCCESS and AUTH_FAIL
  public static AuthenticationResult createAuthSuccessResult(String authId) {
    assert authId != null;
    return new AuthenticationResult(authId, defAction, null);
  }

  public static AuthenticationResult createAuthFailResult(AuthenticationFailedAction action, Throwable cause) {
    assert action != null;
    return new AuthenticationResult(null, action, cause);
  }

  static AuthenticationResult createAuthFailResult(AuthenticationFailedAction action) {
    return createAuthFailResult(action, null);
  }

  /**
   * Used for logging debug/error message to locate the concrete Authenticator implementation
   *
   * @return a distinguishable name
   */
  public abstract String getName();

  /**
   * A method to implicitly indicate the thread safety of Authenticator implementation;
   * Silverking will use this method to create a ThreadLocal Authenticator instance
   * <p>
   * <br/>
   * It's implementor's responsibility to decide to
   * <b>make a real deep copy/clone (instance is not thread-safe to be shared)<b/> OR
   * <b>simply return "this"(instance is thread-safe to be shared)</b>
   *
   * @return a local "copy" of Authenticator
   */
  public abstract Authenticator createLocalCopy();

  /**
   * Silverking itself may give up and cancel syncAuthenticate() if it takes too long,
   * and this function is used to indicate when Silverking cancels the syncAuthenticate() on its behalf, what's the
   * next action to do
   *
   * @param serverside <b>true</b> if authenticating in the DHTNodes who acts like server(receives data);
   *                   <b>false</b> in the DHTNodes
   *                   who acts like client(send data), or DHTClient
   * @return corresponding action for silverking to take when timeout
   */
  public abstract AuthenticationFailedAction onAuthTimeout(boolean serverside);

  /**
   * Allow user to inject authentication before the Silverking network communication between DHTClient and DHTNode
   * (Server) or between two distributed DHTNodes(servers)
   * <p>
   * This function is sync and called before Silverking starts to send data on the given socket; The given socket is
   * connected and sync(blocking) at this moment; After this function call, silverking will turn the socket into
   * async(non-blocking) mode,
   * and user shall <b>NOT</b> re-used the given socket
   * <p>
   * <br/><b>NOTE:</b> Currently, Silverking gives the full control to Authenticator, so it's Authenticator's
   * responsibility to handle the timeout
   * (Silverking will simply be blocked on this method)
   *
   * @param unauthNetwork        the <b>connected</b> and <b>sync</b> raw Socket between client and server, which
   *                             has been not authenticated yet
   * @param serverside           <b>true</b> if authenticating in the DHTNodes who acts like server(receives data);
   *                             <b>false</b> in the DHTNodes
   *                             who acts like client(send data), or DHTClient
   * @param timeoutInMillisecond the maximum time to execute this authentication;
   *                             when timeout, a corresponding AuthResult shall be returned
   * @return a <b>String</b> id of the authentication succeeds, or an <b>empty</b> if authentication fails
   */
  public abstract AuthenticationResult syncAuthenticate(final Socket unauthNetwork, boolean serverside,
      int timeoutInMillisecond);

  public static void checkForAuthFailure(AuthenticationResult authResult, String connInfo, boolean serverside,
      Authenticator authenticator) throws NonRetryableAuthFailedException, RetryableAuthFailedException {
    if (authResult.isFailed()) {
      String msg = "Connection " + connInfo + " fails to be authenticated from " + (serverside ?
          "ServerSide" :
          "ClientSide");
      switch (authResult.getFailedAction()) {
      case GO_WITHOUT_AUTH:
        break;
      case THROW_NON_RETRYABLE:
        throw authResult.getFailCause().isPresent() ? new NonRetryableAuthFailedException(msg,
            authResult.getFailCause().get()) : new NonRetryableAuthFailedException(msg);
      case THROW_RETRYABLE:
        throw authResult.getFailCause().isPresent() ? new RetryableAuthFailedException(msg,
            authResult.getFailCause().get()) : new RetryableAuthFailedException(msg);
      default:
        throw new RuntimeException("Connection " + connInfo + " fails to be authenticated from " + (serverside ?
            "ServerSide" :
            "ClientSide" + " and action for this failure has NOT been defined: " + "please check the behaviour of " + "injected authenticator [" + authenticator.getName() + "]"));
      }
    }
  }
}
