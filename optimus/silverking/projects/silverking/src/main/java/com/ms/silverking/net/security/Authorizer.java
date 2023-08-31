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

import java.util.Optional;

import com.ms.silverking.text.FieldsRequirement;
import com.ms.silverking.text.ObjectDefParser2;
import com.ms.silverking.util.PropertiesHelper;

/**
 * Plugin point for Authorization In general, using an Authorizer will imply that an Authenticator
 * is also plugged in since you will probably want to authorize against some authenticated user, but
 * this is not enforced in this singleton - if desired, the plugin ought to enforce the presence of
 * an authenticator
 */
public abstract class Authorizer {
  public static final String authorizerImplProperty =
      Authorizer.class.getPackage().getName() + ".AuthorizerImplSKDef";
  private static final String emptyDef = "";

  private static Authorizer singletonAuthorizer;
  private static boolean isEnabled;

  static {
    ObjectDefParser2.addParserWithExclusions(
        Authorizer.class, null, FieldsRequirement.ALLOW_INCOMPLETE, null);
    String authDef = PropertiesHelper.systemHelper.getString(authorizerImplProperty, emptyDef);
    setAuthorizer(authDef);
  }

  public static Authorizer parseSKDef(String skDef) {
    return ObjectDefParser2.parse(skDef, Authenticator.class.getPackage());
  }

  public static boolean isEnabled() {
    return isEnabled;
  }

  public static Authorizer getPlugin() {
    if (isEnabled) {
      return singletonAuthorizer;
    } else {
      throw new RuntimeException("Invalid call to getPlugin when Authorizer was not enabled!");
    }
  }

  public static AuthorizationResult createAuthFailedResult(
      AuthorizationFailedAction action, Throwable cause) {
    assert action != null;
    return new AuthorizationResult(null, action, cause);
  }

  public static AuthorizationResult createAuthSuccessResult(String authorizedId) {
    assert authorizedId != null;
    return new AuthorizationResult(authorizedId, null, null);
  }

  /**
   * Used for logging debug/error message to locate the concrete Authorizer implementation
   *
   * @return a distinguishable name
   */
  public abstract String getName();

  public final String toSKDef() {
    return ObjectDefParser2.toClassAndDefString(this);
  }

  /**
   * Allow user to inject authorization of Silverking communication between DHTClient and DHTNode
   * (Server) or between two distributed DHTNodes(servers)
   *
   * @param authenticated The user authenticated for the current connection
   * @param requestedUser The user to authorize
   * @return an AuthResult instance, which may define a failure action for a rejected authorization
   *     attempt
   */
  public abstract AuthorizationResult syncAuthorize(
      Optional<String> authenticated, byte[] requestedUser);

  public static void setAuthorizer(String authDef) {
    if (authDef == null || authDef.equals(emptyDef)) {
      isEnabled = false;
    } else {
      isEnabled = true;
      singletonAuthorizer = parseSKDef(authDef);
    }
  }
}
