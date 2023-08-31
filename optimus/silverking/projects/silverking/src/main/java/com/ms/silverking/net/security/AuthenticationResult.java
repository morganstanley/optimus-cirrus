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

public class AuthenticationResult {
  private String authenticatedId;
  // We encapsulate AuthFailedAction in AuthResult so that Authenticator can define different
  // actions for different
  // authentication situation
  // this action currently is only used if <i>isFailed()</i> returns true
  private AuthenticationFailedAction failedAction;
  private Throwable cause;

  /**
   * @param authenticatedId AuthenticationId; <b>null</b> if authentication fails
   * @param action action for Silverking to take when authentication fails
   */
  AuthenticationResult(String authenticatedId, AuthenticationFailedAction action, Throwable cause) {
    this.authenticatedId = authenticatedId;
    this.failedAction = action;
    this.cause = cause;
  }

  public boolean isSuccessful() {
    return getAuthenticatedId().isPresent();
  }

  public boolean isFailed() {
    return !isSuccessful();
  }

  public Optional<String> getAuthenticatedId() {
    return Optional.ofNullable(authenticatedId);
  }

  public AuthenticationFailedAction getFailedAction() {
    return failedAction;
  }

  public Optional<Throwable> getFailCause() {
    return Optional.ofNullable(cause);
  }
}
