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

public class AuthorizationResult {
  private String authorizedId;
  private AuthorizationFailedAction failedAction;
  private Throwable cause;

  AuthorizationResult(String authorizedId, AuthorizationFailedAction action, Throwable cause) {
    this.authorizedId = authorizedId;
    this.failedAction = action;
    this.cause = cause;
  }

  public boolean isSuccessful() {
    return getAuthorizedId().isPresent();
  }

  public boolean isFailed() {
    return !isSuccessful();
  }

  public Optional<String> getAuthorizedId() {
    return Optional.ofNullable(authorizedId);
  }

  public AuthorizationFailedAction getFailedAction() {
    return failedAction;
  }

  public Optional<Throwable> getFailCause() {
    return Optional.ofNullable(cause);
  }
}
