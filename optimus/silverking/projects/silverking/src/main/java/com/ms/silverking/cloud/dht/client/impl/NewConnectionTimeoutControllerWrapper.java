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
package com.ms.silverking.cloud.dht.client.impl;

import com.ms.silverking.cloud.dht.client.SessionEstablishmentTimeoutController;
import com.ms.silverking.net.AddrAndPort;
import com.ms.silverking.net.async.NewConnectionTimeoutController;

/**
 * Allows a SessionEstablishmentTimeoutController to be used as the basis of a NewConnectionTimeoutController
 */
class NewConnectionTimeoutControllerWrapper implements NewConnectionTimeoutController {
  private final SessionEstablishmentTimeoutController sessionEstablishmentTimeoutController;

  NewConnectionTimeoutControllerWrapper(SessionEstablishmentTimeoutController sessionEstablishmentTimeoutController) {
    this.sessionEstablishmentTimeoutController = sessionEstablishmentTimeoutController;
  }

  @Override
  public int getMaxAttempts(AddrAndPort addrAndPort) {
    return sessionEstablishmentTimeoutController.getMaxAttempts(null);
  }

  @Override
  public int getRelativeTimeoutMillisForAttempt(AddrAndPort addrAndPort, int attemptIndex) {
    return sessionEstablishmentTimeoutController.getRelativeTimeoutMillisForAttempt(null, attemptIndex);
  }

  @Override
  public int getMaxRelativeTimeoutMillis(AddrAndPort addrAndPort) {
    return sessionEstablishmentTimeoutController.getMaxRelativeTimeoutMillis(null);
  }
}
