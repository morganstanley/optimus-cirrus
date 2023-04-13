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

/**
 * The behaviour for Silverking when auth fails for a connection
 */
public enum AuthenticationFailedAction {
  /**
   * Silverking will throw an exception which will NOT be internally caught to have retries
   */
  THROW_NON_RETRYABLE,
  /**
   * Silverking will throw an exception which will be internally caught to have retries
   */
  THROW_RETRYABLE,
  /**
   * Silverking will continue to work without authentication
   * (May be used if authentication is not a must, and this is Silverking's current default behaviour)
   */
  GO_WITHOUT_AUTH
}
