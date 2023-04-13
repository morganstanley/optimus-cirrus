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

public enum AuthorizationFailedAction {
  /**
   * SilverKing will throw a exception (for now silverking will catch it in server side and log the error)
   */
  THROW_EXCEPTION,
  /**
   * Default behaviour if no user plugin - silverking will continue the operation despite authorization
   */
  GO_WITHOUT_AUTH,
  /**
   * Silverking will swallow the request and respond with an error; the operation is not started
   */
  RETURN_ERROR_RESPONSE
}
