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
package com.ms.silverking.cloud.dht.client;

/** Enumeration of failure causes. */
public enum FailureCause {
  ERROR,

  TIMEOUT,

  MUTATION,

  // Multiple OpResults for keys in a batch (e.g. 1 Succeed, 1 Error). This is seen as a Failure for
  // the whole batch
  MULTIPLE,

  INVALID_VERSION,

  SIMULTANEOUS_PUT,

  NO_SUCH_VALUE,

  NO_SUCH_NAMESPACE,

  CORRUPT,

  LOCKED,

  SESSION_CLOSED,

  ALL_REPLICAS_EXCLUDED;
}
