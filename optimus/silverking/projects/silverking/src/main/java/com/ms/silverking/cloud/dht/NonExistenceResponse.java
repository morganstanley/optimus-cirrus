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
package com.ms.silverking.cloud.dht;

/** Specifies the response of a Retrieval operation to a non-existing value */
public enum NonExistenceResponse {
  /** Return a null value for keys without associated values */
  NULL_VALUE,
  /** Throw an exception if a key has no associated value */
  EXCEPTION;

  /** By default, non-existence returns null values */
  public static final NonExistenceResponse defaultResponse = NULL_VALUE;
}
