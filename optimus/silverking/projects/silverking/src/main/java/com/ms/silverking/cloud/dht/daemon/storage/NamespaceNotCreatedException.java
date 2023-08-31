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
package com.ms.silverking.cloud.dht.daemon.storage;

/**
 * Indicates that an operation that requires a prior explicit namespace creation cannot find the
 * required namespace.
 */
public class NamespaceNotCreatedException extends RuntimeException {
  public NamespaceNotCreatedException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public NamespaceNotCreatedException(String message, Throwable cause) {
    super(message, cause);
  }

  public NamespaceNotCreatedException(String message) {
    super(message);
  }
}
