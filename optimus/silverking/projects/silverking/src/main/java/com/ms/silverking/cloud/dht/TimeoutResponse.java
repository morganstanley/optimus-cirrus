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

/**
 * Specifies the response of a WaitFor operation to a timeout. Exit quietly or throw an exception.
 */
public enum TimeoutResponse {
  /** Throw an exception when a WaitFor timeout occurs */
  EXCEPTION,
  /** Ignore the timeout and exit quietly when a WaitFor timeout occurs */
  IGNORE;

  /** By default, throw an exception when a timeout occurs. */
  public static final TimeoutResponse defaultResponse = EXCEPTION;
}
