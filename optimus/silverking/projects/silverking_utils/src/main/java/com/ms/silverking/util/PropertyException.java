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
package com.ms.silverking.util;

/** Thrown when a required property is not correctly defined. */
public class PropertyException extends RuntimeException {
  private static final long serialVersionUID = 3109646764798499269L;

  public PropertyException() {}

  public PropertyException(String message) {
    super(message);
  }

  public PropertyException(Throwable cause) {
    super(cause);
  }

  public PropertyException(String message, Throwable cause) {
    super(message, cause);
  }

  public PropertyException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
