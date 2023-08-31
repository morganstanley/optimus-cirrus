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

/** Thrown to indicate that an attempted comparison is invalid */
public class IncomparableException extends RuntimeException {
  private static final long serialVersionUID = 9003263057703665275L;

  public IncomparableException() {
    super();
  }

  public IncomparableException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public IncomparableException(String message, Throwable cause) {
    super(message, cause);
  }

  public IncomparableException(String message) {
    super(message);
  }

  public IncomparableException(Throwable cause) {
    super(cause);
  }
}
