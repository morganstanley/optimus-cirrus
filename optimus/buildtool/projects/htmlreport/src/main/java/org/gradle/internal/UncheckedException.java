/*
 * Copyright 2010 the original author or authors. (see https://github.com/gradle/gradle which also uses Apache 2.0)
 *
 * Modifications were made to that code for compatibility with Optimus Build Tool and its report file layout.
 * For those changes only, where additions and modifications are indicated with 'ms' in comments:
 *
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

package org.gradle.internal;

import java.io.IOException;

import org.gradle.api.UncheckedIOException;

/** Wraps a checked exception. Carries no other context. */
public final class UncheckedException extends RuntimeException {
  public UncheckedException(Throwable cause) {
    super(cause);
  }

  private UncheckedException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Note: always throws the failure in some form. The return value is to keep the compiler happy.
   */
  public static RuntimeException throwAsUncheckedException(Throwable t) {
    return throwAsUncheckedException(t, false);
  }

  /**
   * Note: always throws the failure in some form. The return value is to keep the compiler happy.
   */
  public static RuntimeException throwAsUncheckedException(Throwable t, boolean preserveMessage) {
    if (t instanceof RuntimeException) {
      throw (RuntimeException) t;
    }
    if (t instanceof Error) {
      throw (Error) t;
    }
    if (t instanceof IOException) {
      if (preserveMessage) {
        throw new UncheckedIOException(t.getMessage(), t);
      } else {
        throw new UncheckedIOException(t);
      }
    }
    if (preserveMessage) {
      throw new UncheckedException(t.getMessage(), t);
    } else {
      throw new UncheckedException(t);
    }
  }
}
