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

package org.gradle.api.tasks.testing;

/** Describes a test result. */
public interface TestResult {
  /** The final status of a test. */
  public enum ResultType {
    SUCCESS,
    FAILURE,
    SKIPPED
  }

  /**
   * Returns the type of result. Generally one wants it to be SUCCESS!
   *
   * @return The result type.
   */
  ResultType getResultType();

  /**
   * Returns the time when this test started execution.
   *
   * @return The start time, in milliseconds since the epoch.
   */
  long getStartTime();

  /**
   * Returns the time when this test completed execution.
   *
   * @return The end t ime, in milliseconds since the epoch.
   */
  long getEndTime();
}
