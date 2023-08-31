/*
 * Copyright 2012 the original author or authors. (see https://github.com/gradle/gradle which also uses Apache 2.0)
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

package org.gradle.api.internal.tasks.testing.junit.result;

import java.io.Closeable;
import java.io.Writer;

import org.gradle.api.Action;
import org.gradle.api.tasks.testing.TestOutputEvent;

public interface TestResultsProvider extends Closeable {
  /**
   * Writes the output of the given test to the given writer. This method must be called only after
   * {@link #visitClasses(org.gradle.api.Action)}.
   *
   * <p>Writes all output for the test class.
   */
  void writeAllOutput(long id, TestOutputEvent.Destination destination, Writer writer);

  /**
   * Visits the results of each test class, in no specific order. Each class is visited exactly
   * once.
   */
  void visitClasses(Action<? super TestClassResult> visitor);

  boolean hasOutput(long id, TestOutputEvent.Destination destination);
}
