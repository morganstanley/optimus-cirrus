/*
 * Copyright 2011 the original author or authors. (see https://github.com/gradle/gradle which also uses Apache 2.0)
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
package org.gradle.api.internal.tasks.testing.junit.report;

import org.gradle.internal.time.TimeFormatting;

public abstract class TestResultModel {

  public abstract org.gradle.api.tasks.testing.TestResult.ResultType
      getResultType(); // ms: https://github.com/scala/bug/issues/6589

  public abstract long getDuration();

  public abstract String getTitle();

  public String getFormattedDuration() {
    return TimeFormatting.formatDurationVeryTerse(getDuration());
  }

  public String getStatusClass() {
    switch (getResultType()) {
      case SUCCESS:
        return "success";
      case FAILURE:
        return "failures";
      case SKIPPED:
        return "skipped";
      default:
        throw new IllegalStateException();
    }
  }

  public String getFormattedResultType() {
    switch (getResultType()) {
      case SUCCESS:
        return "passed";
      case FAILURE:
        return "failed";
      case SKIPPED:
        return "ignored";
      default:
        throw new IllegalStateException();
    }
  }
}
