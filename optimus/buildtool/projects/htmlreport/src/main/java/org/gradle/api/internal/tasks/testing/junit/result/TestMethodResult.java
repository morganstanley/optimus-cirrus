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

import org.gradle.api.tasks.testing.TestResult;

import java.util.ArrayList;
import java.util.List;

public class TestMethodResult {
  private final long id;
  private final String name;
  private final String displayName;
  private TestResult.ResultType resultType;
  private long duration;
  private long endTime;
  private List<TestFailure> failures = new ArrayList<TestFailure>();

  public TestMethodResult(long id, String name, String displayName) {
    this.id = id;
    this.name = name;
    this.displayName = displayName;
  }

  public TestMethodResult(
      long id,
      String name,
      String displayName,
      TestResult.ResultType resultType,
      long duration,
      long endTime) {
    if (id < 1) {
      throw new IllegalArgumentException("id must be > 0");
    }
    this.id = id;
    this.name = name;
    this.displayName = displayName;
    this.resultType = resultType;
    this.duration = duration;
    this.endTime = endTime;
  }

  public TestMethodResult completed(TestResult result) {
    resultType = result.getResultType();
    duration = result.getEndTime() - result.getStartTime();
    endTime = result.getEndTime();
    return this;
  }

  public TestMethodResult addFailure(String message, String stackTrace, String exceptionType) {
    this.failures.add(new TestFailure(message, stackTrace, exceptionType));
    return this;
  }

  public long getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getDisplayName() {
    return displayName;
  }

  public List<TestFailure> getFailures() {
    return failures;
  }

  public TestResult.ResultType getResultType() {
    return resultType;
  }

  public long getDuration() {
    return duration;
  }

  public long getEndTime() {
    return endTime;
  }
}
