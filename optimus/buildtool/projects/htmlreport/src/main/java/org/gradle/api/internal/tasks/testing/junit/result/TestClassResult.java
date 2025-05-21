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

public class TestClassResult {
  private final List<TestMethodResult> methodResults = new ArrayList<TestMethodResult>();
  private final String className;
  private long startTime;
  private int failuresCount;
  private int skippedCount;
  private long id;

  public TestClassResult(long id, String className, long startTime) {
    if (id < 1) {
      throw new IllegalArgumentException("id must be > 0");
    }
    this.id = id;
    this.className = className;
    this.startTime = startTime;
  }

  public long getId() {
    return id;
  }

  public String getClassName() {
    return className;
  }

  public TestClassResult add(TestMethodResult methodResult) {
    if (methodResult.getResultType() == TestResult.ResultType.FAILURE) {
      failuresCount++;
    }
    if (methodResult.getResultType() == TestResult.ResultType.SKIPPED) {
      skippedCount++;
    }
    methodResults.add(methodResult);
    return this;
  }

  public List<TestMethodResult> getResults() {
    return methodResults;
  }

  public long getStartTime() {
    return startTime;
  }

  public int getTestsCount() {
    return methodResults.size();
  }

  public int getFailuresCount() {
    return failuresCount;
  }

  public int getSkippedCount() {
    return skippedCount;
  }

  public long getDuration() {
    long end = startTime;
    for (TestMethodResult m : methodResults) {
      if (end < m.getEndTime()) {
        end = m.getEndTime();
      }
    }
    return end - startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public String getXmlTestSuiteName() {
    return className;
  }
}
