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

import java.math.BigDecimal;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import static org.gradle.api.tasks.testing.TestResult.ResultType;
import org.gradle.api.internal.tasks.testing.junit.result.CoverageResult;

public abstract class CompositeTestResults extends TestResultModel {
  private final CompositeTestResults parent;
  private int tests;
  private final Set<TestResult> failures = new TreeSet<TestResult>();
  private final Set<TestResult> ignored = new TreeSet<TestResult>();
  private long duration;
  private Optional<CoverageResult> coverageResult = Optional.empty();

  protected CompositeTestResults(CompositeTestResults parent) {
    this.parent = parent;
  }

  public CompositeTestResults getParent() {
    return parent;
  }

  public abstract String getBaseUrl();

  public String getUrlTo(CompositeTestResults model) {
    String otherUrl = model.getBaseUrl();
    String thisUrl = getBaseUrl();

    int maxPos = Math.min(thisUrl.length(), otherUrl.length());
    int endPrefix = 0;
    while (endPrefix < maxPos) {
      int endA = thisUrl.indexOf('/', endPrefix);
      int endB = otherUrl.indexOf('/', endPrefix);
      if (endA != endB || endA < 0) {
        break;
      }
      if (!thisUrl.regionMatches(endPrefix, otherUrl, endPrefix, endA - endPrefix)) {
        break;
      }
      endPrefix = endA + 1;
    }

    StringBuilder result = new StringBuilder();
    int endA = endPrefix;
    while (endA < thisUrl.length()) {
      int pos = thisUrl.indexOf('/', endA);
      if (pos < 0) {
        break;
      }
      result.append("../");
      endA = pos + 1;
    }
    result.append(otherUrl.substring(endPrefix));

    return result.toString();
  }

  public int getTestCount() {
    return tests;
  }

  public int getFailureCount() {
    return failures.size();
  }

  public int getIgnoredCount() {
    return ignored.size();
  }

  public int getRunTestCount() {
    return tests - getIgnoredCount();
  }

  @Override
  public long getDuration() {
    return duration;
  }

  @Override
  public String getFormattedDuration() {
    return getTestCount() == 0 ? "-" : super.getFormattedDuration();
  }

  public Set<TestResult> getFailures() {
    return failures;
  }

  public Set<TestResult> getIgnored() {
    return ignored;
  }

  public ResultType getCoverageResultType() {
    if (coverageResult.isPresent()) {
      if (coverageResult.get().isFailure()) {
        return ResultType.FAILURE;
      } else {
        return ResultType.SUCCESS;
      }
    } else {
      return ResultType.SKIPPED;
    }
  }

  @Override
  public ResultType getResultType() {
    if (!failures.isEmpty()) {
      return ResultType.FAILURE;
    }
    if (getIgnoredCount() > 0) {
      return ResultType.SKIPPED;
    }
    return ResultType.SUCCESS;
  }

  public String getFormattedExpectedCoverageRate() {
    Number coverageRate = getExpectedCoverageRate();
    if (coverageRate == null) return "-";
    return coverageRate + "%";
  }

  public Number getExpectedCoverageRate() {
    if (coverageResult.isPresent()) {
      return coverageResult.get().getExpectedPct();
    } else {
      return null;
    }
  }

  public String getCoverageLink() {
    if (coverageResult.isPresent()) {
      return coverageResult.get().getReportLink().get().toAbsolutePath().toString();
    } else {
      return "#";
    }
  }

  public String getFormattedActualCoverageRate() {
    Number coverageRate = getActualCoverageRate();
    if (coverageRate == null) return "-";
    return coverageRate + "%";
  }

  public Number getActualCoverageRate() {
    if (coverageResult.isPresent()) {
      return coverageResult.get().getActualPct();
    } else {
      return null;
    }
  }

  public String getCoverageStatusClass() {
    switch (getCoverageResultType()) {
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

  public String getFormattedSuccessRate() {
    Number successRate = getSuccessRate();
    if (successRate == null) {
      return "-";
    }
    return successRate + "%";
  }

  public Number getSuccessRate() {
    if (getRunTestCount() == 0) {
      return null;
    }

    BigDecimal runTests = BigDecimal.valueOf(getRunTestCount());
    BigDecimal successful = BigDecimal.valueOf(getRunTestCount() - getFailureCount());

    return successful
        .divide(runTests, 2, BigDecimal.ROUND_DOWN)
        .multiply(BigDecimal.valueOf(100))
        .intValue();
  }

  protected void failed(TestResult failedTest) {
    failures.add(failedTest);
    if (parent != null) {
      parent.failed(failedTest);
    }
  }

  protected void ignored(TestResult ignoredTest) {
    ignored.add(ignoredTest);
    if (parent != null) {
      parent.ignored(ignoredTest);
    }
  }

  protected TestResult addTest(TestResult test) {
    tests++;
    duration += test.getDuration();
    return test;
  }

  protected Optional<CoverageResult> addCoverage(Optional<CoverageResult> coverage) {
    coverageResult = coverage;
    return coverage;
  }
}
