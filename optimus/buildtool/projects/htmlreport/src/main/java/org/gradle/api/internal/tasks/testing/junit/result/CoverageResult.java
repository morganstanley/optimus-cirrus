/*
 * Copyright 2013 the original author or authors. (see https://github.com/gradle/gradle which also uses Apache 2.0)
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

import java.nio.file.Path;
import java.util.Optional;

public class CoverageResult {
  private final double expectedPct;
  private final double actualPct;
  private final String message;
  private final Optional<Path> reportLink;

  private final boolean isFailure;

  public CoverageResult(
      double expectedPct,
      double actualPct,
      boolean isFailure,
      String message,
      Optional<Path> reportLink) {
    this.expectedPct = expectedPct;
    this.actualPct = actualPct;
    this.isFailure = isFailure;
    this.message = message;
    this.reportLink = reportLink;
  }

  public double getExpectedPct() {
    return expectedPct;
  }

  public double getActualPct() {
    return actualPct;
  }

  public String getMessage() {
    return message;
  }

  public Optional<Path> getReportLink() {
    return reportLink;
  }

  public boolean isFailure() {
    return isFailure;
  }
}
