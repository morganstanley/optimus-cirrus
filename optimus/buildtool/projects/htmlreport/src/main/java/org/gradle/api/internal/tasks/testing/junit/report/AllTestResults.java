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

import org.apache.commons.lang3.StringUtils;
import org.gradle.api.internal.tasks.testing.junit.result.CoverageResult;

import java.util.*;

/** The model for the test report. */
public class AllTestResults extends CompositeTestResults {
  private final Map<String, PackageTestResults> packages =
      new TreeMap<String, PackageTestResults>();
  private final String baseUrl; // ms

  public AllTestResults(String baseUrl) {
    super(null);
    this.baseUrl = baseUrl;
  }

  @Override
  public String getTitle() {
    return "Test Summary";
  }

  @Override
  public String getBaseUrl() {
    return baseUrl;
  }

  public Collection<PackageTestResults> getPackages() {
    return packages.values();
  }

  public TestResult addTest(long classId, String className, String testName, long duration) {
    PackageTestResults packageResults = addPackageForClass(className);
    return addTest(packageResults.addTest(classId, className, testName, duration));
  }

  public void addCoverageResult(String scope, CoverageResult coverage) {
    addCoverageByScope(scope, coverage);
  }

  public ClassTestResults addTestClass(long classId, String className) {
    return addPackageForClass(className).addClass(classId, className);
  }

  private PackageTestResults addPackageForClass(String className) {
    String packageName = StringUtils.substringBeforeLast(className, ".");
    if (packageName.equals(className)) {
      packageName = "";
    }
    return addPackage(packageName);
  }

  private PackageTestResults addPackage(String packageName) {
    PackageTestResults packageResults = packages.get(packageName);
    if (packageResults == null) {
      packageResults = new PackageTestResults(packageName, this);
      packages.put(packageName, packageResults);
    }
    return packageResults;
  }
}
