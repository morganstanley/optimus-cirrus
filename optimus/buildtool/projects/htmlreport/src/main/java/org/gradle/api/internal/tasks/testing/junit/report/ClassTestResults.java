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
import org.gradle.internal.FileUtils;

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

/** Test results for a given class. */
public class ClassTestResults extends CompositeTestResults {
  private final long id;
  private final String name;
  private final PackageTestResults packageResults;
  private final Set<TestResult> results = new TreeSet<TestResult>();
  private final String baseUrl;

  public ClassTestResults(long id, String name, PackageTestResults packageResults) {
    super(packageResults);
    this.id = id;
    this.name = name;
    this.packageResults = packageResults;
    baseUrl = "classes/" + FileUtils.toSafeFileName(name) + ".html";
  }

  public long getId() {
    return id;
  }

  @Override
  public String getTitle() {
    return "Class " + name;
  }

  @Override
  public String getBaseUrl() {
    return baseUrl;
  }

  public String getName() {
    return name;
  }

  public String getSimpleName() {
    String simpleName = StringUtils.substringAfterLast(name, ".");
    if (simpleName.equals("")) {
      return name;
    }
    return simpleName;
  }

  public PackageTestResults getPackageResults() {
    return packageResults;
  }

  public Collection<TestResult> getTestResults() {
    return results;
  }

  public TestResult addTest(String testName, long duration) {
    TestResult test = new TestResult(testName, duration, this);
    results.add(test);
    return addTest(test);
  }
}
