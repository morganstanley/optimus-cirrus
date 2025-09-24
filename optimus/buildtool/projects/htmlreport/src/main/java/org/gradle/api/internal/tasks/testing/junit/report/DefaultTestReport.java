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

import static org.gradle.api.tasks.testing.TestResult.ResultType.SKIPPED;

import java.io.File;
import java.util.List;

import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.internal.tasks.testing.junit.result.JUnitXmlResultOptions;
import org.gradle.api.internal.tasks.testing.junit.result.JUnitXmlResultWriter;
import org.gradle.api.internal.tasks.testing.junit.result.TestClassResult;
import org.gradle.api.internal.tasks.testing.junit.result.TestFailure;
import org.gradle.api.internal.tasks.testing.junit.result.TestMethodResult;
import org.gradle.api.internal.tasks.testing.junit.result.TestResultsProvider;
import org.gradle.reporting.HtmlReportBuilder;
import org.gradle.reporting.HtmlReportRenderer;
import org.gradle.reporting.ReportRenderer;

public class DefaultTestReport {

  public void generateReport(TestResultsProvider resultsProvider, File reportDir) {
    AllTestResults model = loadModelFromProvider(resultsProvider);
    generateFiles(model, resultsProvider, reportDir);
  }

  public void generateXmlReport(TestResultsProvider resultsProvider, File reportDir) {
    AllTestResults model = loadModelFromProvider(resultsProvider);
    generateXmlFiles(model, resultsProvider, reportDir);
  }

  private void generateXmlFiles(
      AllTestResults model, TestResultsProvider resultsProvider, File reportDir) {
    try {
      JUnitXmlResultOptions options = new JUnitXmlResultOptions(true, false, false, false);
      JUnitXmlResultWriter xmlWriter = new JUnitXmlResultWriter(resultsProvider, options);
      xmlWriter.writeToFile(model, reportDir);
    } catch (Exception e) {
      throw new GradleException(
          String.format("Could not generate XML test report to '%s'.", reportDir), e);
    }
  }

  public void generateOverview(TestResultsProvider resultsProvider, File reportDir) {
    AllTestResults model = loadModelFromProvider(resultsProvider);
    generateOverviewOnly(model, resultsProvider, reportDir);
  }

  private AllTestResults loadModelFromProvider(TestResultsProvider resultsProvider) {
    final AllTestResults model = new AllTestResults(reportFile());
    resultsProvider.visitClasses(
        new Action<TestClassResult>() {
          public void execute(TestClassResult classResult) {
            model.addTestClass(classResult.getId(), classResult.getClassName());
            List<TestMethodResult> collectedResults = classResult.getResults();
            for (TestMethodResult collectedResult : collectedResults) {
              final TestResult testResult =
                  model.addTest(
                      classResult.getId(),
                      classResult.getClassName(),
                      collectedResult.getName(),
                      collectedResult.getDuration());
              if (collectedResult.getResultType() == SKIPPED) {
                testResult.setIgnored();
              } else {
                List<TestFailure> failures = collectedResult.getFailures();
                for (TestFailure failure : failures) {
                  testResult.addFailure(failure);
                }
              }
            }
          }
        });
    resultsProvider.getCoverageResultsByScope().forEach(model::addCoverageResult);
    return model;
  }

  protected String reportFile() {
    return "index.html"; // ms
  }

  private void generateFiles(
      AllTestResults model, final TestResultsProvider resultsProvider, File reportDir) {
    try {
      HtmlReportRenderer htmlRenderer = new HtmlReportRenderer();
      htmlRenderer.render(
          model,
          new ReportRenderer<AllTestResults, HtmlReportBuilder>() {
            @Override
            public void render(final AllTestResults model, final HtmlReportBuilder output) {
              generator(reportFile(), model, new OverviewPageRenderer(), output).run();
              for (PackageTestResults packageResults : model.getPackages()) {
                generator(
                        packageResults.getBaseUrl(),
                        packageResults,
                        new PackagePageRenderer(),
                        output)
                    .run();
                for (ClassTestResults classResults : packageResults.getClasses()) {
                  generator(
                          classResults.getBaseUrl(),
                          classResults,
                          new ClassPageRenderer(resultsProvider),
                          output)
                      .run();
                }
              }
            }
          },
          reportDir);
    } catch (Exception e) {
      throw new GradleException(
          String.format("Could not generate test report to '%s'.", reportDir), e);
    }
  }

  private void generateOverviewOnly(
      AllTestResults model, final TestResultsProvider resultsProvider, File reportDir) {
    try {
      HtmlReportRenderer htmlRenderer = new HtmlReportRenderer();
      htmlRenderer.render(
          model,
          new ReportRenderer<AllTestResults, HtmlReportBuilder>() {
            @Override
            public void render(final AllTestResults model, final HtmlReportBuilder output) {
              generator(reportFile(), model, new OverviewPageRenderer(), output).run();
            }
          },
          reportDir);
    } catch (Exception e) {
      throw new GradleException(
          String.format("Could not generate test report to '%s'.", reportDir), e);
    }
  }

  public static <T extends CompositeTestResults> HtmlReportFileGenerator<T> generator(
      String fileUrl, T results, PageRenderer<T> renderer, HtmlReportBuilder output) {
    return new HtmlReportFileGenerator<T>(fileUrl, results, renderer, output);
  }

  private static class HtmlReportFileGenerator<T extends CompositeTestResults> implements Runnable {
    private final String fileUrl;
    private final T results;
    private final PageRenderer<T> renderer;
    private final HtmlReportBuilder output;

    HtmlReportFileGenerator(
        String fileUrl, T results, PageRenderer<T> renderer, HtmlReportBuilder output) {
      this.fileUrl = fileUrl;
      this.results = results;
      this.renderer = renderer;
      this.output = output;
    }

    @Override
    public void run() {
      output.renderHtmlPage(fileUrl, results, renderer);
    }
  }
}
