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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.gradle.api.internal.tasks.testing.junit.result;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.commons.io.output.WriterOutputStream;
import org.gradle.api.internal.tasks.testing.junit.report.AllTestResults;
import org.gradle.api.internal.tasks.testing.junit.report.ClassTestResults;
import org.gradle.api.internal.tasks.testing.junit.report.CompositeTestResults;
import org.gradle.api.internal.tasks.testing.junit.report.PackageTestResults;
import org.gradle.api.internal.tasks.testing.junit.report.TestResult;
import org.gradle.api.tasks.testing.TestResult.ResultType;
import org.gradle.api.tasks.testing.TestOutputEvent;
import org.gradle.internal.UncheckedException;
import org.gradle.internal.xml.SimpleXmlWriter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.time.format.DateTimeFormatter.ISO_INSTANT;

public class JUnitXmlResultWriter {

  private final TestResultsProvider testResultsProvider;
  private final JUnitXmlResultOptions options;

  // ms
  public JUnitXmlResultWriter(
      TestResultsProvider testResultsProvider, JUnitXmlResultOptions options) {
    this.testResultsProvider = testResultsProvider;
    this.options = options;
  }

  // ms
  public void writeToFile(AllTestResults allResults, File output) throws IOException {
    if (!output.exists()) {
      throw new IOException(
          "Given output directory - "
              + output
              + " does not exist. JUnit XML report cannot be generated");
    }
    File xmlFile = new File(output, "test-report.xml");
    try (OutputStream outputStream = new FileOutputStream(xmlFile)) {
      writeResults(allResults, outputStream);
    }
  }

  // ms
  public String writeToString(AllTestResults allResults) throws IOException {
    try (StringWriter stringWriter = new StringWriter()) {
      writeResults(allResults, new WriterOutputStream(stringWriter));
      return stringWriter.toString();
    }
  }

  // ms
  private void writeResults(AllTestResults allResults, OutputStream outputStream)
      throws IOException {
    SimpleXmlWriter xmlWriter = new SimpleXmlWriter(outputStream, "  ");
    // Calculate overall statistics
    String name = allResults.getTitle();
    int totalTests = allResults.getTestCount();
    int totalFailures = allResults.getFailureCount();
    int totalIgnored = allResults.getIgnoredCount();
    double totalDuration = allResults.getDuration() / 1000.0;
    double successRate = calculateSuccessRate(allResults);

    xmlWriter
        .startElement("testsuites")
        .attribute("id", name)
        .attribute("name", name)
        .attribute("tests", String.valueOf(totalTests))
        .attribute("failures", String.valueOf(totalFailures))
        .attribute("ignored", String.valueOf(totalIgnored))
        .attribute("duration", String.valueOf(totalDuration))
        .attribute("successRate", successRate + "%");

    for (PackageTestResults packageResult : allResults.getPackages()) {
      xmlWriter
          .startElement("testsuite")
          .attribute("id", "package")
          .attribute("name", packageResult.getName())
          .attribute("tests", String.valueOf(packageResult.getTestCount()))
          .attribute("failures", String.valueOf(packageResult.getFailureCount()))
          .attribute("ignored", String.valueOf(packageResult.getIgnoredCount()))
          .attribute("duration", String.valueOf(packageResult.getDuration() / 1000.0))
          .attribute("successRate", calculateSuccessRate(packageResult) + "%");

      for (ClassTestResults classResult : packageResult.getClasses()) {
        xmlWriter
            .startElement("class")
            .attribute("id", "class")
            .attribute("name", classResult.getName())
            .attribute("tests", String.valueOf(classResult.getTestCount()))
            .attribute("failures", String.valueOf(classResult.getFailureCount()))
            .attribute("ignored", String.valueOf(classResult.getIgnoredCount()))
            .attribute("duration", String.valueOf(classResult.getDuration() / 1000.0))
            .attribute("successRate", calculateSuccessRate(classResult) + "%");

        for (TestResult methodResult : classResult.getTestResults()) {
          xmlWriter
              .startElement("testcase")
              .attribute("name", methodResult.getName())
              .attribute("duration", String.valueOf(methodResult.getDuration() / 1000.0))
              .attribute("result", getResultString(methodResult.getResultType()));

          if (methodResult.getResultType() == ResultType.FAILURE) {
            xmlWriter
                .startElement("failure")
                .attribute("message", methodResult.getFailures().get(0).getMessage())
                .characters(methodResult.getFailures().get(0).getStackTrace())
                .endElement();
          }

          xmlWriter.endElement(); // Test
        }
        xmlWriter.endElement(); // Class
      }
      xmlWriter.endElement(); // Package
    }
    xmlWriter.endElement(); // TestResults
    xmlWriter.close();
  }

  // ms
  private double calculateSuccessRate(CompositeTestResults result) {
    int totalTests = result.getTestCount();
    int failedTests = result.getFailureCount();
    int skippedTests = result.getIgnoredCount();
    int successfulTests = totalTests - failedTests - skippedTests;
    return (totalTests == 0) ? 0 : ((double) successfulTests / totalTests) * 100;
  }

  private String getResultString(ResultType resultType) {
    switch (resultType) {
      case SUCCESS:
        return "passed";
      case FAILURE:
        return "failed";
      case SKIPPED:
        return "ignore";
      default:
        throw new IllegalArgumentException("Unknown ResultType: " + resultType);
    }
  }

  private TestCase discreteTestCase(String className, long classId, TestMethodResult methodResult) {
    return new TestCase(
        methodResult.getDisplayName(),
        className,
        methodResult.getDuration(),
        discreteTestCaseExecutions(classId, methodResult));
  }

  private Iterable<? extends TestCaseExecution> discreteTestCaseExecutions(
      final long classId, final TestMethodResult methodResult) {
    switch (methodResult.getResultType()) {
        //            case FAILURE:
        //                return failures(classId, methodResult, FailureType.FAILURE);
      case SKIPPED:
        return Collections.singleton(skipped(classId, methodResult.getId()));
      case SUCCESS:
        return Collections.singleton(success(classId, methodResult.getId()));
      default:
        throw new IllegalStateException("Unexpected result type: " + methodResult.getResultType());
    }
  }

  abstract static class TestCaseExecution {
    private final OutputProvider outputProvider;
    private final JUnitXmlResultOptions options;

    TestCaseExecution(OutputProvider outputProvider, JUnitXmlResultOptions options) {
      this.outputProvider = outputProvider;
      this.options = options;
    }

    abstract void write(SimpleXmlWriter writer) throws IOException;

    protected void writeOutput(SimpleXmlWriter writer) throws IOException {
      if (options.includeSystemOutLog && outputProvider.has(TestOutputEvent.Destination.StdOut)) {
        writer.startElement("system-out");
        writer.startCDATA();
        outputProvider.write(TestOutputEvent.Destination.StdOut, writer);
        writer.endCDATA();
        writer.endElement();
      }

      if (options.includeSystemErrLog && outputProvider.has(TestOutputEvent.Destination.StdErr)) {
        writer.startElement("system-err");
        writer.startCDATA();
        outputProvider.write(TestOutputEvent.Destination.StdErr, writer);
        writer.endCDATA();
        writer.endElement();
      }
    }
  }

  private static class TestCase {
    final String name;
    final String className;
    final long duration;
    final Iterable<? extends TestCaseExecution> executions;

    TestCase(
        String name,
        String className,
        long duration,
        Iterable<? extends TestCaseExecution> executions) {
      this.name = name;
      this.className = className;
      this.duration = duration;
      this.executions = executions;
    }
  }

  private static class TestCaseExecutionSuccess extends TestCaseExecution {
    TestCaseExecutionSuccess(OutputProvider outputProvider, JUnitXmlResultOptions options) {
      super(outputProvider, options);
    }

    @Override
    public void write(SimpleXmlWriter writer) throws IOException {
      writeOutput(writer);
    }
  }

  private static class TestCaseExecutionSkipped extends TestCaseExecution {
    TestCaseExecutionSkipped(OutputProvider outputProvider, JUnitXmlResultOptions options) {
      super(outputProvider, options);
    }

    @Override
    public void write(SimpleXmlWriter writer) throws IOException {
      writer.startElement("skipped").endElement();
      writeOutput(writer);
    }
  }

  private TestCaseExecution success(long classId, long id) {
    return new TestCaseExecutionSuccess(outputProvider(classId, id), options);
  }

  private TestCaseExecution skipped(long classId, long id) {
    return new TestCaseExecutionSkipped(outputProvider(classId, id), options);
  }

  private OutputProvider outputProvider(long classId, long id) {
    return options.outputPerTestCase
        ? new BackedOutputProvider(classId, id)
        : NullOutputProvider.INSTANCE;
  }

  interface OutputProvider {
    boolean has(TestOutputEvent.Destination destination);

    void write(TestOutputEvent.Destination destination, Writer writer);
  }

  class BackedOutputProvider implements OutputProvider {
    private final long classId;
    private final long testId;

    public BackedOutputProvider(long classId, long testId) {
      this.classId = classId;
      this.testId = testId;
    }

    @Override
    public boolean has(TestOutputEvent.Destination destination) {
      return testResultsProvider.hasOutput(classId, testId, destination);
    }

    @Override
    public void write(TestOutputEvent.Destination destination, Writer writer) {
      testResultsProvider.writeTestOutput(classId, testId, destination, writer);
    }
  }

  static class NullOutputProvider implements OutputProvider {
    static final OutputProvider INSTANCE = new NullOutputProvider();

    @Override
    public boolean has(TestOutputEvent.Destination destination) {
      return false;
    }

    @Override
    public void write(TestOutputEvent.Destination destination, Writer writer) {
      throw new UnsupportedOperationException();
    }
  }
}
