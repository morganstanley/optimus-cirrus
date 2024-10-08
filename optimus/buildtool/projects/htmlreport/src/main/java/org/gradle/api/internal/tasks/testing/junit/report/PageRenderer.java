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

import org.gradle.api.Action;
import org.gradle.api.tasks.testing.TestResult.ResultType;
import org.gradle.internal.ErroringAction;
import org.gradle.internal.html.SimpleHtmlWriter;
import org.gradle.reporting.ReportRenderer;
import org.gradle.reporting.TabbedPageRenderer;
import org.gradle.reporting.TabsRenderer;

import java.io.IOException;
import java.net.URL;

abstract class PageRenderer<T extends CompositeTestResults> extends TabbedPageRenderer<T> {
  private static final URL STYLE_URL = PageRenderer.class.getResource("style.css");

  private T results;
  private final TabsRenderer<T> tabsRenderer = new TabsRenderer<T>();

  protected T getResults() {
    return results;
  }

  protected abstract void renderBreadcrumbs(SimpleHtmlWriter htmlWriter) throws IOException;

  protected abstract void registerTabs();

  @Override
  protected URL getStyleUrl() {
    return STYLE_URL;
  }

  protected void addTab(String title, final Action<SimpleHtmlWriter> contentRenderer) {
    tabsRenderer.add(
        title,
        new ReportRenderer<T, SimpleHtmlWriter>() {
          @Override
          public void render(T model, SimpleHtmlWriter writer) {
            contentRenderer.execute(writer);
          }
        });
  }

  protected void renderTabs(SimpleHtmlWriter htmlWriter) throws IOException {
    tabsRenderer.render(getModel(), htmlWriter);
  }

  protected void addFailuresTab() {
    if (!results.getFailures().isEmpty()) {
      addTab(
          "Failed tests",
          new ErroringAction<SimpleHtmlWriter>() {
            public void doExecute(SimpleHtmlWriter element) throws IOException {
              renderFailures(element);
            }
          });
    }
  }

  protected void renderFailures(SimpleHtmlWriter htmlWriter) throws IOException {
    htmlWriter.startElement("ul").attribute("class", "linkList");
    for (TestResult test : results.getFailures()) {
      htmlWriter.startElement("li");
      htmlWriter
          .startElement("a")
          .attribute("href", asHtmlLinkEncoded(getResults().getUrlTo(test.getClassResults())))
          .characters(test.getClassResults().getSimpleName())
          .endElement();
      htmlWriter.characters(".");
      String link =
          asHtmlLinkEncoded(getResults().getUrlTo(test.getClassResults())) + "#" + test.getName();
      htmlWriter.startElement("a").attribute("href", link).characters(test.getName()).endElement();
      htmlWriter.endElement();
    }
    htmlWriter.endElement();
  }

  protected void addIgnoredTab() {
    if (!results.getIgnored().isEmpty()) {
      addTab(
          "Ignored tests",
          new ErroringAction<SimpleHtmlWriter>() {
            public void doExecute(SimpleHtmlWriter htmlWriter) throws IOException {
              renderIgnoredTests(htmlWriter);
            }
          });
    }
  }

  protected void renderIgnoredTests(SimpleHtmlWriter htmlWriter) throws IOException {
    htmlWriter.startElement("ul").attribute("class", "linkList");
    for (TestResult test : getResults().getIgnored()) {
      htmlWriter.startElement("li");
      htmlWriter
          .startElement("a")
          .attribute("href", asHtmlLinkEncoded(getResults().getUrlTo(test.getClassResults())))
          .characters(test.getClassResults().getSimpleName())
          .endElement();
      htmlWriter.characters(".");
      String link =
          asHtmlLinkEncoded(getResults().getUrlTo(test.getClassResults())) + "#" + test.getName();
      htmlWriter.startElement("a").attribute("href", link).characters(test.getName()).endElement();
      htmlWriter.endElement();
    }
    htmlWriter.endElement();
  }

  @Override
  protected String getTitle() {
    return getModel().getTitle();
  }

  @Override
  protected String getPageTitle() {
    return "Test results - " + getModel().getTitle();
  }

  @Override
  protected ReportRenderer<T, SimpleHtmlWriter> getHeaderRenderer() {
    return new ReportRenderer<T, SimpleHtmlWriter>() {
      @Override
      public void render(T model, SimpleHtmlWriter htmlWriter) throws IOException {
        PageRenderer.this.results = model;
        renderBreadcrumbs(htmlWriter);

        // summary
        htmlWriter.startElement("div").attribute("id", "summary");
        htmlWriter.startElement("table");
        htmlWriter.startElement("tr");
        htmlWriter.startElement("td");
        htmlWriter.startElement("div").attribute("class", "summaryGroup");
        htmlWriter.startElement("table");
        htmlWriter.startElement("tr");
        htmlWriter.startElement("td");
        htmlWriter.startElement("div").attribute("class", "infoBox").attribute("id", "tests");
        htmlWriter
            .startElement("div")
            .attribute("class", "counter")
            .characters(Integer.toString(results.getTestCount()))
            .endElement();
        htmlWriter.startElement("p").characters("tests").endElement();
        htmlWriter.endElement();
        htmlWriter.endElement();
        htmlWriter.startElement("td");
        htmlWriter.startElement("div").attribute("class", "infoBox").attribute("id", "failures");
        htmlWriter
            .startElement("div")
            .attribute("class", "counter")
            .characters(Integer.toString(results.getFailureCount()))
            .endElement();
        htmlWriter.startElement("p").characters("failures").endElement();
        htmlWriter.endElement();
        htmlWriter.endElement();
        htmlWriter.startElement("td");
        htmlWriter.startElement("div").attribute("class", "infoBox").attribute("id", "ignored");
        htmlWriter
            .startElement("div")
            .attribute("class", "counter")
            .characters(Integer.toString(results.getIgnoredCount()))
            .endElement();
        htmlWriter.startElement("p").characters("ignored").endElement();
        htmlWriter.endElement();
        htmlWriter.endElement();
        htmlWriter.startElement("td");
        htmlWriter.startElement("div").attribute("class", "infoBox").attribute("id", "duration");
        htmlWriter
            .startElement("div")
            .attribute("class", "counter")
            .characters(results.getFormattedDuration())
            .endElement();
        htmlWriter.startElement("p").characters("duration").endElement();
        htmlWriter.endElement();
        htmlWriter.endElement();
        htmlWriter.endElement();
        htmlWriter.endElement();
        htmlWriter.endElement();
        htmlWriter.endElement();
        htmlWriter.startElement("td");
        htmlWriter
            .startElement("div")
            .attribute("class", "infoBox " + results.getStatusClass() + " successRate")
            .attribute("id", "successRate");
        htmlWriter
            .startElement("div")
            .attribute("class", "percent")
            .characters(results.getFormattedSuccessRate())
            .endElement();
        htmlWriter.startElement("p").characters("successful tests").endElement();
        htmlWriter.endElement();
        htmlWriter.endElement();
        if (!results.getCoverageResultType().equals(ResultType.SKIPPED)) {
          htmlWriter.startElement("td");
          htmlWriter
              .startElement("div")
              .attribute("class", "infoBox " + results.getCoverageStatusClass() + " successRate")
              .attribute("id", "coverageRate");
          htmlWriter
              .startElement("div")
              .attribute("class", "percent")
              .characters(results.getFormattedActualCoverageRate())
              .endElement();
          htmlWriter.startElement("p").characters("actual coverage").endElement();
          htmlWriter
              .startElement("a")
              .attribute("href", results.getCoverageLink())
              .attribute("target", "_blank")
              .characters("expected: " + results.getFormattedExpectedCoverageRate())
              .endElement();
          htmlWriter.endElement();
          htmlWriter.endElement();
        }
        htmlWriter.endElement();
        htmlWriter.endElement();
        htmlWriter.endElement();
      }
    };
  }

  @Override
  protected ReportRenderer<T, SimpleHtmlWriter> getContentRenderer() {
    return new ReportRenderer<T, SimpleHtmlWriter>() {
      @Override
      public void render(T model, SimpleHtmlWriter htmlWriter) throws IOException {
        PageRenderer.this.results = model;
        tabsRenderer.clear();
        registerTabs();
        renderTabs(htmlWriter);
      }
    };
  }

  protected String asHtmlLinkEncoded(String rawLink) {
    return rawLink.replace("#", "%23");
  }
}
