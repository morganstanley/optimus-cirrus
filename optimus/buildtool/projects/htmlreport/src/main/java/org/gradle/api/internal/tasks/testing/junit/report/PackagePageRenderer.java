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

import org.gradle.internal.ErroringAction;
import org.gradle.internal.html.SimpleHtmlWriter;

import java.io.IOException;

class PackagePageRenderer extends PageRenderer<PackageTestResults> {

  @Override
  protected void renderBreadcrumbs(SimpleHtmlWriter htmlWriter) throws IOException {
    htmlWriter.startElement("div").attribute("class", "breadcrumbs");
    htmlWriter
        .startElement("a")
        .attribute("href", getResults().getUrlTo(getResults().getParent()))
        .characters("all")
        .endElement();
    htmlWriter.characters(" > " + getResults().getName());
    htmlWriter.endElement();
  }

  private void renderClasses(SimpleHtmlWriter htmlWriter) throws IOException {
    htmlWriter.startElement("table");
    htmlWriter.startElement("thread");
    htmlWriter.startElement("tr");

    htmlWriter.startElement("th").characters("Class").endElement();
    htmlWriter.startElement("th").characters("Tests").endElement();
    htmlWriter.startElement("th").characters("Failures").endElement();
    htmlWriter.startElement("th").characters("Ignored").endElement();
    htmlWriter.startElement("th").characters("Duration").endElement();
    htmlWriter.startElement("th").characters("Success rate").endElement();

    htmlWriter.endElement();
    htmlWriter.endElement();

    for (ClassTestResults testClass : getResults().getClasses()) {
      htmlWriter.startElement("tr");
      htmlWriter.startElement("td").attribute("class", testClass.getStatusClass());
      htmlWriter
          .startElement("a")
          .attribute("href", asHtmlLinkEncoded(getResults().getUrlTo(testClass)))
          .characters(testClass.getSimpleName())
          .endElement();
      htmlWriter.endElement();
      htmlWriter
          .startElement("td")
          .characters(Integer.toString(testClass.getTestCount()))
          .endElement();
      htmlWriter
          .startElement("td")
          .characters(Integer.toString(testClass.getFailureCount()))
          .endElement();
      htmlWriter
          .startElement("td")
          .characters(Integer.toString(testClass.getIgnoredCount()))
          .endElement();
      htmlWriter.startElement("td").characters(testClass.getFormattedDuration()).endElement();
      htmlWriter
          .startElement("td")
          .attribute("class", testClass.getStatusClass())
          .characters(testClass.getFormattedSuccessRate())
          .endElement();
      htmlWriter.endElement();
    }
    htmlWriter.endElement();
  }

  @Override
  protected void registerTabs() {
    addFailuresTab();
    addIgnoredTab();
    addTab(
        "Classes",
        new ErroringAction<SimpleHtmlWriter>() {
          public void doExecute(SimpleHtmlWriter htmlWriter) throws IOException {
            renderClasses(htmlWriter);
          }
        });
  }
}
