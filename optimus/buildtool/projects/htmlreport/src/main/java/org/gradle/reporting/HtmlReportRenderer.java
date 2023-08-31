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
package org.gradle.reporting;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.net.URL;
import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.gradle.api.UncheckedIOException;
import org.gradle.internal.ErroringAction;
import org.gradle.internal.IoActions;
import org.gradle.internal.html.SimpleHtmlWriter;
import org.gradle.util.GFileUtils;

public class HtmlReportRenderer {
  /** Renders a multi-page HTML report from the given model, into the given directory. */
  public <T> void render(
      T model, ReportRenderer<T, HtmlReportBuilder> renderer, File outputDirectory) {
    try {
      outputDirectory.mkdirs();
      DefaultHtmlReportContext context = new DefaultHtmlReportContext(outputDirectory);
      renderer.render(model, context);
      for (Resource resource : context.resources.values()) {
        File destFile = new File(outputDirectory, resource.path);
        if (!destFile.exists()) {
          GFileUtils.copyURLToFile(resource.source, destFile);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static class Resource {
    final URL source;
    final String path;

    private Resource(URL source, String path) {
      this.source = source;
      this.path = path;
    }
  }

  private static class DefaultHtmlReportContext implements HtmlReportBuilder {
    private final File outputDirectory;
    private final Map<String, Resource> resources = new HashMap<String, Resource>();

    public DefaultHtmlReportContext(File outputDirectory) {
      this.outputDirectory = outputDirectory;
    }

    Resource addResource(URL source) {
      String urlString = source.toString();
      Resource resource = resources.get(urlString);
      if (resource == null) {
        String name = StringUtils.substringAfterLast(source.getPath(), "/");
        String type = StringUtils.substringAfterLast(source.getPath(), ".");
        if (type.equalsIgnoreCase("png") || type.equalsIgnoreCase("gif")) {
          type = "images";
        }
        String path = type + "/" + name;
        resource = new Resource(source, path);
        resources.put(urlString, resource);
      }
      return resource;
    }

    public <T> void renderHtmlPage(
        final String name,
        final T model,
        final ReportRenderer<T, HtmlPageBuilder<SimpleHtmlWriter>> renderer) {
      File outputFile = new File(outputDirectory, name);
      IoActions.writeTextFile(
          outputFile,
          "utf-8",
          new ErroringAction<Writer>() {
            @Override
            protected void doExecute(Writer writer) throws Exception {
              SimpleHtmlWriter htmlWriter = new SimpleHtmlWriter(writer, "");
              htmlWriter.startElement("html");
              renderer.render(
                  model, new DefaultHtmlPageBuilder<SimpleHtmlWriter>(prefix(name), htmlWriter));
              htmlWriter.endElement();
            }
          });
    }

    private String prefix(String name) {
      StringBuilder builder = new StringBuilder();
      int pos = 0;
      while (pos < name.length()) {
        int next = name.indexOf('/', pos);
        if (next < 0) {
          break;
        }
        builder.append("../");
        pos = next + 1;
      }
      return builder.toString();
    }

    private class DefaultHtmlPageBuilder<D> implements HtmlPageBuilder<D> {
      private final String prefix;
      private final D output;

      public DefaultHtmlPageBuilder(String prefix, D output) {
        this.prefix = prefix;
        this.output = output;
      }

      public String requireResource(URL source) {
        Resource resource = addResource(source);
        return prefix + resource.path;
      }

      public String formatDate(Date date) {
        return DateFormat.getDateTimeInstance().format(date);
      }

      public D getOutput() {
        return output;
      }
    }
  }
}
