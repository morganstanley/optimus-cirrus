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

package org.gradle.internal.xml;

import java.io.*;

/** A streaming XML writer. */
public class SimpleXmlWriter extends SimpleMarkupWriter {
  /**
   * Constructs a writer with the given output.
   *
   * @param output The output, should be unbuffered, as this class performs buffering
   */
  public SimpleXmlWriter(OutputStream output) throws IOException {
    this(output, null);
  }

  /**
   * Constructs a writer with the given output.
   *
   * @param output The output, should be unbuffered, as this class performs buffering
   */
  public SimpleXmlWriter(OutputStream output, String indent) throws IOException {
    this(new BufferedWriter(new OutputStreamWriter(output, "UTF-8")), indent, "UTF-8");
  }

  /**
   * Constructs a writer with the given output.
   *
   * @param writer The output, should be buffered.
   */
  public SimpleXmlWriter(Writer writer, String indent, String encoding) throws IOException {
    super(writer, indent);
    writeXmlDeclaration(encoding);
  }

  private void writeXmlDeclaration(String encoding) throws IOException {
    writeRaw("<?xml version=\"1.0\" encoding=\"");
    writeRaw(encoding);
    writeRaw("\"?>");
  }
}
