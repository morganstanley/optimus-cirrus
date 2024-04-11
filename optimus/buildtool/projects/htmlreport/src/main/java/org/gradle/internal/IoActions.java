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
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gradle.internal;

import org.gradle.api.Action;
import org.gradle.api.UncheckedIOException;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

/** Various utilities for dealing with IO actions. */
public abstract class IoActions {

  /**
   * Gives a writer for the given file/encoding to the given write action, managing the streams.
   *
   * @param output The file to write to
   * @param encoding The character encoding to write with
   * @param action The action to write the actual content
   */
  public static void writeTextFile(
      File output, String encoding, Action<? super BufferedWriter> action) {
    createTextFileWriteAction(output, encoding).execute(action);
  }

  /**
   * Creates an action that itself takes an action that will perform that actual writing to the
   * file.
   *
   * <p>All IO is deferred until the execution of the returned action.
   *
   * @param output The file to write to
   * @param encoding The character encoding to write with
   * @return An action that receives an action that performs the actual writing
   */
  public static Action<Action<? super BufferedWriter>> createTextFileWriteAction(
      File output, String encoding) {
    return new TextFileWriterIoAction(output, encoding);
  }

  private static class TextFileWriterIoAction implements Action<Action<? super BufferedWriter>> {
    private final File file;
    private final String encoding;

    private TextFileWriterIoAction(File file, String encoding) {
      this.file = file;
      this.encoding = encoding;
    }

    public void execute(Action<? super BufferedWriter> action) {
      try {
        File parentFile = file.getParentFile();
        if (parentFile != null) {
          if (!parentFile.mkdirs() && !parentFile.isDirectory()) {
            throw new IOException(String.format("Unable to create directory '%s'", parentFile));
          }
        }
        BufferedWriter writer =
            new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), encoding));
        try {
          action.execute(writer);
        } finally {
          writer.close();
        }
      } catch (Exception e) {
        throw new UncheckedIOException(String.format("Could not write to file '%s'.", file), e);
      }
    }
  }
}
