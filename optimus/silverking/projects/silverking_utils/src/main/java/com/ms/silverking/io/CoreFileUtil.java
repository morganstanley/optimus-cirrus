/*
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
package com.ms.silverking.io;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class CoreFileUtil {
  static byte[] readFileAsBytes(File file) throws IOException {
    try (FileInputStream fIn = new FileInputStream(file)) {
      byte[] buf;

      buf = new byte[(int) file.length()];
      CoreStreamUtil.readBytes(buf, 0, buf.length, new BufferedInputStream(fIn));
      return buf;
    }
  }

  public static String readFileAsString(File file) throws IOException {
    return new String(readFileAsBytes(file));
  }

  public static void writeToFile(File file, String text) throws IOException {
    writeToFile(file, text, false);
  }

  public static void writeToFile(File file, String text, boolean append) throws IOException {
    try (BufferedWriter out =
        new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, append)))) {
      out.write(text);
    }
  }
}
