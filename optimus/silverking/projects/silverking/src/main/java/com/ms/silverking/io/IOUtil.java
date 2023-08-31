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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;

public class IOUtil {
  public static <T> void writeAsLines(File outFile, Collection<T> c) throws IOException {
    writeAsLines(new FileOutputStream(outFile), c, true);
  }

  public static <T> void writeAsLines(OutputStream out, Collection<T> c, boolean close)
      throws IOException {
    try {
      for (T element : c) {
        out.write(element.toString().getBytes());
        out.write('\n');
      }
    } finally {
      if (close) {
        out.close();
      }
    }
  }
}
