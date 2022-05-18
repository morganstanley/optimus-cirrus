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

import java.io.IOException;
import java.io.InputStream;

public class CoreStreamUtil {
  public static void readBytes(byte[] b, int offset, int length, InputStream in) throws IOException {
    int totalRead;
    int numRead;

    numRead = 0;
    totalRead = 0;
    while (numRead >= 0 && totalRead < length) {
      numRead = in.read(b, offset + totalRead, length - totalRead);
      if (numRead > 0) {
        totalRead += numRead;
      }
    }
  }
}