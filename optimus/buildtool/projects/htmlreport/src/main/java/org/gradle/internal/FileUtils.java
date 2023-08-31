/*
 * Copyright 2009 the original author or authors. (see https://github.com/gradle/gradle which also uses Apache 2.0)
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

public class FileUtils {

  /**
   * Converts a string into a string that is safe to use as a file name. The result will only
   * include ascii characters and numbers, and the "-","_", #, $ and "." characters.
   */
  public static String toSafeFileName(String name) {
    int size = name.length();
    StringBuffer rc = new StringBuffer(size * 2);
    for (int i = 0; i < size; i++) {
      char c = name.charAt(i);
      boolean valid = c >= 'a' && c <= 'z';
      valid = valid || (c >= 'A' && c <= 'Z');
      valid = valid || (c >= '0' && c <= '9');
      valid = valid || (c == '_') || (c == '-') || (c == '.') || (c == '$');
      if (valid) {
        rc.append(c);
      } else {
        // Encode the character using hex notation
        rc.append('#');
        rc.append(Integer.toHexString(c));
      }
    }
    return rc.toString();
  }
}
