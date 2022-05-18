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
package com.ms.silverking.collection;

public class EnumUtil {
  public static <T extends Enum<T>> T valueOfIgnoreCase(Class<T> e, String v) {
    for (T en : e.getEnumConstants()) {
      if (en.name().equalsIgnoreCase(v)) {
        return en;
      }
    }
    return null;
  }

  public static <T extends Enum<T>> T getEnumFromStringStart(Class<T> e, String v) {
    for (T en : e.getEnumConstants()) {
      if (v.startsWith(en.name())) {
        return en;
      }
    }
    return null;
  }
}
