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
package optimus;

import java.lang.reflect.Array;
import java.util.Arrays;

public class CoreUtils {
  public static <T> T[] merge(T[] a, T[] b) {
    if (b == null) return a;
    if (a == null) return b;
    var newArr = Arrays.copyOf(a, a.length + b.length);
    System.arraycopy(b, 0, newArr, a.length, b.length);
    return newArr;
  }

  public static <T> T[] merge(T a, T[] b) {
    @SuppressWarnings("unchecked")
    var newArr = (T[]) Array.newInstance(b.getClass().getComponentType(), b.length + 1);
    newArr[0] = a;
    System.arraycopy(b, 0, newArr, 1, b.length);
    return newArr;
  }

  public static <T> T[] dropFirst(T[] a, int count) {
    return Arrays.copyOfRange(a, count, a.length);
  }

  public static String stripSuffix(String str, String suffix) {
    if (str.endsWith(suffix)) return str.substring(0, str.length() - suffix.length());
    else return str;
  }

  public static String stripPrefix(String str, String prefix) {
    if (str.startsWith(prefix)) return str.substring(prefix.length());
    else return str;
  }

  public static String stripFromLast(String str, int suffix) {
    var index = str.lastIndexOf(suffix);
    if (index < 0) return "";
    return str.substring(0, index);
  }
}
