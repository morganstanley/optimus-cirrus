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
package com.ms.silverking.net;

public class AddrAndPortUtil {
  public static String toString(AddrAndPort[] aps) {
    StringBuilder sb;
    boolean first;

    sb = new StringBuilder();
    first = true;
    for (AddrAndPort ap : aps) {
      if (!first) {
        sb.append(AddrAndPort.multipleDefDelimiter);
      } else {
        first = false;
      }
      sb.append(ap.toString());
    }
    return sb.toString();
  }

  public static int hashCode(AddrAndPort[] ensemble) {
    int hashCode;

    hashCode = 0;
    for (AddrAndPort member : ensemble) {
      hashCode = hashCode ^ member.hashCode();
    }
    return hashCode;
  }
}
