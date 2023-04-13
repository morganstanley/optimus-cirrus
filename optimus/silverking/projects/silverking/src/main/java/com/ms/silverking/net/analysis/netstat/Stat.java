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
package com.ms.silverking.net.analysis.netstat;

public class Stat {
  private final String fullName;
  private final String name;
  private final long value;

  public Stat(String fullName, String name, long value) {
    this.fullName = fullName;
    this.name = name;
    this.value = value;
    if (value < 0) {
      throw new RuntimeException(String.format("Value < 0\t%s %s %d", fullName, name, value));
    }
  }

  public String getFullName() {
    return fullName;
  }

  public String getName() {
    return name;
  }

  public long getValue() {
    return value;
  }

  public Stat add(Stat s) {
    long total;

    if (!s.fullName.equals(this.fullName)) {
      throw new RuntimeException(s.fullName + " != " + this.fullName);
    }
    total = this.value + s.value;
    if (total < this.value) {
      throw new RuntimeException("Long overflow");
    }
    return new Stat(fullName, name, total);
  }

  @Override
  public String toString() {
    return fullName + ":" + name + ":" + value;
  }

  public static Stat parse(String parentName, String s) {
    int splitIndex;
    int nameIndex;
    int valueIndex;

    s = s.trim();
    splitIndex = s.indexOf(':');
    if (splitIndex < 0) {
      splitIndex = s.indexOf(' ');
      if (splitIndex < 0) {
        return null;
      } else {
        nameIndex = 1;
        valueIndex = 0;
      }
    } else {
      nameIndex = 0;
      valueIndex = 1;
    }
    if (s.length() == splitIndex) {
      return null;
    } else {
      String[] subs;

      subs = new String[2];
      subs[0] = s.substring(0, splitIndex).trim();
      subs[1] = s.substring(splitIndex + 1).trim();
      return new Stat(parentName != null ? parentName + ":" + subs[nameIndex] : "", subs[nameIndex],
          Long.parseLong(subs[valueIndex]));
    }
  }

  public static void main(String[] args) {
    try {
      System.out.println(parse(null, "    59815 active connections openingss"));
      System.out.println(parse(null, "        InType0: 26144"));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
