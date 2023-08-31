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
package com.ms.silverking.net.analysis.iperf;

public class Measurement {
  private final int id;
  private final TimeInterval timeInterval;
  private final long bytes;
  private final long bitsPerSecond;

  public Measurement(int id, TimeInterval timeInterval, long bytes, long bitsPerSecond) {
    this.id = id;
    this.timeInterval = timeInterval;
    this.bytes = bytes;
    this.bitsPerSecond = bitsPerSecond;
  }

  public int getId() {
    return id;
  }

  public TimeInterval getTimeInterval() {
    return timeInterval;
  }

  public long getBytes() {
    return bytes;
  }

  public long getBitsPerSecond() {
    return bitsPerSecond;
  }

  private static String formatBytes(long bytes) {
    return "" + bytes;
  }

  private static String formatBitsPerSecond(long bitsPerSecond) {
    return "" + bitsPerSecond;
  }

  @Override
  public String toString() {
    return id
        + "\t"
        + timeInterval
        + "\t"
        + formatBytes(bytes)
        + "\t"
        + formatBitsPerSecond(bitsPerSecond);
  }

  public String toIDBandwidthString() {
    return id + "\t" + formatBitsPerSecond(bitsPerSecond);
  }

  public static Measurement parse(String s) {
    String[] defs;

    defs = splitDef(s);
    return new Measurement(
        parseID(defs[0]), TimeInterval.parse(defs[1]), parseValue(defs[2]), parseValue(defs[3]));
  }

  private static String[] splitDef(String s) {
    String[] defs;
    int i;
    int j;

    defs = new String[4];
    defs[0] = s.substring(0, 5);
    i = s.indexOf('s', 5);
    defs[1] = s.substring(5, i).trim();
    j = s.indexOf('s', i + 1) + 1;
    defs[2] = s.substring(i + 3, j).trim();
    defs[3] = s.substring(j).trim();
    return defs;
  }

  private static int parseID(String s) {
    return Integer.parseInt(s.substring(1, s.length() - 1).trim());
  }

  private static long parseValue(String s) {
    String[] defs;

    defs = s.split(" ");
    return (long) (Double.parseDouble(defs[0].trim()) * (double) getUnitMagnitude(defs[1].trim()));
  }

  private static long getUnitMagnitude(String units) {
    char unit;

    unit = units.charAt(0);
    switch (unit) {
      case 'K':
        return 1000;
      case 'M':
        return 1000000;
      case 'G':
        return 1000000000;
      default:
        throw new RuntimeException("Unknown unit: " + unit);
    }
  }

  public static void main(String[] args) {
    System.out.println(Measurement.parse("[502]  0.0-27.0 sec    137 MBytes  42.5 Mbits/sec"));
    System.out.println(Measurement.parse("[152]  0.0-31.0 sec  8.00 KBytes  2.11 Kbits/sec"));
    System.out.println(Measurement.parse("[100]  0.0-20.7 sec  3.39 GBytes  1.41 Gbits/sec"));
    System.out.println(Measurement.parse("[ 51]  0.0-20.4 sec  76.2 MBytes  31.3 Mbits/sec"));
    System.out.println(Measurement.parse("[234]  0.0-20.3 sec    107 MBytes  44.1 Mbits/sec"));
    System.out.println(Measurement.parse("[222]  0.0-23.1 sec  1.81 GBytes    673 Mbits/sec"));
    System.out.println(Measurement.parse("[152]  0.0-31.0 sec  8.00 KBytes  2.11 Kbits/sec"));
  }
}
