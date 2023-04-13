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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class StatSample {
  private final int sampleIndex;
  private final long absTimeMillis;
  private final List<Segment> segments;

  private enum DateFormat {DATE, SECONDS}

  ;
  private static final DateFormat dateFormat;
  private static String DateFormatProperty = "DateFormat";

  private static int _sampleIndex;

  static {
    String v;

    v = System.getProperty(DateFormatProperty);
    if (v != null) {
      dateFormat = DateFormat.valueOf(v);
    } else {
      dateFormat = DateFormat.DATE;
    }
    //System.out.println(dateFormat);
  }

  public StatSample(int sampleIndex, long absTimeMillis, List<Segment> segments) {
    this.sampleIndex = sampleIndex;
    this.absTimeMillis = absTimeMillis;
    this.segments = segments;
  }

  public int getSampleIndex() {
    return sampleIndex;
  }

  public long getAbsTimeMillis() {
    return absTimeMillis;
  }

  public List<Segment> getSegments() {
    return segments;
  }

  public List<Stat> getMatchingStats(String regex) {
    List<Stat> stats;

    stats = new ArrayList<>();
    for (Stat stat : Segment.getAllStats(segments)) {
      if (stat.getFullName().matches(regex)) {
        stats.add(stat);
      }
    }
    return stats;
  }

  public static StatSample parse(List<String> def) {
    int sampleIndex;
    long absTimeMillis;
    int line;

    line = 0;
    try {
      switch (dateFormat) {
      case DATE:
        sampleIndex = Integer.parseInt(def.get(line++));
        absTimeMillis = unixDateToAbsTimeMillis(def.get(line++));
        break;
      case SECONDS:
        sampleIndex = _sampleIndex++;
        absTimeMillis = unixSecondsToAbsTimeMillis(def.get(line++));
        break;
      default:
        throw new RuntimeException("panic");
      }
    } catch (ParseException pe) {
      throw new RuntimeException(pe);
    }
    return new StatSample(sampleIndex, absTimeMillis, Segment.parseSegments(def, line));
  }

  private static long unixDateToAbsTimeMillis(String date) throws ParseException {
    SimpleDateFormat simpleDateFormat;

    simpleDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");
    return simpleDateFormat.parse(date).getTime();
  }

  private static long unixSecondsToAbsTimeMillis(String s) {
    return Long.parseLong(s) * 1000;
  }

  @Override
  public String toString() {
    StringBuffer sb;

    sb = new StringBuffer();
    sb.append(sampleIndex + "\n");
    sb.append(new Date(absTimeMillis) + "\n");
    for (Segment segment : segments) {
      sb.append(segment);
    }
    return sb.toString();
  }
}
