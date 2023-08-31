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

import java.util.ArrayList;
import java.util.List;

public class Segment {
  private final String fullName;
  private final String name;
  private final List<Segment> subSegments;
  private final List<Stat> stats;

  private static final int emptyLine = -1;
  private static final String indentationString = "    ";
  private static final int indentation = indentationString.length();

  public Segment(String fullName, String name, List<Segment> subSegments, List<Stat> stats) {
    this.fullName = fullName;
    this.name = name;
    this.subSegments = subSegments;
    this.stats = stats;
  }

  public String getName() {
    return name;
  }

  public List<Segment> getSubSegments() {
    return subSegments;
  }

  public List<Stat> getStats() {
    return stats;
  }

  private static void ensureName(String s) {
    if (!isName(s)) {
      throw new RuntimeException("Not name: " + s);
    }
  }

  private static boolean isName(String s) {
    return s.endsWith(":");
  }

  private static int depthNonEmpty(String s) {
    int depth;

    depth = depth(s);
    if (depth == emptyLine) {
      throw new RuntimeException("Unexpected empty line");
    }
    return depth;
  }

  private static int depth(String s) {
    for (int i = 0; i < s.length(); i++) {
      if (s.charAt(i) != ' ') {
        return i;
      }
    }
    return emptyLine;
  }

  public static int findNextLineAtDepth(List<String> lines, int startLine) {
    int depth;

    depth = depthNonEmpty(lines.get(startLine));
    for (int i = startLine + 1; i < lines.size(); i++) {
      if (depthNonEmpty(lines.get(i)) <= depth) {
        return i;
      }
    }
    return lines.size();
  }

  public static Segment parse(String parentName, List<String> def) {
    int myDepth;
    String nameDef;
    String fullName;
    String name;
    List<Segment> subSegments;
    List<Stat> stats;

    // System.out.printf("parse %d\n", def.size());
    nameDef = def.get(0);
    myDepth = depthNonEmpty(nameDef);
    ensureName(nameDef);
    name = nameDef.trim();
    fullName = parentName != null ? parentName + ":" + name : name;
    stats = new ArrayList<>();
    subSegments = new ArrayList<>();
    for (int i = 1; i < def.size(); ) {
      String s;

      s = def.get(i);
      if (isName(s)) {
        int lastDefLine;

        lastDefLine = findNextLineAtDepth(def, i);
        if (lastDefLine != i) {
          subSegments.add(parse(fullName, def.subList(i, lastDefLine - 1)));
        } else {
          throw new RuntimeException("Unexpected empty name: " + s);
        }
        i = lastDefLine;
      } else {
        int sDepth;

        sDepth = depthNonEmpty(s);
        if (sDepth <= myDepth) {
          System.err.println();
          System.err.println();
          throw new RuntimeException("Unexpected depth");
        } else {
          if (s.indexOf("Quick") < 0 && s.indexOf("Detected") < 0) {
            stats.add(Stat.parse(fullName, s));
          }
        }
        i++;
      }
    }
    return new Segment(fullName, name, subSegments, stats);
  }

  public static List<Segment> parseSegments(List<String> def, int line) {
    List<Segment> segments;
    List<String> curDef;

    // System.out.printf("parseSegments %d %d\n", def.size(), line);
    segments = new ArrayList<>();
    curDef = new ArrayList<>();
    for (int i = line; i < def.size(); i++) {
      String s;

      s = def.get(i);
      if (s.charAt(0) != ' ') {
        // System.out.println("segmentStart: "+ s);
        if (curDef.size() > 0) {
          segments.add(parse(null, curDef));
          curDef = new ArrayList<>();
        }
        curDef.add(s);
      } else {
        curDef.add(s);
      }
    }
    if (curDef.size() > 0) {
      segments.add(parse(null, curDef));
    }
    return segments;
  }

  @Override
  public String toString() {
    StringBuilder sb;

    sb = new StringBuilder();
    toString(sb, 0);
    return sb.toString();
  }

  public void toString(StringBuilder sb, int depth) {
    for (int i = 0; i < depth; i++) {
      sb.append(indentationString);
    }
    sb.append(name);
    sb.append('\n');
    for (Stat stat : stats) {
      for (int i = 0; i < depth + 1; i++) {
        sb.append(indentationString);
      }
      sb.append(stat);
      sb.append('\n');
    }
    for (Segment subSegment : subSegments) {
      subSegment.toString(sb, depth + 1);
    }
  }

  public static List<Stat> getAllStats(List<Segment> segments) {
    List<Stat> stats;

    stats = new ArrayList<>();
    for (Segment segment : segments) {
      stats.addAll(segment.getStats());
    }
    return stats;
  }

  public static void main(String[] args) {
    try {
      System.out.println(parse(null, testDef()));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static List<String> testDef() {
    ArrayList<String> al;

    al = new ArrayList<>();
    al.add("            Icmp:");
    al.add("                188360 ICMP messages received");
    al.add("                18 input ICMP message failed.");
    al.add("                ICMP input histogram:");
    al.add("                    destination unreachable: 498");
    al.add("                    timeout in transit: 2");
    al.add("                    echo requests: 161716");
    al.add("                    echo replies: 26144");
    al.add("                188136 ICMP messages sent");
    al.add("                0 ICMP messages failed");
    al.add("                ICMP output histogram:");
    al.add("                    destination unreachable: 272");
    al.add("                    time exceeded: 1");
    al.add("                    echo request: 26156");
    al.add("                    echo replies: 161707");
    return al;
  }
}
