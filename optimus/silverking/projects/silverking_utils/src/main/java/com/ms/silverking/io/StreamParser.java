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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

public class StreamParser {
  public enum TrimMode {noTrim, trim}
  public enum CloseMode {noClose, close}

  private static final char setDelimiterChar = ',';
  private static final String setDelimiter = "" + setDelimiterChar;
  private static final String mapChar = "=";

  public static List<String> parseFileLines(File file, String regex) throws IOException {
    return parseLines(new FileInputStream(file), regex);
  }

  public static List<String> parseFileLines(File file) throws IOException {
    return parseLines(new FileInputStream(file));
  }

  public static List<String> parseGZFileLines(File file, TrimMode trimMode) throws IOException {
    return parseLines(new GZIPInputStream(new FileInputStream(file)), trimMode);
  }

  public static List<String> parseFileLines(File file, TrimMode trimMode) throws IOException {
    return parseLines(new FileInputStream(file), trimMode);
  }

  public static List<String> parseFileLines(String fileName) throws IOException {
    return parseLines(new FileInputStream(fileName));
  }

  public static List<String> parseFileLines(String fileName, TrimMode trimMode) throws IOException {
    return parseLines(new FileInputStream(fileName), trimMode);
  }

  public static List<String> parseLines(File file) throws IOException {
    return parseLines(new FileInputStream(file), TrimMode.trim);
  }

  public static List<String> parseLines(File file, TrimMode trimMode) throws IOException {
    return parseLines(new FileInputStream(file), trimMode, CloseMode.close, null);
  }

  public static List<String> parseLines(InputStream inStream) throws IOException {
    return parseLines(inStream, TrimMode.trim, CloseMode.close, null);
  }

  public static List<String> parseLines(InputStream inStream, String regex) throws IOException {
    return parseLines(inStream, TrimMode.trim, CloseMode.close, regex);
  }

  public static List<String> parseLines(InputStream inStream, CloseMode closeMode) throws IOException {
    return parseLines(inStream, TrimMode.trim, closeMode, null);
  }

  public static List<String> parseLines(InputStream inStream, TrimMode trimMode) throws IOException {
    return parseLines(inStream, trimMode, CloseMode.close, null);
  }

  public static List<String> parseLines(InputStream inStream, TrimMode trimMode, CloseMode closeMode, String regex)
      throws IOException {
    BufferedReader reader;
    ArrayList<String> lines;
    String line;

    reader = new BufferedReader(new InputStreamReader(inStream));
    try {
      lines = new ArrayList<String>();
      do {
        line = reader.readLine();
        if (line != null) {
          if (regex == null || line.matches(regex)) {
            if (trimMode == TrimMode.trim) {
              lines.add(line.trim());
            } else {
              lines.add(line);
            }
          }
        }
      } while (line != null);
    } finally {
      if (closeMode == CloseMode.close) {
        reader.close();
      }
    }
    return lines;
  }

  public static String parseLine(File file) throws IOException {
    return parseLine(file, TrimMode.trim, CloseMode.close);
  }

  public static String parseLine(File file, TrimMode trimMode, CloseMode closeMode) throws IOException {
    return parseLine(new FileInputStream(file), trimMode, closeMode);
  }

  public static String parseLine(InputStream inStream) throws IOException {
    return parseLine(inStream, TrimMode.trim, CloseMode.close);
  }

  public static String parseLine(InputStream inStream, TrimMode trimMode, CloseMode closeMode) throws IOException {
    BufferedReader reader;

    reader = new BufferedReader(new InputStreamReader(inStream));
    try {
      if (trimMode == TrimMode.trim) {
        return reader.readLine().trim();
      } else {
        return reader.readLine();
      }
    } finally {
      if (closeMode == CloseMode.close) {
        reader.close();
      }
    }
  }

  public static Map<String, String> parseMap(InputStream inStream) throws IOException {
    return parseMap(inStream, TrimMode.trim, CloseMode.close);
  }

  public static Map<String, String> parseMap(InputStream inStream, TrimMode trimMode, CloseMode closeMode)
      throws IOException {
    Map<String, String> map;
    List<String> lines;

    map = new HashMap<>();
    lines = parseLines(inStream, trimMode, closeMode, null);
    for (String line : lines) {
      int mapCharIndex;

      mapCharIndex = line.indexOf(mapChar);
      if (mapCharIndex > 0) {
        if (line.indexOf(mapChar, mapCharIndex + 1) >= 0) {
          throw new IOException("Bad map entry: " + line);
        }
        map.put(line.substring(0, mapCharIndex), line.substring(mapCharIndex + 1));
      }
    }
    return map;
  }

  public static Set<String> parseSet(InputStream inStream) throws IOException {
    return parseSet(inStream, TrimMode.trim, CloseMode.close);
  }

  public static Set<String> parseSet(InputStream inStream, TrimMode trimMode, CloseMode closeMode) throws IOException {
    Set<String> set;
    List<String> lines;

    set = new HashSet<>();
    lines = parseLines(inStream, trimMode, closeMode, null);
    for (String line : lines) {
      line = line.trim();
      if (line.length() > 0) {
        int index;

        index = 0;
        while (index < line.length()) {
          int nextIndex;

          if (line.charAt(index) == setDelimiterChar) {
            throw new RuntimeException("bad set definition: " + line);
          }
          nextIndex = line.indexOf(setDelimiter, index);
          if (nextIndex < 0) {
            nextIndex = line.length();
          }
          set.add(line.substring(index, nextIndex));
          index = nextIndex + 1;
        }
      }
    }
    return set;
  }
}

