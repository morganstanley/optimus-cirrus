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
package com.ms.silverking.text;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ms.silverking.cloud.dht.crypto.MD5Digest;
import com.ms.silverking.numeric.MutableInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringUtil {
  private static final int defaultHexMinorGroupSize = CoreStringUtil.defaultHexMinorGroupSize;
  private static final int defaultHexMajorGroupSize = CoreStringUtil.defaultHexMajorGroupSize;

  private static Logger log = LoggerFactory.getLogger(StringUtil.class);

  private static final char[] quoteChars = { '"', '\'' };

  private static final String[] digits = CoreStringUtil.digits;

  private static final char hexMajorDelim = CoreStringUtil.hexMajorDelim;
  private static final char hexMinorDelim = CoreStringUtil.hexMinorDelim;
  private static final char delim = CoreStringUtil.delim;

  private static final String defaultNullString = "<null>";

  public static boolean isNullOrEmpty(String s) {
    return s == null || s.length() == 0;
  }

  public static boolean isNullOrEmptyTrimmed(String s) {
    return s == null || s.trim().length() == 0;
  }

  public static String[] splitAndTrim(String source, String regex) {
    String[] splitSource;

    splitSource = source.trim().split(regex);
    for (int i = 0; i < splitSource.length; i++) {
      splitSource[i] = splitSource[i].trim();
    }
    return splitSource;
  }

  public static String[] splitAndTrim(String source) {
    return splitAndTrim(source, "\\s+");
  }

  public static String toHexString(BigInteger x) {
    return byteArrayToHexString(x.toByteArray());
  }

  public static String byteArrayToHexString(byte[] inBytes) {
    return CoreStringUtil.byteArrayToHexString(inBytes);
  }

  public static String byteArrayToHexString(byte[] inBytes, int offset, int length) {
    return CoreStringUtil.byteArrayToHexString(inBytes, offset, length);
  }

  public static String byteArrayToHexString(byte[] inBytes,
                                            int offset,
                                            int length,
                                            int minorGroupSize,
                                            int majorGroupSize) {
    return CoreStringUtil.byteArrayToHexString(inBytes, offset, length, minorGroupSize, majorGroupSize);
  }

  public static String byteBufferToHexString(ByteBuffer buf) {
    return byteBufferToHexString(buf, defaultHexMinorGroupSize, defaultHexMajorGroupSize);
  }

  public static String byteBufferToHexString(ByteBuffer buf, int minorGroupSize, int majorGroupSize) {
    if (buf == null) {
      return "<null>";
    } else {
      StringBuilder out;

      out = new StringBuilder(buf.limit() * 2);
      for (int i = buf.position(); i < buf.limit(); i++) {
        byte curByte;

        curByte = buf.get(i);
        out.append(digits[(curByte & 0xF0) >>> 4]);
        out.append(digits[curByte & 0x0f]);
        if ((i + 1) % minorGroupSize == 0) {
          if (i % majorGroupSize == 0) {
            out.append(hexMajorDelim);
          } else {
            out.append(hexMinorDelim);
          }
        }
      }
      return out.toString();
    }
  }

  public static ByteBuffer hexStringToByteBuffer(String s) {
    MutableInteger sIndex;
    int bIndex;
    byte[] b;
    byte[] b2;

    sIndex = new MutableInteger();
    bIndex = 0;
    b = new byte[s.length() * 2];
    while (sIndex.getValue() < s.length()) {
      byte _b;
      String _s;

      _s = nextByteChars(s, sIndex);
      if (_s != null) {
        _b = parseByte(_s);
        b[bIndex] = _b;
        bIndex++;
      }
    }
    b2 = new byte[bIndex];
    System.arraycopy(b, 0, b2, 0, bIndex);
    return ByteBuffer.wrap(b2);
  }

  public static boolean parseBoolean(String s) throws ParseException {
    return CoreStringUtil.parseBoolean(s);
  }

  public static byte parseByte(String s) {
    if (s.length() != 2) {
      throw new RuntimeException("Must be two hex chars, not " + s);
    } else {
      char c0;
      char c1;

      c0 = s.charAt(0);
      c1 = s.charAt(1);
      return (byte) ((Character.digit(c0, 16) << 4) | (Character.digit(c1, 16)));
    }
  }

  static final String nextByteChars(String s, MutableInteger index) {
    StringBuilder sb;
    int startIndex;

    startIndex = index.getValue();
    sb = new StringBuilder();
    while (sb.length() < 2) {
      char c;

      if (index.getValue() >= s.length()) {
        if (sb.length() > 0) {
          throw new RuntimeException("Couldn't find two-char byte def starting at " + startIndex);
        } else {
          return null;
        }
      }
      c = s.charAt(index.getValue());
      if (isHexDigit(c)) {
        sb.append(c);
      } else if (c == hexMajorDelim || c == hexMinorDelim) {
        // skip this character
      } else {
        throw new RuntimeException("Bad char: " + c);
      }
      index.increment();
    }
    return sb.toString();
  }

  static boolean isHexDigit(char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
  }

  ///////////////////////////////////

  public static void display(String[][] sArray) {
    for (String[] member : sArray) {
      display(member);
    }
  }

  public static void display(List<String> list) {
    log.info("{}",listToString(list));
  }

  public static void display(List<String> list, char delim) {
    log.info("{}",listToString(list, delim));
  }

  public static void display(String[] sArray) {
    log.info("{}",arrayToString(sArray));
  }

  public static String arrayToString(String[][] sArray) {
    StringBuilder sBuf;

    sBuf = new StringBuilder();
    for (String[] subArray : sArray) {
      sBuf.append(arrayToString(subArray));
    }
    return sBuf.toString();
  }

  public static String arrayToString(Object[] sArray) {
    return getToString(Arrays.asList(sArray), delim);
  }

  public static String arrayToString(Object[] sArray, char _delim) {
    return getToString(Arrays.asList(sArray), _delim);
  }

  public static String arrayToQuotedString(String[] sArray) {
    return CoreStringUtil.arrayToQuotedString(sArray);
  }

  public static String listToString(List<String> sArray) {
    return listToString(sArray, delim);
  }

  public static String listToString(List<? extends Object> sArray, char delim) {
    return getToString(sArray, delim);
  }

  public static String toString(Collection<String> sArray) {
    return toString(sArray, delim);
  }

  public static String toString(Collection<? extends Object> sArray, char delim) {
    return getToString(sArray, delim);
  }

  private static <T> String getToString(Iterable<T> iter, char delim) {
    return CoreStringUtil.getToString(iter, "", "", delim);
  }

  public static final boolean startsWithIgnoreCase(String target, String value) {
    if (target.length() < value.length()) {
      return false;
    } else {
      String targetStart;
      targetStart = target.substring(0, value.length());
      return targetStart.equalsIgnoreCase(value);
    }
  }

  public static String md5(String string) {
    return CoreStringUtil.md5(string);
  }

  public static String trimLength(String s, int length) {
    return CoreStringUtil.trimLength(s, length);
  }

  public static int countOccurrences(String string, char c) {
    return CoreStringUtil.countOccurrences(string, c);
  }

  public static int countOccurrences(String string, char c, int limit) {
    return CoreStringUtil.countOccurrences(string, c, limit);
  }

  public static String replicate(char c, int n) {
    return CoreStringUtil.replicate(c, n);
  }

  public static String replicate(String string, int n) {
    return CoreStringUtil.replicate(string, n);
  }

  public static String[] splitAndTrimQuoted(String source) {
    ArrayList<String> tokens;
    Pattern pattern;
    Matcher matcher;
    int prevEnd;

    prevEnd = 0;
    tokens = new ArrayList<String>();
    pattern = Pattern.compile("'((\\S+)(\\s*))*?\\S+'");
    matcher = pattern.matcher(source);
    while (matcher.find()) {
      String token;

      if (matcher.start() > prevEnd) {
        String[] unquotedTokens;

        unquotedTokens = splitAndTrim(source.substring(prevEnd, matcher.start()));
        for (String ut : unquotedTokens) {
          if (ut.trim().length() > 0) {
            tokens.add(ut);
          }
        }
      }
      token = source.substring(matcher.start(), matcher.end());
      prevEnd = matcher.end();
      if (token.trim().length() > 0) {
        tokens.add(token);
      }
    }
    if (prevEnd < source.length()) {
      String[] unquotedTokens;

      unquotedTokens = splitAndTrim(source.substring(prevEnd));
      for (String ut : unquotedTokens) {
        tokens.add(ut);
      }
    }
    return tokens.toArray(new String[0]);
  }

  public static List<String> projectColumn(List<String> src, int column, String regex, boolean filterNonexistent) {
    List<String> projection;

    projection = new ArrayList<String>();
    for (String line : src) {
      String[] columns;

      columns = line.split(regex);
      if (column < columns.length) {
        projection.add(columns[column]);
      } else {
        if (!filterNonexistent) {
          projection.add(null);
        }
      }
    }
    return projection;
  }

  public static String stripQuotes(String s) {
    for (char c : quoteChars) {
      if (s.charAt(0) == c) {
        if (s.charAt(s.length() - 1) == c) {
          s = s.substring(1, s.length() - 1);
        } else {
          throw new RuntimeException("Unmatched quote in " + s);
        }
      } else {
        if (s.charAt(s.length() - 1) == c) {
          throw new RuntimeException("Unmatched quote in " + s);
        }
      }
    }
    return s;
  }

  public static String nullSafeToString(Object o) {
    return CoreStringUtil.nullSafeToString(o);
  }

  public static String nullSafeToString(Object o, String nullString) {
    return CoreStringUtil.nullSafeToString(o, nullString);
  }

  public static String firstNCharsToUpperCase(String s, int n) {
    if (s == null || s.length() < 1) {
      return s;
    } else {
      String s0;

      if (n > s.length()) {
        n = s.length();
      }
      s0 = s.substring(0, n).toUpperCase();
      if (s.length() <= n) {
        return s0;
      } else {
        return s0 + s.substring(n);
      }
    }
  }

  public static String firstCharToUpperCase(String s) {
    return firstNCharsToUpperCase(s, 1);
  }
}
