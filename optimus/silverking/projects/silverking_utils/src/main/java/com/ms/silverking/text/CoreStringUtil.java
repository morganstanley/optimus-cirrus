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

import java.security.MessageDigest;
import java.text.ParseException;
import java.util.Arrays;

import com.ms.silverking.cloud.dht.crypto.MD5Digest;

public class CoreStringUtil {
  static final char delim = ' ';

  public static final int defaultHexMinorGroupSize = 4;
  public static final int defaultHexMajorGroupSize = 16;

  public static final String[] digits = { "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e",
      "f" };

  public static final char hexMajorDelim = ' ';
  public static final char hexMinorDelim = ':';
  private static final String defaultNullString = "<null>";

  public static boolean parseBoolean(String s) throws ParseException {
    if (s.equalsIgnoreCase("true")) {
      return true;
    } else if (s.equalsIgnoreCase("false")) {
      return false;
    } else {
      throw new ParseException("Not a valid boolean: " + s, 0);
    }
  }

  public static String trimLength(String s, int length) {
    return s.substring(0, Math.min(s.length(), length));
  }

  public static String arrayToQuotedString(String[] sArray) {
    return getToStringQuoted(Arrays.asList(sArray));
  }

  private static <T> String getToStringQuoted(Iterable<T> iter) {
    return getToString(iter, "\"", "\"", CoreStringUtil.delim);
  }

  static <T> String getToString(Iterable<T> iter, String before, String after, char delim) {
    StringBuilder sBuf;

    sBuf = new StringBuilder();
    for (Object member : iter) {
      sBuf.append(before);
      sBuf.append(member);
      sBuf.append(after);
      sBuf.append(delim);
    }
    return sBuf.toString();
  }

  public static String nullSafeToString(Object o) {
    return nullSafeToString(o, defaultNullString);
  }

  public static String nullSafeToString(Object o, String nullString) {
    if (o == null) {
      return nullString;
    } else {
      return o.toString();
    }
  }

  public static String replicate(char c, int n) {
    return replicate(c + "", n);
  }

  public static String replicate(String string, int n) {
    StringBuilder sBuf;

    sBuf = new StringBuilder();
    for (int i = 0; i < n; i++) {
      sBuf.append(string);
    }
    return sBuf.toString();
  }

  public static int countOccurrences(String string, char c) {
    return countOccurrences(string, c, Integer.MAX_VALUE);
  }

  public static int countOccurrences(String string, char c, int limit) {
    int occurrences;

    occurrences = 0;
    limit = Math.min(limit, string.length());
    for (int i = 0; i < limit; i++) {
      if (string.charAt(i) == c) {
        occurrences++;
      }
    }
    return occurrences;
  }

  public static String md5(String string) {
    MessageDigest digest;

    digest = MD5Digest.getLocalMessageDigest();
    digest.update(string.getBytes(), 0, string.length());
    return byteArrayToHexString(digest.digest());
  }

  public static String byteArrayToHexString(byte[] inBytes) {
    return byteArrayToHexString(inBytes, 0, inBytes.length);
  }

  public static String byteArrayToHexString(byte[] inBytes, int offset, int length) {
    return byteArrayToHexString(inBytes, offset, length, defaultHexMinorGroupSize, defaultHexMajorGroupSize);
  }

  public static String byteArrayToHexString(byte[] inBytes,
                                            int offset,
                                            int length,
                                            int minorGroupSize,
                                            int majorGroupSize) {
    if (inBytes == null) {
      return defaultNullString;
    } else {
      StringBuilder out;

      out = new StringBuilder(inBytes.length * 2);
      for (int i = 0; i < length; i++) {
        byte curByte;

        curByte = inBytes[offset + i];
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

}
