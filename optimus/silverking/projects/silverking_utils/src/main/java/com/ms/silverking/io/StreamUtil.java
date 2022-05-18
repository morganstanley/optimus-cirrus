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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ms.silverking.numeric.NumConversion;

public final class StreamUtil {
  private static final int DEF_SKIP_BUF_SIZE = 4096;
  private static final int DEF_BUF_SIZE = 128;
  private static final int NEWLINE = '\n';
  private static final int CR = '\r';

  private static Logger log = LoggerFactory.getLogger(StreamUtil.class);

  public static void readFully(InputStream in, byte[] buf) throws IOException {
    readFully(in, buf, 0, buf.length);
  }

  public static void readFully(InputStream in, byte[] buf, int offset, int length) throws IOException {
    int totalBytesRead;

    totalBytesRead = 0;
    while (totalBytesRead < length) {
      int bytesRead;

      bytesRead = in.read(buf, offset + totalBytesRead, length - totalBytesRead);
      if (bytesRead >= 0) {
        totalBytesRead += bytesRead;
      } else {
        log.debug("{} {} {} {} {}",buf.length , offset,  length, totalBytesRead ,bytesRead);
        throw new EOFException();
      }
    }
  }

  public static int readIntLE(InputStream in) throws IOException {
    byte b0;
    byte b1;
    byte b2;
    byte b3;

    b3 = readByte(in);
    b2 = readByte(in);
    b1 = readByte(in);
    b0 = readByte(in);
    return NumConversion.bytesToInt(b0, b1, b2, b3);
  }

  public static int readInt(InputStream in) throws IOException {
    byte b0;
    byte b1;
    byte b2;
    byte b3;

    b0 = readByte(in);
    b1 = readByte(in);
    b2 = readByte(in);
    b3 = readByte(in);
    return NumConversion.bytesToInt(b0, b1, b2, b3);
  }

  public static byte readByte(InputStream in) throws IOException {
    int val;

    val = in.read();
    if (val < 0) {
      throw new EOFException();
    }
    return (byte) val;
  }

  public static void skipBytes(InputStream in, long bytesToSkip) throws IOException {
    long numRead;
    byte[] b;

    if (bytesToSkip <= 0) {
      // early exit before allocating
      return;
    }
    b = new byte[DEF_SKIP_BUF_SIZE];
    numRead = 0;
    while (numRead < bytesToSkip) {
      long skipBytesLeft;
      int readSize;

      skipBytesLeft = (bytesToSkip - numRead);
      if (skipBytesLeft > Integer.MAX_VALUE) {
        readSize = b.length;
      } else {
        readSize = Math.min(b.length, (int) skipBytesLeft);
      }
      numRead += in.read(b, 0, readSize);
    }
  }

  public static void readBytes(byte[] b, int offset, int length, InputStream in) throws IOException {
    CoreStreamUtil.readBytes(b, offset, length, in);
  }

  public static void readBytes(byte[] b, InputStream in) throws IOException {
    readBytes(b, 0, b.length, in);
  }

  public static long limitedStream(InputStream in, OutputStream out, boolean close, long limit) throws IOException {
    byte[] buf;

    buf = new byte[DEF_BUF_SIZE];
    return limitedStream(in, out, close, buf, limit);
  }

  public static long stream(InputStream in, OutputStream out, boolean close) throws IOException {
    byte[] buf;

    buf = new byte[DEF_BUF_SIZE];
    return stream(in, out, close, buf);
  }

  public static String readLine(InputStream in) throws IOException {
    StringBuilder sb;
    int c;

    sb = new StringBuilder();
    do {
      c = in.read();
      if (c != NEWLINE && c != CR && c >= 0) {
        sb.append((char) c);
      }
    } while (c != NEWLINE && c != CR && c >= 0);
    return sb.toString();
  }

  public static byte[] readToBytes(InputStream in, boolean close) throws IOException {
    ByteArrayOutputStream tmp;

    tmp = new ByteArrayOutputStream();
    stream(in, tmp, close, new byte[DEF_BUF_SIZE]);
    return tmp.toByteArray();
  }

  ////////////////////////////////////////////////////////////

  public static long limitedStream(InputStream in, OutputStream out, boolean close, byte[] buf, long limit)
      throws IOException {
    long numWritten;

    numWritten = 0;
    try {
      int numRead;

      numRead = 0;
      do {
        int maxToRead;
        long remaining;

        remaining = limit - numWritten;
        if (remaining >= buf.length) {
          maxToRead = buf.length;
        } else {
          maxToRead = (int) remaining;
        }
        numRead = in.read(buf, 0, maxToRead);
        if (numRead > 0) {
          out.write(buf, 0, numRead);
          out.flush();
          numWritten += numRead;
        }
      } while (numRead >= 0 && numWritten < limit);
    } finally {
      out.flush();
      if (close && in != null) {
        try {
          in.close();
        } catch (IOException ioe_close) {
          ioe_close.printStackTrace();
        }
      }
      if (close && out != null) {
        try {
          out.close();
        } catch (IOException ioe_close) {
          ioe_close.printStackTrace();
        }
      }
    }
    return numWritten;
  }

  public static long stream(InputStream in, OutputStream out, boolean close, byte[] buf) throws IOException {
    long numWritten;

    numWritten = 0;
    try {
      int numRead;

      numRead = 0;
      do {
        numRead = in.read(buf);
        if (numRead > 0) {
          out.write(buf, 0, numRead);
          out.flush();
          numWritten += numRead;
        }
      } while (numRead >= 0);
    } finally {
      out.flush();
      if (close && in != null) {
        try {
          in.close();
        } catch (IOException ioe_close) {
          ioe_close.printStackTrace();
        }
      }
      if (close && out != null) {
        try {
          out.close();
        } catch (IOException ioe_close) {
          ioe_close.printStackTrace();
        }
      }
    }
    return numWritten;
  }

  public static long streamToFile(InputStream in, File outFile, boolean close) throws IOException {
    return stream(in, new FileOutputStream(outFile), close);
  }

  public static long streamToFile(InputStream in, File outFile, boolean close, byte[] buf) throws IOException {
    return stream(in, new FileOutputStream(outFile), close, buf);
  }

  public static long streamToFile(InputStream in, String outFileName, boolean close) throws IOException {
    return streamToFile(in, new File(outFileName), close);
  }

  public static long streamToFile(String s, File outFile, boolean close) throws IOException {
    return streamToFile(stringToInputStream(s), outFile, close);
  }

  public static long streamToFile(String s, File outFile) throws IOException {
    return streamToFile(stringToInputStream(s), outFile, true);
  }

  public static long streamToFile(String s, String outFileName, boolean close) throws IOException {
    return streamToFile(s, new File(outFileName), close);
  }

  public static long streamToFile(String s, String outFileName) throws IOException {
    return streamToFile(s, new File(outFileName), true);
  }

  public static InputStream stringToInputStream(String s) {
    return new ByteArrayInputStream(s.getBytes());
  }
}
