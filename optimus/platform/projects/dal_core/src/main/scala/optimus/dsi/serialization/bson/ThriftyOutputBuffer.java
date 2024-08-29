/**
 * Copyright (C) 2008 10gen Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
package optimus.dsi.serialization.bson;

import java.io.IOException;
import java.io.OutputStream;
// TODO (OPTIMUS-57177): license needs reformatting in order to publish this code as open-source
/**
 * This class is a modification of BasicOutputBuffer in order to optimise for memory usage when
 * serializing SerializedEntity object directly into BSON byte arrays.
 *
 * <p>It uses a thread local byte array for writing to minimise growing and copying arrays.
 */
public class ThriftyOutputBuffer extends OutputStream {

  private static final int StartBuffSize = 512;
  private static final int MaxThreadLocalSize = 1024 * 1024 * 10;
  private static final ThreadLocal<byte[]> bufferThreadLocal =
      new ThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() {
          return new byte[StartBuffSize];
        }
      };
  private byte[] buff = bufferThreadLocal.get();
  private int pos = 0;

  /**
   * Writes the given integer value to the buffer.
   *
   * @param x the value to write
   */
  public void writeInt(int x) {
    write(x >> 0);
    write(x >> 8);
    write(x >> 16);
    write(x >> 24);
  }

  public void writeInt(int pos, int x) {
    final int save = getPosition();
    setPosition(pos);
    writeInt(x);
    setPosition(save);
  }

  /**
   * Writes the given long value to the buffer.
   *
   * @param x the value to write
   */
  public void writeLong(long x) {
    write((byte) (0xFFL & (x >> 0)));
    write((byte) (0xFFL & (x >> 8)));
    write((byte) (0xFFL & (x >> 16)));
    write((byte) (0xFFL & (x >> 24)));
    write((byte) (0xFFL & (x >> 32)));
    write((byte) (0xFFL & (x >> 40)));
    write((byte) (0xFFL & (x >> 48)));
    write((byte) (0xFFL & (x >> 56)));
  }

  /**
   * Writes a BSON double to the stream.
   *
   * @param x the value
   */
  public void writeDouble(double x) {
    writeLong(Double.doubleToRawLongBits(x));
  }

  /**
   * Writes a BSON String to the stream.
   *
   * @param str the value
   */
  public void writeString(final String str) {
    writeInt(0); // making space for size
    final int strLen = writeCString(str, false);
    backpatchSize(strLen, 4);
  }

  /**
   * Writes C string (null-terminated string) to underlying buffer.
   *
   * @param str the string
   * @return number of bytes written
   */
  public int writeCString(final String str) {
    return writeCString(str, true);
  }

  private int writeCString(final String str, final boolean checkForNullCharacters) {

    final int len = str.length();
    int total = 0;

    for (int i = 0; i < len; /*i gets incremented*/ ) {
      final int c = Character.codePointAt(str, i);

      if (checkForNullCharacters && c == 0x0) {
        throw new IllegalArgumentException(
            String.format(
                "BSON cstring '%s' is not valid because it contains a null character at index %d",
                str, i));
      }
      if (c < 0x80) {
        write((byte) c);
        total += 1;
      } else if (c < 0x800) {
        write((byte) (0xc0 + (c >> 6)));
        write((byte) (0x80 + (c & 0x3f)));
        total += 2;
      } else if (c < 0x10000) {
        write((byte) (0xe0 + (c >> 12)));
        write((byte) (0x80 + ((c >> 6) & 0x3f)));
        write((byte) (0x80 + (c & 0x3f)));
        total += 3;
      } else {
        write((byte) (0xf0 + (c >> 18)));
        write((byte) (0x80 + ((c >> 12) & 0x3f)));
        write((byte) (0x80 + ((c >> 6) & 0x3f)));
        write((byte) (0x80 + (c & 0x3f)));
        total += 4;
      }

      i += Character.charCount(c);
    }

    write((byte) 0);
    total++;
    return total;
  }

  @Override
  public String toString() {
    return getClass().getName() + " size: " + size() + " pos: " + getPosition();
  }

  /**
   * Backpatches the size of a document or string by writing the size into the four bytes starting
   * at getPosition() - size.
   *
   * @param size the size of the document/string
   */
  public void backpatchSize(final int size) {
    writeInt(getPosition() - size, size);
  }

  /**
   * Backpatches the size of a document or string by writing the size into the four bytes starting
   * at {@code getPosition() - size - additionalOffset}.
   *
   * @param size the size of the document/string
   * @param additionalOffset the offset from the current position to write the size
   */
  protected void backpatchSize(final int size, final int additionalOffset) {
    writeInt(getPosition() - size - additionalOffset, size);
  }

  /**
   * Truncates the buffer to the given new position, which must be greater than or equal to zero and
   * less than or equal to the current size of this buffer.
   *
   * @param newPosition the position to truncate this buffer to
   */
  public void truncateToPosition(int newPosition) {
    setPosition(newPosition);
  }

  public byte[] toByteArray() {
    byte[] ba = new byte[pos];
    System.arraycopy(buff, 0, ba, 0, pos);
    return ba;
  }

  private void ensureCapacity(int len) {
    if (pos + len >= buff.length) {
      int newSize = Math.max(pos + len, buff.length * 2 + 1);
      byte[] newBuff = new byte[newSize];
      System.arraycopy(buff, 0, newBuff, 0, buff.length);
      if (newBuff.length < MaxThreadLocalSize) bufferThreadLocal.set(newBuff);
      buff = newBuff;
    }
  }

  @Override
  public void write(byte[] b) {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) {
    ensureCapacity(len);
    System.arraycopy(b, 0, buff, pos, len);
    pos += len;
  }

  @Override
  public void write(int b) {
    ensureCapacity(1);
    buff[pos++] = (byte) (0xFF & b);
  }

  public int getPosition() {
    return pos;
  }

  public void setPosition(int position) {
    pos = position;
  }

  public int size() {
    return pos;
  }

  /**
   * Pipe the contents of this output buffer into the given output stream
   *
   * @param out the stream to pipe to
   * @return number of bytes written to the stream
   * @throws java.io.IOException if the stream throws an exception
   */
  public int pipe(OutputStream out) throws IOException {
    out.write(buff, 0, pos);
    return pos;
  }
}
