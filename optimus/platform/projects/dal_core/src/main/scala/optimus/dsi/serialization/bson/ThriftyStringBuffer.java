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

import java.io.UnsupportedEncodingException;
// TODO (OPTIMUS-57177): license needs reformatting in order to publish this code as open-source
/**
 * This is a modification of PoolOutputBuffer used in reading strings from BSON encoded
 * SerializedEntitiy byte arrays.
 *
 * <p>It uses a thread local buffer strategy to minimise growing and copying of char and byte
 * arrays.
 */
class ThriftyStringBuffer {
  public static final int CharBuffSize = 100;
  public static final int CharBuffMaxSize = 1024 * 1024 * 10;
  public static final int ByteBuffSize = 100;
  public static final int ByteBuffMaxSize = 1024 * 1024 * 10;

  private static final ThreadLocal<byte[]> byteBuffThreadLocal =
      new ThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() {
          return new byte[ByteBuffSize];
        }
      };
  private static final ThreadLocal<char[]> charBuffThreadLocal =
      new ThreadLocal<char[]>() {
        @Override
        protected char[] initialValue() {
          return new char[CharBuffSize];
        }
      };

  int pos = 0;
  boolean allAscii = true;

  private byte[] byteBuff = byteBuffThreadLocal.get();
  private char[] charBuff = charBuffThreadLocal.get();

  private void ensureCapacity() {
    ensureCapacity(1);
  }

  private void ensureCapacity(int len) {
    if (pos + len > byteBuff.length) {
      int newSize = Math.max(byteBuff.length * 3 / 2 + 1, pos + len);
      byte[] newBuff = new byte[newSize];
      System.arraycopy(byteBuff, 0, newBuff, 0, byteBuff.length);
      if (newSize < ByteBuffMaxSize) {
        byteBuffThreadLocal.set(newBuff);
      }
      byteBuff = newBuff;
    }
  }

  private void ensureCharCapacity(int len) {
    if (charBuff == null || len > charBuff.length) {
      charBuff = new char[len];
      if (len < CharBuffMaxSize) {
        charBuffThreadLocal.set(charBuff);
      }
    }
  }

  public void reset() {
    pos = 0;
    allAscii = true;
  }

  public void write(byte[] bytes, int offset, int length) {
    ensureCapacity(length);
    for (int i = offset; i < length; i++) {
      _write(bytes[i]);
    }
  }

  public void write(byte b) {
    ensureCapacity();
    _write(b);
  }

  private void _write(byte b) {
    allAscii = allAscii && ThriftyBSONDecoder._isAscii(b);
    byteBuff[pos++] = b;
  }

  public String get() throws UnsupportedEncodingException {
    String newStr;
    if (allAscii) {
      newStr = getAscii();
    } else {
      newStr = getUTF8();
    }
    return newStr;
  }

  private String getAscii() {
    ensureCharCapacity(pos);
    for (int i = 0; i < pos; i++) {
      charBuff[i] = (char) byteBuff[i];
    }
    String newStr = new String(charBuff, 0, pos);
    reset();
    return newStr;
  }

  public String getUTF8() throws UnsupportedEncodingException {
    String newStr = new String(byteBuff, 0, pos, ThriftyBSONDecoder.DEFAULT_ENCODING);
    reset();
    return newStr;
  }

  public void fill(byte[] src, int offset, int len) {
    ensureCapacity(len);
    System.arraycopy(src, offset, byteBuff, pos, len);
    pos += len;
  }
}
