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
package com.ms.silverking.cloud.skfs.dir.serverside;

import com.ms.silverking.util.ArrayUtil;

public class ByteString implements Comparable<ByteString> {
  private final byte[] buf;
  private final int offset;
  private final int length;
  private final int hashCode;

  public ByteString(byte[] buf, int offset, int length) {
    this.buf = buf;
    this.offset = offset;
    this.length = length;
    this.hashCode = ArrayUtil.hashCode(buf, offset, length);
  }

  public static ByteString copy(byte[] buf, int offset, int length) {
    byte[] b;

    b = new byte[length];
    System.arraycopy(buf, offset, b, 0, length);
    return new ByteString(b, 0, length);
  }

  public static ByteString copy(byte[] buf) {
    return copy(buf, 0, buf.length);
  }

  public ByteString duplicateBuffer() {
    return copy(buf, offset, length);
  }

  public int length() {
    return length;
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public boolean equals(Object other) {
    ByteString o;

    o = (ByteString) other;
    if (o.hashCode() != hashCode()) {
      return false;
    } else {
      if (o == this) {
        return true;
      } else {
        if (length != o.length) {
          return false;
        } else {
          return ArrayUtil.equals(buf, offset, o.buf, o.offset, length);
        }
      }
    }
  }

  @Override
  public int compareTo(ByteString o) {
    return ArrayUtil.compareUnsigned(buf, offset, length, o.buf, o.offset, o.length);
  }

  @Override
  public String toString() {
    return new String(buf, offset, length);
  }
}
