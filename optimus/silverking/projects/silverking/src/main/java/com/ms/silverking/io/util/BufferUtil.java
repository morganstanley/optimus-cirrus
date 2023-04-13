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
package com.ms.silverking.io.util;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

public class BufferUtil {
  public static ByteBuffer[] flip(ByteBuffer[] buffers) {
    for (ByteBuffer buffer : buffers) {
      buffer.flip();
    }
    return buffers;
  }

  public static List<ByteBuffer> flip(List<ByteBuffer> buffers) {
    for (ByteBuffer buffer : buffers) {
      //System.out.println("f\t"+ buffer);
      buffer.flip();
    }
    //System.out.println();
    return buffers;
  }

  public static Collection<ByteBuffer> flip(Collection<ByteBuffer> buffers) {
    for (ByteBuffer buffer : buffers) {
      buffer.flip();
    }
    return buffers;
  }

  public static int totalRemaining(Buffer[] buffers) {
    int totalRemaining;

    totalRemaining = 0;
    for (Buffer buffer : buffers) {
      totalRemaining += buffer.remaining();
    }
    return totalRemaining;
  }

  public static boolean equals(ByteBuffer bufA, int startA, byte[] arrayB, int startB, int length) {
    // FUTURE - optimize
    //for (int i = 0; i < length; i++) {
    //    System.out.println(bufA.get(startA + i) +"\t"+ arrayB[startB + i]);
    //}
    for (int i = 0; i < length; i++) {
      if (bufA.get(startA + i) != arrayB[startB + i]) {
        return false;
      }
    }
    return true;
  }

  private static boolean withinLimit(ByteBuffer buf, int start, int length) {
    return start + length <= buf.limit();
  }

  public static boolean equals(ByteBuffer bufA, int startA, ByteBuffer bufB, int startB, int length) {
    if (!withinLimit(bufA, startA, length) || !withinLimit(bufB, startB, length)) {
      return false;
    } else {
      for (int i = 0; i < length; i++) {
        if (bufA.get(startA + i) != bufB.get(startB + i)) {
          return false;
        }
      }
    }
    return true;
  }

  public static void get(ByteBuffer src, int srcOffset, byte[] dst, int dstOffset, int length) {
    for (int i = 0; i < length; i++) {
      dst[dstOffset + i] = src.get(srcOffset + i);
    }
  }

  public static void get(ByteBuffer src, int srcOffset, byte[] dst, int length) {
    get(src, srcOffset, dst, 0, length);
  }

  public static void get(ByteBuffer src, int srcOffset, byte[] dst) {
    get(src, srcOffset, dst, dst.length);
  }

  public static byte[] arrayCopy(ByteBuffer src, int offset, int length) {
    byte[] dst;

    dst = new byte[length];
    src = src.duplicate();
    src.position(offset);
    src.get(dst);
    return dst;
  }

  public static byte[] arrayCopy(ByteBuffer src, int length) {
    byte[] dst;

    dst = new byte[length];
    src.get(dst);
    return dst;
  }

  public static byte[] arrayCopy(ByteBuffer src) {
    return arrayCopy(src, src.remaining());
  }

  public static ByteBuffer convertToArrayBacked(ByteBuffer buf) {
    if (buf.hasArray()) {
      throw new RuntimeException("Already array backed");
    } else {
      return ByteBuffer.wrap(arrayCopy(buf, buf.position(), buf.limit() - buf.position()));
    }
  }

  public static ByteBuffer ensureArrayBacked(ByteBuffer buf) {
    if (buf.hasArray()) {
      return buf;
    } else {
      return convertToArrayBacked(buf);
    }
  }

  /**
   * Byte order preserving duplicate
   *
   * @param buf
   * @return
   */
  public static ByteBuffer duplicate(ByteBuffer buf) {
    return buf.duplicate().order(buf.order());
  }

  /**
   * Byte order preserving slice
   *
   * @param buf
   * @return
   */
  public static ByteBuffer slice(ByteBuffer buf) {
    return buf.slice().order(buf.order());
  }

  public static ByteBuffer sliceAt(ByteBuffer buf, int newPosition) {
    return ((ByteBuffer) buf.duplicate().position(newPosition)).slice().order(buf.order());
  }

  public static ByteBuffer sliceRange(ByteBuffer buf, int newPosition, int newLimit) {
    return ((ByteBuffer) buf.duplicate().position(newPosition).limit(newLimit)).slice().order(buf.order());
  }
}
