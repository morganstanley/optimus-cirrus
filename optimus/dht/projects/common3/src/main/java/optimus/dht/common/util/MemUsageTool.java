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
package optimus.dht.common.util;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

/** A simple utility class that helps with estimating memory usage. */
public class MemUsageTool {

  private static final Logger logger = LoggerFactory.getLogger(MemUsageTool.class);

  // Since Java 9, Strings that only use ISO-8859-1 chars are encoded using 1 byte per character.
  // But since there is no quick way to check if a string uses 1 byte per character encoder, we need
  // to check the
  // size of the underlying byte[] instead.
  private static final MethodHandle STRING_VALUE_HANDLE;

  static {
    try {
      Field field = String.class.getDeclaredField("value");
      field.setAccessible(true);
      STRING_VALUE_HANDLE = MethodHandles.lookup().unreflectGetter(field);
    } catch (Exception e) {
      throw new RuntimeException("Cannot create String.value MethodHandle", e);
    }
  }

  private static final Unsafe UNSAFE;

  static {
    if (MemUsageTool.class.getClassLoader() == null) {
      UNSAFE = Unsafe.getUnsafe();
    } else
      try {
        Field fld = Unsafe.class.getDeclaredField("theUnsafe");
        fld.setAccessible(true);
        UNSAFE = (Unsafe) fld.get(Unsafe.class);
      } catch (Exception e) {
        throw new RuntimeException("Could not obtain access to sun.misc.Unsafe", e);
      }
  }

  private static final long ARRAY_HEADER_SIZE = UNSAFE.ARRAY_BYTE_BASE_OFFSET;

  private static final long OBJECT_ALIGNMENT_M1 = 7;

  private static final long OOP_SIZE = UNSAFE.ARRAY_OBJECT_INDEX_SCALE;

  private static class JVMLayoutTest {
    private long firstAndOnlyField;
  }

  private static final long FIRST_FIELD_OFFSET;

  static {
    try {
      FIRST_FIELD_OFFSET =
          UNSAFE.objectFieldOffset(JVMLayoutTest.class.getDeclaredField("firstAndOnlyField"));
    } catch (Exception e) {
      throw new RuntimeException("Could not calculate object header size", e);
    }
  }

  private static final ImmutableMap<Class<?>, Integer> PRIMITIVE_SIZES =
      ImmutableMap.of(
          Boolean.TYPE,
          1,
          Byte.TYPE,
          1,
          Character.TYPE,
          2,
          Short.TYPE,
          2,
          Integer.TYPE,
          4,
          Float.TYPE,
          4,
          Long.TYPE,
          8,
          Double.TYPE,
          8);

  public static long instanceSize(Class clazz) {
    try {
      Field lastField = null;
      long lastFieldOffset = 0;

      for (Field field : clazz.getDeclaredFields()) {
        if (Modifier.isStatic(field.getModifiers())) {
          continue;
        }
        var offset = UNSAFE.objectFieldOffset(field);
        if (offset > lastFieldOffset) {
          lastFieldOffset = offset;
          lastField = field;
        }
      }

      long size;
      if (lastField == null) {
        size = FIRST_FIELD_OFFSET;
      } else {
        var fieldClass = lastField.getType();
        if (!fieldClass.isPrimitive()) {
          size = lastFieldOffset + OOP_SIZE;
        } else {
          Integer primitiveSize = PRIMITIVE_SIZES.get(fieldClass);
          if (primitiveSize == null) {
            throw new IllegalArgumentException("Unknown primitive type " + fieldClass);
          } else {
            size = lastFieldOffset + primitiveSize;
          }
        }
        size = roundUp(size);
      }

      logger.info("Calculated size of {} to be {}", clazz.getCanonicalName(), size);
      return size;
    } catch (Exception e) {
      throw new RuntimeException("Could not calculate the size of " + clazz, e);
    }
  }

  private static final long STRING_INSTANCE_SIZE = instanceSize(String.class);

  private static long roundUp(long size) {
    return (size + OBJECT_ALIGNMENT_M1) & ~OBJECT_ALIGNMENT_M1;
  }

  /**
   * Estimates memory used by a byte array.
   *
   * @param array input
   * @return estimate of memory used by a byte array
   */
  public static long bytesUsed(byte[] array) {
    return array != null ? ARRAY_HEADER_SIZE + roundUp(array.length) : 0;
  }

  /**
   * Estimates memory used by an array of byte arrays.
   *
   * @param array input
   * @return estimate of memory used by a byte array
   */
  public static long bytesUsed(byte[][] array) {
    if (array == null) {
      return 0;
    }
    long totalUsage = 0;
    for (byte[] bytes : array) {
      totalUsage += bytesUsed(bytes);
    }
    return bytesUsed((Object[]) array) + totalUsage;
  }

  /**
   * Estimates memory used by a char array.
   *
   * @param array input
   * @return estimate of memory used by a char array
   */
  public static long bytesUsed(char[] array) {
    return array != null ? ARRAY_HEADER_SIZE + roundUp(2 * array.length) : 0;
  }

  /**
   * Estimates memory used by a string.
   *
   * @param string input
   * @return estimate of memory used by a string
   */
  public static long bytesUsed(String string) {
    try {
      if (string == null) {
        return 0;
      } else if (string.isEmpty()) {
        // empty strings share the same empty byte[]
        return STRING_INSTANCE_SIZE;
      } else {
        return STRING_INSTANCE_SIZE
            + MemUsageTool.bytesUsed((byte[]) STRING_VALUE_HANDLE.invokeExact(string));
      }
    } catch (Throwable e) {
      throw new RuntimeException("Cannot measure String size", e);
    }
  }

  /**
   * Estimates memory used by an objects array.
   *
   * @param array input
   * @return estimate of memory used by an object array
   */
  public static <T> long bytesUsed(T[] array) {
    return array != null ? ARRAY_HEADER_SIZE + roundUp(OOP_SIZE * array.length) : 0;
  }
}
