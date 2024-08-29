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

public final class ArrayUtil {

  private ArrayUtil() {}

  public static long totalSize(byte[][] nestedArrays) {
    long size = 0;
    for (int i = 0; i < nestedArrays.length; ++i) {
      size += nestedArrays[i].length;
    }
    return size;
  }

  public static byte[][] createNestedArrays(long targetSize, int segmentSize) {
    int fullArrays = (int) (targetSize / segmentSize);
    int lastArraySize = (int) (targetSize % segmentSize);
    int arrayCount = lastArraySize == 0 ? fullArrays : fullArrays + 1;
    byte[][] nestedArrays = new byte[arrayCount][];
    for (int i = 0; i < fullArrays; ++i) {
      nestedArrays[i] = new byte[segmentSize];
    }
    if (lastArraySize != 0) {
      nestedArrays[fullArrays] = new byte[lastArraySize];
    }
    return nestedArrays;
  }

  public static byte[][] copyToNestedArrays(byte[] input, int segmentSize) {
    if (input.length <= segmentSize && input.length != 0) {
      return new byte[][] {input};
    }

    byte[][] nestedArrays = createNestedArrays(input.length, segmentSize);
    int inputPos = 0;
    for (byte[] nested : nestedArrays) {
      int len = nested.length;
      System.arraycopy(input, inputPos, nested, 0, len);
      inputPos += len;
    }
    assert (inputPos == input.length);
    return nestedArrays;
  }
}
