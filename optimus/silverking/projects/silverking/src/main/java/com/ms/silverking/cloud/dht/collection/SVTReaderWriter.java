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
package com.ms.silverking.cloud.dht.collection;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.ms.silverking.numeric.NumConversion;

public class SVTReaderWriter implements ValueTableReaderWriter {
  public static final int overheadBytes = NumConversion.BYTES_PER_INT;

  public SVTReaderWriter() {
  }

  @Override
  public int getSerializedSizeBytes(ValueTable vt) {
    SimpleValueTable svt;

    svt = (SimpleValueTable) vt;
    return overheadBytes + svt.getSizeBytes();
  }

  @Override
  public void write(ByteBuffer out, ValueTable vt) throws IOException {
    SimpleValueTable svt;

    svt = (SimpleValueTable) vt;
    assert svt.getKeys().length == svt.getValues().length;
    out.putInt(svt.getValues().length);
    for (long key : svt.getKeys()) {
      out.putLong(key);
    }
    for (int value : svt.getValues()) {
      out.putInt(value);
    }
  }

  @Override
  public ValueTable read(ByteBuffer in) throws IOException {
    int length;
    long[] keys;
    int[] values;

    length = in.getInt();
    keys = new long[length * 2];
    values = new int[length];
    for (int i = 0; i < length * 2; i++) {
      keys[i] = in.getLong();
    }
    for (int i = 0; i < length; i++) {
      values[i] = in.getInt();
    }
    return new SimpleValueTable(keys, values);
  }
}
