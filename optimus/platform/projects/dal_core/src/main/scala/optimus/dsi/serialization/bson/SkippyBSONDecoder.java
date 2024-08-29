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
import java.util.*;

/**
 * Implementation of BSONDecoder interface that is a version modified from BasicBSONObject
 *
 * <p>It is optimised to scan off a set of specified top level field values and nothing else.
 *
 * <p>This approach could be expanded to extract the minimum info for entitlements without a full
 * deserialize
 */
public class SkippyBSONDecoder extends ThriftyBSONDecoder {

  private final Set<String> fieldNameSet;
  private final Map<String, Object> results;

  public SkippyBSONDecoder(String... fieldNames) {
    fieldNameSet = new HashSet<>(Arrays.asList(fieldNames));
    results = new HashMap<>(fieldNameSet.size());
  }

  public Map<String, Object> scanFields(byte[] bytes) throws IOException {

    _in = new BSONInput(bytes);

    if (_in.numRead() != 0)
      throw new IllegalArgumentException("stale BSONInput has non-zero number of bytes read");

    try {

      final int len = _in.readInt();

      _in.setMax(len);

      while (decodeElement(false))
        ;

      return results;
    } finally {
      _in = null;
    }
  }

  boolean decodeElement(boolean parentSkip) throws IOException {

    final byte type = _in.read();

    if (type == BSONTypeIds.Eoo) return false;

    String name = null;
    if (parentSkip) {
      _in.skipCStr();
    } else {
      name = _in.readCStr();
    }

    boolean skip = parentSkip || !fieldNameSet.contains(name) || results.containsKey(name);

    switch (type) {
      case BSONTypeIds.Null:
        break;

      case BSONTypeIds.Undefined:
        break;

      case BSONTypeIds.Boolean:
        if (skip) _in.skip();
        else results.put(name, _in.read() > 0);
        break;

      case BSONTypeIds.Number:
        if (skip) _in.skip(8);
        else results.put(name, _in.readDouble());
        break;

      case BSONTypeIds.NumberInt:
        if (skip) _in.skip(4);
        else results.put(name, _in.readInt());
        break;

      case BSONTypeIds.NumberLong:
        if (skip) _in.skip(8);
        else results.put(name, _in.readLong());
        break;

      case BSONTypeIds.Symbol:
        if (skip) _in.skipSized();
        else results.put(name, _in.readUTF8String());
        break;

      case BSONTypeIds.String:
        if (skip) _in.skipSized();
        else results.put(name, _in.readUTF8String());
        break;

      case BSONTypeIds.Oid:
        _in.skip(12);
        break;

      case BSONTypeIds.Ref:
        _in.skipSized();
        _in.skip(12);
        break;

      case BSONTypeIds.Date:
        _in.skip(8);
        break;

      case BSONTypeIds.Regex:
        _in.skipCStr();
        _in.skipCStr();
        break;

      case BSONTypeIds.Binary:
        if (skip) {
          skipBinary();
        } else {
          results.put(name, readBinary());
        }
        break;

      case BSONTypeIds.Array:
        _in.skip(4);
        while (decodeElement(true)) {}
        break;

      case BSONTypeIds.Object:
        _in.skip(4);
        while (decodeElement(true)) {}
        break;

      case BSONTypeIds.Timestamp:
        _in.skip(8);
        break;

      case BSONTypeIds.MinKey:
        break;

      case BSONTypeIds.MaxKey:
        break;

      default:
        throw new UnsupportedOperationException(
            "BSONDecoder doesn't understand type : " + type + " name: " + name);
    }

    return fieldNameSet.size() != results.size();
  }

  protected byte[] readBinary() throws IOException {
    final int totalLen = _in.readInt();
    final byte bType = _in.read();

    switch (bType) {
      case BSONTypeIds.BinGeneral:
        {
          final byte[] data = new byte[totalLen];
          _in.fill(data);
          return data;
        }
      case BSONTypeIds.BinBinary:
        final int len = _in.readInt();
        if (len + 4 != totalLen)
          throw new IllegalArgumentException(
              "bad data size subtype 2 len: " + len + " totalLen: " + totalLen);

        final byte[] data = new byte[len];
        _in.fill(data);
        return data;
      case BSONTypeIds.BinUuid:
        throw new IllegalArgumentException("SkippyBSONDecoder doesn't support reading uuids");
    }

    final byte[] data = new byte[totalLen];
    _in.fill(data);

    return data;
  }

  protected void skipBinary() throws IOException {
    final int totalLen = _in.readInt();
    final byte bType = _in.read();

    switch (bType) {
      case BSONTypeIds.BinBinary:
        final int len = _in.readInt();
        _in.skip(len);
      case BSONTypeIds.BinUuid:
        _in.skip(16);
      default:
        _in.skip(totalLen);
    }
  }
}
