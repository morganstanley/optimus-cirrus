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

/**
 * Implementation of BSONDecoder interface that is a version modified from BasicBSONObject
 *
 * <p>It is optimised to read directly from a byte array so avoids any unnecessary buffering of
 * input.
 *
 * <p>It also uses a different string buffering strategy that relies on a thread local buffer
 */
public class ThriftyBSONDecoder {

  private ThriftyStringBuffer _stringBuffer = new ThriftyStringBuffer();

  protected BSONInput _in;

  private BSONCallback _callback;

  static final String DEFAULT_ENCODING = "UTF-8";

  public int decode(byte[] b, BSONCallback callback) {
    try {
      return _decode(new BSONInput(b), callback);
    } catch (IOException ioe) {
      throw new IllegalStateException("should be impossible", ioe);
    }
  }

  private int _decode(BSONInput in, BSONCallback callback) throws IOException {

    if (_in != null || _callback != null) throw new IllegalStateException("not ready");

    _in = in;
    _callback = callback;

    if (in.numRead() != 0)
      throw new IllegalArgumentException("stale BSONInput has non-zero number of bytes read");

    try {

      final int len = _in.readInt();

      _in.setMax(len);

      _callback.objectStart();
      while (decodeElement())
        ;
      _callback.objectDone();

      if (_in.numRead() != len)
        throw new IllegalArgumentException(
            "bad data.  lengths don't match read:" + _in.numRead() + " != len:" + len);

      return len;
    } finally {
      _in = null;
      _callback = null;
    }
  }

  boolean decodeElement() throws IOException {

    final byte type = _in.read();

    if (type == BSONTypeIds.Eoo) return false;

    String name = _in.readCStr();

    switch (type) {
      case BSONTypeIds.Null:
        _callback.gotNull(name);
        break;

      case BSONTypeIds.Boolean:
        _callback.gotBoolean(name, _in.read() > 0);
        break;

      case BSONTypeIds.Number:
        _callback.gotDouble(name, _in.readDouble());
        break;

      case BSONTypeIds.NumberInt:
        _callback.gotInt(name, _in.readInt());
        break;

      case BSONTypeIds.NumberLong:
        _callback.gotLong(name, _in.readLong());
        break;

      case BSONTypeIds.String:
        _callback.gotString(name, _in.readUTF8String());
        break;

      case BSONTypeIds.Oid:
        // Oid is stored as big endian
        _callback.gotObjectId(
            name, new CallbackObjectId(_in.readIntBE(), _in.readIntBE(), _in.readIntBE()));
        break;

      case BSONTypeIds.Binary:
        _binary(name);
        break;

      case BSONTypeIds.Array:
        _in.readInt(); // total size - we don't care....

        _callback.arrayStart(name);
        while (decodeElement())
          ;
        _callback.arrayDone();

        break;

      case BSONTypeIds.Object:
        _in.readInt(); // total size - we don't care....

        _callback.objectStart(name);
        while (decodeElement())
          ;
        _callback.objectDone();

        break;

      default:
        throw new UnsupportedOperationException(
            "BSONDecoder doesn't understand type : " + type + " name: " + name);
    }

    return true;
  }

  protected void _binary(final String name) throws IOException {
    final int totalLen = _in.readInt();
    final byte bType = _in.read();

    switch (bType) {
      case BSONTypeIds.BinBinary:
        final int len = _in.readInt();
        if (len + 4 != totalLen)
          throw new IllegalArgumentException(
              "bad data size subtype 2 len: " + len + " totalLen: " + totalLen);

        final byte[] dataBin = new byte[len];
        _in.fill(dataBin);
        _callback.gotBinary(name, bType, dataBin);
        break;

      default:
        final byte[] data = new byte[totalLen];
        _in.fill(data);
        _callback.gotBinary(name, bType, data);
    }
  }

  protected class BSONInput {

    private int _pos;
    final byte[] _raw;

    public BSONInput(final byte[] in) {
      _raw = in;
      _pos = 0;
    }

    /**
     * ensure that there are num bytes to read _pos is where to start reading from
     *
     * @return where to start reading from
     */
    protected int _need(final int num) throws IOException {
      int oldPos = _pos;
      _pos += num;
      return oldPos;
    }

    public int readInt() throws IOException {
      return Bits.readInt(_raw, _need(4));
    }

    public int readIntBE() throws IOException {
      return Bits.readIntBE(_raw, _need(4));
    }

    public long readLong() throws IOException {
      return Bits.readLong(_raw, _need(8));
    }

    public double readDouble() throws IOException {
      return Double.longBitsToDouble(readLong());
    }

    public void skip() {
      _pos++;
    }

    public void skip(int numBytes) {
      _pos += numBytes;
    }

    public void skipSized() throws IOException {
      final int size = readInt();
      _pos += size;
    }

    public byte read() throws IOException {
      return _raw[_pos++];
    }

    public void fill(byte b[]) throws IOException {
      fill(b, b.length);
    }

    public void fill(byte b[], int len) throws IOException {
      System.arraycopy(_raw, _pos, b, 0, len);
      _pos += len;
    }

    public void fill(ThriftyStringBuffer lsb, int len) throws IOException {
      lsb.fill(_raw, _pos, len);
      _pos += len;
    }

    public void skipCStr() throws IOException {
      while (read() != 0)
        ;
    }

    public String readCStr() throws IOException {

      // short circuit 1 byte strings
      byte first = read();
      if (first == 0) {
        return "";
      }

      byte second = read();
      if (second == 0) {
        final String out = ONE_BYTE_STRINGS[first];
        if (out != null) {
          return out;
        } else {
          byte[] newOne = new byte[1];
          newOne[0] = first;
          return new String(newOne, 0, 1, DEFAULT_ENCODING);
        }
      }

      _stringBuffer.reset();
      _stringBuffer.write(first);
      _stringBuffer.write(second);

      byte b;
      while ((b = read()) != 0) {
        _stringBuffer.write(b);
      }
      return _stringBuffer.get();
    }

    public String readUTF8String() throws IOException {
      final int size = readInt();
      // this is just protection in case it's corrupted, to avoid huge strings
      if (size <= 0) throw new IllegalStateException("bad string size: " + size);

      if (size == 1) {
        read(); // read off the string terminator
        return "";
      } else {
        fill(_stringBuffer, size - 1);
        read(); // read off the string terminator
        return _stringBuffer.getUTF8();
      }
    }

    public int numRead() {
      return _pos;
    }

    public int getPos() {
      return _pos;
    }

    public int getMax() {
      return _max;
    }

    public void setMax(int _max) {
      this._max = _max;
    }

    int _max = 4; // max number of total bytes allowed to ready
  }

  static boolean _isAscii(final byte b) {
    return b >= 0 && b <= 127;
  }

  static final String[] ONE_BYTE_STRINGS = new String[128];

  static void _fillRange(byte min, byte max) {
    while (min < max) {
      String s = "";
      s += (char) min;
      ONE_BYTE_STRINGS[(int) min] = s;
      min++;
    }
  }

  static {
    _fillRange((byte) '0', (byte) '9');
    _fillRange((byte) 'a', (byte) 'z');
    _fillRange((byte) 'A', (byte) 'Z');
  }
}
