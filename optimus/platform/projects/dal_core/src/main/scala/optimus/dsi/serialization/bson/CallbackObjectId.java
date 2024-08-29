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

/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Enumeration;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
// TODO (OPTIMUS-57177): license needs reformatting in order to publish this code as open-source
/**
 * A globally unique identifier for objects.
 *
 * <p>Consists of 12 bytes, divided as follows:
 *
 * <table border="1">
 *     <caption>ObjectID layout</caption>
 *     <tr>
 *         <td>0</td><td>1</td><td>2</td><td>3</td><td>4</td><td>5</td><td>6</td><td>7</td><td>8</td><td>9</td><td>10</td><td>11</td>
 *     </tr>
 *     <tr>
 *         <td colspan="4">time</td><td colspan="3">machine</td> <td colspan="2">pid</td><td colspan="3">inc</td>
 *     </tr>
 * </table>
 *
 * <p>Instances of this class are immutable. This is a simplified version (less methods) of the
 * Mongo driver class, moved here to break the dependency on the driver jar.
 */
public class CallbackObjectId implements Comparable<CallbackObjectId>, java.io.Serializable {

  private static final long serialVersionUID = -4415279469780082174L;

  static final Logger LOGGER = Logger.getLogger(CallbackObjectId.class.getName());

  /**
   * Gets a new object id.
   *
   * @return the new id
   */
  public static CallbackObjectId get() {
    return new CallbackObjectId();
  }

  /**
   * Creates an CallbackObjectId using time, machine and inc values. The Java driver used to create
   * all ObjectIds this way, but it does not match the <a
   * href="http://docs.mongodb.org/manual/reference/object-id/">CallbackObjectId specification</a>,
   * which requires four values, not three. This major release of the Java driver conforms to the
   * specification, but still supports clients that are relying on the behavior of the previous
   * major release by providing this explicit factory method that takes three parameters instead of
   * four.
   *
   * <p>Ordinary users of the driver will not need this method. It's only for those that have
   * written there own BSON decoders.
   *
   * <p>NOTE: This will not break any application that use ObjectIds. The 12-byte representation
   * will be round-trippable from old to new driver releases.
   *
   * @param time time in seconds
   * @param machine machine ID
   * @param inc incremental value
   * @return a new {@code CallbackObjectId} created from the given values
   * @since 2.12.0
   */
  public static CallbackObjectId createFromLegacyFormat(
      final int time, final int machine, final int inc) {
    return new CallbackObjectId(time, machine, inc);
  }

  /**
   * Checks if a string could be an {@code CallbackObjectId}.
   *
   * @param s a potential CallbackObjectId as a String.
   * @return whether the string could be an object id
   * @throws IllegalArgumentException if hexString is null
   */
  public static boolean isValid(String s) {
    if (s == null) return false;

    final int len = s.length();
    if (len != 24) return false;

    for (int i = 0; i < len; i++) {
      char c = s.charAt(i);
      if (c >= '0' && c <= '9') continue;
      if (c >= 'a' && c <= 'f') continue;
      if (c >= 'A' && c <= 'F') continue;

      return false;
    }

    return true;
  }

  /**
   * Turn an object into an {@code CallbackObjectId}, if possible. Strings will be converted into
   * {@code CallbackObjectId}s, if possible, and {@code CallbackObjectId}s will be cast and
   * returned. Passing in {@code null} returns {@code null}.
   *
   * @param o the object to convert
   * @return an {@code CallbackObjectId} if it can be massaged, null otherwise
   */
  private static CallbackObjectId massageToObjectId(Object o) {
    if (o == null) return null;

    if (o instanceof CallbackObjectId) return (CallbackObjectId) o;

    if (o instanceof String) {
      String s = o.toString();
      if (isValid(s)) return new CallbackObjectId(s);
    }

    return null;
  }

  /**
   * Constructs a new instance using the given date.
   *
   * @param time the date
   */
  public CallbackObjectId(Date time) {
    this(time, _genmachine, _nextInc.getAndIncrement());
  }

  /**
   * Constructs a new instances using the given date and counter.
   *
   * @param time the date
   * @param inc the counter
   * @throws IllegalArgumentException if the high order byte of counter is not zero
   */
  public CallbackObjectId(Date time, int inc) {
    this(time, _genmachine, inc);
  }

  /**
   * Constructs an CallbackObjectId using time, machine and inc values. The Java driver has done it
   * this way for a long time, but it does not match the <a
   * href="http://docs.mongodb.org/manual/reference/object-id/">CallbackObjectId specification</a>,
   * which requires four values, not three. The next major release of the Java driver will conform
   * to this specification, but will still need to support clients that are relying on the current
   * behavior. To that end, this constructor is now deprecated in favor of the more explicit factory
   * method CallbackObjectId#createFromLegacyFormat(int, int, int)}, and in the next major release
   * this constructor will be removed.
   *
   * @param time the date
   * @param machine the machine identifier
   * @param inc the counter
   * @see CallbackObjectId#createFromLegacyFormat(int, int, int)
   */
  private CallbackObjectId(Date time, int machine, int inc) {
    _time = (int) (time.getTime() / 1000);
    _machine = machine;
    _inc = inc;
    _new = false;
  }

  /**
   * Creates a new instance from a string.
   *
   * @param s the string to convert
   * @throws IllegalArgumentException if the string is not a valid id
   */
  public CallbackObjectId(String s) {
    this(s, false);
  }

  /**
   * Constructs a new instance of {@code CallbackObjectId} from a string.
   *
   * @param s the string representation of CallbackObjectId. Can contains only [0-9]|[a-f]|[A-F]
   *     characters.
   * @param babble if {@code true} - convert to 'babble' objectId format
   */
  private CallbackObjectId(String s, boolean babble) {

    if (!isValid(s)) throw new IllegalArgumentException("invalid CallbackObjectId [" + s + "]");

    if (babble) s = babbleToMongod(s);

    byte b[] = new byte[12];
    for (int i = 0; i < b.length; i++) {
      b[i] = (byte) Integer.parseInt(s.substring(i * 2, i * 2 + 2), 16);
    }
    ByteBuffer bb = ByteBuffer.wrap(b);
    _time = bb.getInt();
    _machine = bb.getInt();
    _inc = bb.getInt();
    _new = false;
  }

  /**
   * Constructs an CallbackObjectId given its 12-byte binary representation.
   *
   * @param b a byte array of length 12
   */
  public CallbackObjectId(byte[] b) {
    if (b.length != 12) throw new IllegalArgumentException("need 12 bytes");
    ByteBuffer bb = ByteBuffer.wrap(b);
    _time = bb.getInt();
    _machine = bb.getInt();
    _inc = bb.getInt();
    _new = false;
  }

  /**
   * Constructs an CallbackObjectId using time, machine and inc values. The Java driver has done it
   * this way for a long time, but it does not match the <a
   * href="http://docs.mongodb.org/manual/reference/object-id/">CallbackObjectId specification</a>,
   * which requires four values, not three. The next major release of the Java driver will conform
   * to this specification, but we will still need to support clients that are relying on the
   * current behavior. To that end, this constructor is now deprecated in favor of the more explicit
   * factory method CallbackObjectId#createFromLegacyFormat(int, int, int)}, and in the next major
   * release this constructor will be removed.
   *
   * @param time time in seconds
   * @param machine machine ID
   * @param inc incremental value
   * @see CallbackObjectId#createFromLegacyFormat(int, int, int)
   */
  CallbackObjectId(int time, int machine, int inc) {
    _time = time;
    _machine = machine;
    _inc = inc;
    _new = false;
  }

  /** Create a new object id. */
  public CallbackObjectId() {
    _time = (int) (System.currentTimeMillis() / 1000);
    _machine = _genmachine;
    _inc = _nextInc.getAndIncrement();
    _new = true;
  }

  @Override
  public int hashCode() {
    int x = _time;
    x += (_machine * 111);
    x += (_inc * 17);
    return x;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;

    CallbackObjectId other = massageToObjectId(o);
    if (other == null) return false;

    return _time == other._time && _machine == other._machine && _inc == other._inc;
  }

  /**
   * Converts this instance into a 24-byte hexadecimal string representation.
   *
   * @return a string representation of the CallbackObjectId in hexadecimal format
   */
  public String toHexString() {
    final StringBuilder buf = new StringBuilder(24);

    for (final byte b : toByteArray()) {
      buf.append(String.format("%02x", b & 0xff));
    }

    return buf.toString();
  }

  /**
   * Convert to a byte array. Note that the numbers are stored in big-endian order.
   *
   * @return the byte array
   */
  public byte[] toByteArray() {
    byte b[] = new byte[12];
    ByteBuffer bb = ByteBuffer.wrap(b);
    // by default BB is big endian like we need
    bb.putInt(_time);
    bb.putInt(_machine);
    bb.putInt(_inc);
    return b;
  }

  static String _pos(String s, int p) {
    return s.substring(p * 2, (p * 2) + 2);
  }

  private static String babbleToMongod(String b) {
    if (!isValid(b)) throw new IllegalArgumentException("invalid object id: " + b);

    StringBuilder buf = new StringBuilder(24);
    for (int i = 7; i >= 0; i--) buf.append(_pos(b, i));
    for (int i = 11; i >= 8; i--) buf.append(_pos(b, i));

    return buf.toString();
  }

  public String toString() {
    byte b[] = toByteArray();
    StringBuilder buf = new StringBuilder(24);
    for (int i = 0; i < b.length; i++) {
      int x = b[i] & 0xFF;
      String s = Integer.toHexString(x);
      if (s.length() == 1) buf.append("0");
      buf.append(s);
    }
    return buf.toString();
  }

  int _compareUnsigned(int i, int j) {
    long li = 0xFFFFFFFFL;
    li = i & li;
    long lj = 0xFFFFFFFFL;
    lj = j & lj;
    long diff = li - lj;
    if (diff < Integer.MIN_VALUE) return Integer.MIN_VALUE;
    if (diff > Integer.MAX_VALUE) return Integer.MAX_VALUE;
    return (int) diff;
  }

  public int compareTo(CallbackObjectId id) {
    if (id == null) return -1;

    int x = _compareUnsigned(_time, id._time);
    if (x != 0) return x;

    x = _compareUnsigned(_machine, id._machine);
    if (x != 0) return x;

    return _compareUnsigned(_inc, id._inc);
  }

  /**
   * Gets the timestamp (number of seconds since the Unix epoch).
   *
   * @return the timestamp
   */
  public int getTimestamp() {
    return _time;
  }

  /**
   * Gets the timestamp as a {@code Date} instance.
   *
   * @return the Date
   */
  public Date getDate() {
    return new Date(_time * 1000L);
  }

  final int _time;
  final int _machine;
  final int _inc;

  boolean _new;

  private static AtomicInteger _nextInc = new AtomicInteger((new java.util.Random()).nextInt());

  private static final int _genmachine;

  static {
    try {
      // build a 2-byte machine piece based on NICs info
      int machinePiece;
      {
        try {
          StringBuilder sb = new StringBuilder();
          Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
          while (e.hasMoreElements()) {
            NetworkInterface ni = e.nextElement();
            sb.append(ni.toString());
          }
          machinePiece = sb.toString().hashCode() << 16;
        } catch (Throwable e) {
          // exception sometimes happens with IBM JVM, use random
          LOGGER.log(Level.WARNING, e.getMessage(), e);
          machinePiece = (new Random().nextInt()) << 16;
        }
        LOGGER.fine("machine piece post: " + Integer.toHexString(machinePiece));
      }

      // add a 2 byte process piece. It must represent not only the JVM but the class loader.
      // Since static var belong to class loader there could be collisions otherwise
      final int processPiece;
      {
        int processId = new java.util.Random().nextInt();
        try {
          processId =
              java.lang.management.ManagementFactory.getRuntimeMXBean().getName().hashCode();
        } catch (Throwable t) {
        }

        ClassLoader loader = CallbackObjectId.class.getClassLoader();
        int loaderId = loader != null ? System.identityHashCode(loader) : 0;

        StringBuilder sb = new StringBuilder();
        sb.append(Integer.toHexString(processId));
        sb.append(Integer.toHexString(loaderId));
        processPiece = sb.toString().hashCode() & 0xFFFF;
        LOGGER.fine("process piece: " + Integer.toHexString(processPiece));
      }

      _genmachine = machinePiece | processPiece;
      LOGGER.fine("machine : " + Integer.toHexString(_genmachine));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
