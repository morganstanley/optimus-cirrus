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

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.base.Splitter;
import optimus.dht.common.api.transport.CorruptedStreamException;
import io.netty.buffer.ByteBuf;
import io.netty.util.internal.PlatformDependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DHTUtil {

  private static final Logger logger = LoggerFactory.getLogger(DHTUtil.class);

  private static final Path PUBLIC_CLOUD_MARKER = Paths.get("/etc/cloud/cloud-marker");

  private static final String IP_DISCOVERY_BASIC = "basic";
  private static final String IP_DISCOVERY_PROVIDED = "provided:";
  private static final String IP_DISCOVERY_ROUTE = "route:";

  private static final boolean HAS_UNSAFE_AND_UNALIGNED =
      PlatformDependent.hasUnsafe() && PlatformDependent.isUnaligned();

  private static final long EPOCH_NANO_OFFSET;

  static {
    var nanoTime = System.nanoTime();
    var instant = Instant.now();
    var nanosSinceEpoch = TimeUnit.SECONDS.toNanos(instant.getEpochSecond()) + instant.getNano();
    EPOCH_NANO_OFFSET = nanoTime - nanosSinceEpoch;
  }

  public static int byteArrayToInt(byte[] array) {
    if (array.length < 4) {
      int ret = 0;
      for (int i = 0; i < array.length; ++i) {
        ret = (ret << 8) | (array[i] & 0xff);
      }
      return Integer.reverseBytes(ret);
    } else if (HAS_UNSAFE_AND_UNALIGNED) {
      int v = PlatformDependent.getInt(array, 0);
      return PlatformDependent.BIG_ENDIAN_NATIVE_ORDER ? Integer.reverseBytes(v) : v;
    } else {
      return (array[0] & 0xff)
          | (array[1] & 0xff) << 8
          | (array[2] & 0xff) << 16
          | (array[3] & 0xff) << 24;
    }
  }

  public static String getFQDN() {
    try {
      return InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
      logger.warn("Unable to obtain server's FQDN", e);
      return "UNKNOWN";
    }
  }

  public static String getLocalOutgoingAddress(String method) {
    if (method.equals(IP_DISCOVERY_BASIC)) {
      try {
        return InetAddress.getLocalHost().getHostAddress();
      } catch (IOException e) {
        throw new RuntimeException("Unable to discover host's outgoing IP address", e);
      }
    } else if (method.startsWith(IP_DISCOVERY_PROVIDED)) {
      return method.substring(IP_DISCOVERY_PROVIDED.length());
    } else if (method.startsWith("route:")) {
      try {
        try (DatagramSocket socket = new DatagramSocket()) {
          // we don't actually try to connect here (UDP is connectionless)
          // if host has multiple external IP addresses, the code below will return the one used to
          // communicate
          // with the provided address
          socket.connect(
              InetAddress.getByName(method.substring(IP_DISCOVERY_ROUTE.length())), 32768);
          return socket.getLocalAddress().getHostAddress();
        }
      } catch (IOException e) {
        throw new RuntimeException("Unable to discover host's outgoing IP address", e);
      }
    } else {
      throw new IllegalArgumentException("Unsupported IP discovery method: " + method);
    }
  }

  public static String getCloudName() {
    String treadmillId = getTreadmillId();
    return treadmillId != null ? treadmillId : getPublicCloudName();
  }

  private static String getPublicCloudName() {
    if (Files.exists(PUBLIC_CLOUD_MARKER)) {
      String fqdn = getFQDN();
      return Splitter.on('.').split(fqdn).iterator().next();
    }
    return null;
  }

  private static String getTreadmillId() {
    // note: TM2 and TM3 use different envvars

    final String cell = System.getenv("TREADMILL_CELL");
    if (cell == null) {
      return null; // no treadmill
    }

    final String name = System.getenv("TREADMILL_NAME");
    if (name != null) {
      // TM2
      return cell + "/" + name;
    } else {
      // TM3
      return cell
          + "/"
          + System.getenv("TREADMILL_APP")
          + "#"
          + System.getenv("TREADMILL_INSTANCEID");
    }
  }

  public static String randomAlphanumeric(int length) {
    Random random = new Random();
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; ++i) {
      int nextInt = random.nextInt(62);
      if (nextInt < 10) {
        sb.append((char) ('0' + nextInt));
      } else if (nextInt < 36) {
        sb.append((char) ('A' + nextInt - 10));
      } else {
        sb.append((char) ('a' + nextInt - 36));
      }
    }
    return sb.toString();
  }

  public static void checkPrefix(String expectedString, ByteBuf input) {
    byte[] expected = expectedString.getBytes(StandardCharsets.US_ASCII);
    int toCheck = Math.min(expected.length, input.readableBytes());
    for (int i = 0; i < toCheck; ++i) {
      if (expected[i] != input.getByte(i)) {
        throw new CorruptedStreamException(
            "Expected "
                + expectedString
                + " but on position "
                + i
                + " received "
                + input.getByte(i)
                + " (expected "
                + expected[i]
                + ")");
      }
    }
  }

  public static <T, S> T findDuplicate(
      Collection<? extends T> collection, Function<T, S> extractor) {
    Set<S> elements = new HashSet<>(collection.size());
    for (T element : collection) {
      if (!elements.add(extractor.apply(element))) {
        return element;
      }
    }
    return null;
  }

  public static long nanosSinceEpoch(long nanoTime) {
    return nanoTime - EPOCH_NANO_OFFSET;
  }

  public static Instant instantFromNanos(long nanos) {
    return Instant.EPOCH.plus(nanos, ChronoUnit.NANOS);
  }
}
