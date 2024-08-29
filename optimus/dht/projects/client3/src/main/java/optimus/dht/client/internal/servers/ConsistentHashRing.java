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
package optimus.dht.client.internal.servers;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Sets;
import optimus.dht.client.api.Key;
import optimus.dht.client.api.hash.HashCalculator;
import optimus.dht.client.api.servers.ServerConnection;
import optimus.dht.client.api.servers.ServerRingPlacement;
import optimus.dht.common.util.DHTUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsistentHashRing {

  private static final Logger logger = LoggerFactory.getLogger(ConsistentHashRing.class);

  private final ConcurrentNavigableMap<Integer, ImmutableSortedSet<ServerConnection>> ring =
      new ConcurrentSkipListMap<>();

  private final ConcurrentMap<ServerConnection, ServerConnection> connections =
      new ConcurrentHashMap<>();

  private final int serverSlots;
  private final HashCalculator hashCalculator;
  private final ServerRingPlacement serverRingPlacement;

  public ConsistentHashRing(
      int serverSlots, HashCalculator hashCalculator, ServerRingPlacement serverRingPlacement) {
    this.serverSlots = serverSlots;
    this.hashCalculator = hashCalculator;
    this.serverRingPlacement = serverRingPlacement;
  }

  public void addServer(ServerConnection serverConnection) {
    String baseString = serverRingPlacement.ringPlacement(serverConnection);
    for (int i = 0; i < serverSlots; ++i) {
      byte[] slotStringBytes = (baseString + i).getBytes(StandardCharsets.ISO_8859_1);
      byte[] slotKey = hashCalculator.computeHash(slotStringBytes);
      Integer key = Integer.valueOf(DHTUtil.byteArrayToInt(slotKey));

      for (; ; ) {
        ImmutableSortedSet<ServerConnection> currentValue = ring.get(key);
        ImmutableSortedSet<ServerConnection> singletonSet = ImmutableSortedSet.of(serverConnection);

        if (currentValue == null) {
          if (ring.putIfAbsent(key, singletonSet) == null) {
            break;
          }
        } else {
          ImmutableSortedSet<ServerConnection> newSet =
              ImmutableSortedSet.copyOf(Sets.union(currentValue, singletonSet));
          if (ring.replace(key, currentValue, newSet)) {
            break;
          }
        }
      }
    }
    var existing = connections.putIfAbsent(serverConnection, serverConnection);
    logger.info(
        "Server added to the ring, server={}, ringSize={}, servers={}, existedBefore={}",
        serverConnection,
        ring.size(),
        connections.size(),
        existing != null);
  }

  public void removeServer(ServerConnection serverConnection) {
    String baseString = serverRingPlacement.ringPlacement(serverConnection);
    for (int i = 0; i < serverSlots; ++i) {
      byte[] slotStringBytes = (baseString + i).getBytes(StandardCharsets.ISO_8859_1);
      byte[] slotBytes = hashCalculator.computeHash(slotStringBytes);
      Integer key = Integer.valueOf(DHTUtil.byteArrayToInt(slotBytes));

      for (; ; ) {
        ImmutableSortedSet<ServerConnection> currentValue = ring.get(key);

        if (currentValue == null) {
          break;
        } else if (currentValue.size() == 1) {
          if (currentValue.contains(serverConnection)) {
            if (ring.remove(key, currentValue)) {
              break;
            }
          } else {
            break;
          }
        } else {
          ImmutableSortedSet<ServerConnection> singletonSet =
              ImmutableSortedSet.of(serverConnection);
          ImmutableSortedSet<ServerConnection> newSet =
              ImmutableSortedSet.copyOf(Sets.difference(currentValue, singletonSet));
          if (ring.replace(key, currentValue, newSet)) {
            break;
          }
        }
      }
    }
    var removed = connections.remove(serverConnection, serverConnection);
    logger.info(
        "Server removed from the ring, server={}, ringSize={}, servers={}, removed={}",
        serverConnection,
        ring.size(),
        connections.size(),
        removed);
  }

  @FunctionalInterface
  public interface IteratorFilter {
    boolean matches(ServerConnection serverConnection);
  }

  public Iterator<ServerConnection> serversForKey(Key key, IteratorFilter iteratorFilter) {
    Integer initialKey = Integer.valueOf(DHTUtil.byteArrayToInt(key.hash()));
    return new RingIterator(initialKey, iteratorFilter);
  }

  private enum RingIterationState {
    INIT,
    TAIL,
    HEAD,
    END
  }

  // Custom Iterator is 40% faster and produces 2.5x less garbage than the older solution based on
  // Stream API
  private class RingIterator implements Iterator<ServerConnection> {

    private final Integer initialKey;
    private final IteratorFilter iteratorFilter;

    private Set<ServerConnection> alreadyReturnedServers = new HashSet<>();
    private RingIterationState iterationState = RingIterationState.INIT;
    private Iterator<ImmutableSortedSet<ServerConnection>> activeIterator = null;
    private ServerConnection nextElement;

    public RingIterator(Integer initialKey, IteratorFilter iteratorFilter) {
      this.initialKey = initialKey;
      this.iteratorFilter = iteratorFilter;
    }

    @Override
    public boolean hasNext() {
      if (nextElement != null) {
        return true;
      }
      if (iterationState == RingIterationState.END) {
        return false;
      }
      for (; ; ) {
        if (activeIterator == null || !activeIterator.hasNext()) {
          switch (iterationState) {
            case INIT:
              activeIterator = ring.tailMap(initialKey, true).values().iterator();
              iterationState = RingIterationState.TAIL;
              break;
            case TAIL:
              activeIterator = ring.headMap(initialKey, false).values().iterator();
              iterationState = RingIterationState.HEAD;
              break;
            case HEAD:
              activeIterator = null;
              iterationState = RingIterationState.END;
              return false;
          }
        }

        while (activeIterator.hasNext()) {
          ServerConnection candidate = activeIterator.next().first();
          if (alreadyReturnedServers.add(candidate)) {
            if (iteratorFilter.matches(candidate)) {
              nextElement = candidate;
              return true;
            }
          }
        }
      }
    }

    @Override
    public ServerConnection next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      ServerConnection element = nextElement;
      nextElement = null;
      return element;
    }
  }
}
