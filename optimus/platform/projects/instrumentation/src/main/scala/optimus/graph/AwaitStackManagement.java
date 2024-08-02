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
package optimus.graph;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import sun.misc.Unsafe;

public class AwaitStackManagement {

  private static final sun.misc.Unsafe U;

  static {
    if (AwaitStackManagement.class.getClassLoader() == null) U = Unsafe.getUnsafe();
    else
      try {
        Field fld = Unsafe.class.getDeclaredField("theUnsafe");
        fld.setAccessible(true);
        U = (Unsafe) fld.get(Unsafe.class);
      } catch (Exception e) {
        throw new RuntimeException("Could not obtain access to sun.misc.Unsafe", e);
      }
  }

  private AwaitableContext ec;

  /*  Memory layout in C++ of thread-local awaitData block:
            long methodID
            long sampledSignalToSet;   <-- our pointer, set in AwaitStackManagement constructor
            long sampledSignal;
            long stackId[MAX_AWAIT_STACKS+1];
  */

  private final long awaitData;

  private void putAwaitData(int i, long v) {
    U.putLong(awaitData + i * 8, v);
  }

  AwaitStackManagement(AwaitableContext ec) {
    long ad = AsyncProfilerIntegration.getAwaitDataAddress();
    U.putLong(ad, runChainMethodID);
    awaitData = ad + 8;
    this.ec = ec;

    // TODO (OPTIMUS-55070): Remove with _savedAwaitStacks race condition is fixed in profiler.cpp
    // For now, save a frame gratuitously in this thread to assure that flightRecorder.cpp sees the
    // flag.
    work[0] = overflowMarker;
    work[1] = 0;
    AsyncProfilerIntegration.saveAwaitFrames(work, 1);
  }

  // Leaky map of await stacks that have already been saved.
  private static final int SAVED_SLOTS_BITS = 10;
  private static final int SAVED_SLOTS = 1 << SAVED_SLOTS_BITS;
  private static final long SAVED_MASK = (long) SAVED_SLOTS - 1;
  private final long[] AS1 = new long[SAVED_SLOTS];
  private final long[] AS2 = new long[SAVED_SLOTS];
  private static final int CUCKOO_ITERATIONS = 5;
  private static final long MEMRESETSEC =
      DiagnosticSettings.getIntProperty("optimus.async.profiler.cache.reset.sec", 600);
  private static final long MEMRESETNS = 1000L * 1000L * 1000L * MEMRESETSEC;
  private long stackMemResetTime = 0; // last time we cleared the saved stack maps on this thread

  // Storage for reversing the await stack prior to saving into async-profiler
  private static final int MAX_CHAIN_IDX =
      DiagnosticSettings.getIntProperty("optimus.async.profiler.max.chain", 250);
  private final long[] work = new long[MAX_CHAIN_IDX + 2];

  // JMethodID that async-profiler will replace with an launcher hash id
  private static long runChainMethodID = 0;
  private static long overflowMarker = 0;
  private static String overflowName = "[overflow]";

  static {
    if (AsyncProfilerIntegration.ensureLoadedIfEnabled()) {
      runChainMethodID =
          AsyncProfilerIntegration.getMethodID(
              AwaitStackManagement.class, "runChain", "(Loptimus/graph/Awaitable;)V");
      overflowMarker = AsyncProfilerIntegration.saveString(overflowName);
    }
  }

  // Launcher stack hash id that will replace the insertion id
  boolean savedIfNecessary = false;

  /** Abstract the caching of char* pointers representing different T */
  private abstract static class StringPointers<T> {
    private long[] pointers;

    StringPointers(int initialSize) {
      pointers = new long[initialSize];
    }

    protected abstract String getName(T t);

    protected abstract int getId(T t);

    long get(T t) {
      var id = getId(t);
      long[] ps = pointers;
      if (id < ps.length && ps[id] != 0) return ps[id];
      else
        synchronized (this) {
          if (id >= pointers.length) pointers = Arrays.copyOf(ps, Math.max(ps.length * 2, id * 2));
          var name = getName(t);
          var p = AsyncProfilerIntegration.saveString(name);
          pointers[id] = p;
          return p;
        }
    }
  }

  // Cache the awaitable names weakly by Awaitable identity...
  private static Cache<Awaitable, String> awaitable2Name =
      Caffeine.newBuilder().maximumSize(10 * 1000).weakKeys().build();
  /// ... and also intern.
  private static Cache<String, String> frameNameInterner =
      Caffeine.newBuilder().maximumSize(10 * 1000).build();

  private static class StackKey {
    final long hash;
    final String extraMods;

    StackKey(long hash, String extraMods) {
      this.hash = hash;
      this.extraMods = extraMods;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      StackKey stackKey = (StackKey) o;
      return hash == stackKey.hash && extraMods.equals(stackKey.extraMods);
    }

    @Override
    public int hashCode() {
      return Objects.hash(hash, extraMods);
    }
  }

  private static Cache<StackKey, ArrayList<String>> awaitStackCache =
      Caffeine.newBuilder().maximumSize(10 * 1000).build();

  private static String frameName(Awaitable awaitable) {
    return frameName(awaitable, "");
  }

  private static String frameName(Awaitable awaitable, String extraMods) {
    return awaitable2Name.get(
        awaitable, n -> frameNameInterner.get(awaitable.flameFrameName(extraMods), x -> x));
  }

  private static StringPointers<Awaitable> namePtrs =
      new StringPointers<>(10000) {
        @Override
        protected String getName(Awaitable awaitable) {
          return frameName(awaitable, "");
        }

        @Override
        protected int getId(Awaitable awaitable) {
          return awaitable.getProfileId();
        }
      };

  private static String elisionName(int i) {
    return "[elided:" + i + "]";
  }

  private static StringPointers<Integer> elisionPtrs =
      new StringPointers<>(10) {
        @Override
        protected String getName(Integer i) {
          return elisionName(i);
        }

        @Override
        protected int getId(Integer i) {
          return i;
        }
      };

  private boolean isAlreadySaved(long hash) {
    // Maybe we've already saved this hash, in which case we want to avoid bothering async-profiler.
    // Cuckoo-hash-like: Nomenclature is a bit wonky, since what we're storing is in fact a hash (of
    // await stacks),
    // and we're storing it in hash tables.  We'll use the lowest and next-lowest SAVED_SLOT_BITS as
    // bin numbers.
    maybeClearSavedAwaitStacks();
    long h = hash, c;
    int i1 = (int) (h & SAVED_MASK), i2;
    if (h == AS1[i1] || h == AS2[(int) ((h >> SAVED_SLOTS_BITS) & SAVED_MASK)]) {
      savedIfNecessary = true;
      return true;
    }
    // Store the new hash in S1, possibly expelling current occupant, which we would store in S2,
    // possibly expelling
    // its current occupant, etc.  Give up after a while; worst case is that we end up storing too
    // many times.
    for (int j = 0; j < CUCKOO_ITERATIONS; j++) {
      if ((c = AS1[i1]) == 0) {
        AS1[i1] = h;
        break;
      } // Done if no current occupant in S1.
      AS1[i1] = h;
      h = c; // Push out current occupant of S1
      // Position in S2 for expelled occupant of S1
      i2 = (int) ((h >> SAVED_SLOTS_BITS) & SAVED_MASK);
      if ((c = AS2[i2]) == 0) {
        AS2[i2] = h;
        break;
      } // Done if no current occupant in S2.
      AS2[i2] = h;
      h = c; // Push out current occupant of S2
      i1 = (int) (h & SAVED_MASK); // Position in S1 for expelled occupant of S2
    }

    return false;
  }

  public void maybeSaveAwaitStack(Awaitable awaitable) {
    if (savedIfNecessary) return;

    // If sampling occurred, will have been set to our taskId
    long signal = U.getLong(awaitData + 8);
    if (signal != awaitable.getId()) return;

    savedIfNecessary = true;

    // Construct await frame stacks to save in async-profiler.  The current Launcher path may
    // contain multiple
    // such stacks, separated by java frames, which are indicated by a bit in the hash.
    int i = 0, len = 0;
    Awaitable n = awaitable;
    long prevName = 0;
    do {
      long h = n.getLauncherStackHash();
      if (i == 0) {
        // We're at the innermost awaitable of the current stack, either because we just started, or
        // because
        // we hit a java boundary.
        if (isAlreadySaved(h)) break;
        work[0] = h;
        // next element will be the inner-most awaitable
        i = 1;
      }
      long name;
      Awaitable parent = n.getLauncher();
      // The proxy name is the same, except already terminating in "~$"
      while (parent != null && parent != n && parent.elideChildFrame()) {
        n = parent;
        parent = n.getLauncher();
      }
      boolean isJavaCallBoundary = (h & 1L) == 1L;
      // skip over repeated entries, since chains can be very long
      int recursion = 0;
      while ((name = namePtrs.get(n)) == prevName
          && parent != null
          && parent != n
          && !isJavaCallBoundary) {
        recursion++;
        n = parent;
        parent = n.getLauncher();
        h = n.getLauncherStackHash();
        isJavaCallBoundary = (h & 1L) == 1L;
      }
      if (recursion > 0 && i < MAX_CHAIN_IDX - 2) {
        if (recursion > 1) work[i++] = elisionPtrs.get(recursion - 1);
        work[i++] = prevName;
      }
      prevName = name;
      work[i++] = name;
      // Terminate this stack if...
      if (parent == null
          || // we're at the end of the stack,
          parent == n
          || // we're in an infinite loop
          isJavaCallBoundary
          || // we've hit a java point
          i == MAX_CHAIN_IDX) { // or if there's no more room.
        if (i == MAX_CHAIN_IDX) work[i++] = overflowMarker;
        len = i;
        i = 0;
        work[len] = 0;
        AsyncProfilerIntegration.saveAwaitFrames(work, len);
      }
      n = parent;
      // Technically, even if we've hit the maximum chain that a-p can ingest, it's still possible
      // that
      // we'll find further chains on the other side of a java frame, but it's simpler to just give
      // up.
    } while (n != null && len <= MAX_CHAIN_IDX);
  }

  public static ArrayList<String> awaitStack(long hash) {
    if (hash == 0) return null;
    else return awaitStackCache.getIfPresent(new StackKey(hash, ""));
  }

  // The logic is similar to maybeSaveAwaitStacks above, but attempts at refactoring them to share
  public static ArrayList<String> awaitStack(Awaitable awaitable) {
    return awaitStack(awaitable, "");
  }

  public static ArrayList<String> awaitStack(Awaitable awaitable, String extraMods) {
    return awaitStackCache.get(
        new StackKey(awaitable.stackId(), extraMods),
        k -> {
          ArrayList<String> ret = new ArrayList<>();
          Awaitable n = awaitable;
          String prevName = frameName(awaitable, extraMods);
          ret.add(prevName);
          n = n.getLauncher();
          while (Objects.nonNull(n)) {
            long h = n.getLauncherStackHash();
            String name;
            Awaitable parent = n.getLauncher();
            // The proxy name is the same, except already terminating in "~$"
            while (parent != null && parent != n && parent.elideChildFrame()) {
              n = parent;
              parent = n.getLauncher();
            }
            boolean isJavaCallBoundary = (h & 1L) == 1L;
            // skip over repeated entries, since chains can be very long
            int recursion = 0;
            while ((name = frameName(n)).equals(prevName)
                && parent != null
                && parent != n
                && !isJavaCallBoundary) {
              recursion++;
              n = parent;
              parent = n.getLauncher();
              h = n.getLauncherStackHash();
              isJavaCallBoundary = (h & 1L) == 1L;
            }
            if (recursion > 0) {
              if (recursion > 1) ret.add(elisionName(recursion - 1));
              ret.add(prevName);
            }
            prevName = name;
            ret.add(name);
            if (isJavaCallBoundary) ret.add("[java]");

            if (parent == n) {
              ret.add("[infinite]");
              n = null;
            } else if (ret.size() == MAX_CHAIN_IDX) {
              ret.add(overflowName);
              n = null;
            } else {
              n = parent;
            }
          }
          return ret;
        });
  }

  private void maybeClearSavedAwaitStacks() {
    if (stackMemResetTime < AsyncProfilerIntegration.globalStackMemResetTime()
        || TestableClock.nanoTime() > stackMemResetTime + MEMRESETNS) {
      for (int j = 0; j < SAVED_SLOTS; j++) {
        AS1[j] = AS2[j] = 0;
      }
      stackMemResetTime = TestableClock.nanoTime();
    }
  }

  private static int MAX_AWAIT_STACKS = 10; // matches profiler.h
  private long[] currentHashes = new long[MAX_AWAIT_STACKS + 1];
  private int[] currentTaskIds = new int[MAX_AWAIT_STACKS + 1];
  private long[] insertionIds = new long[MAX_AWAIT_STACKS + 1];
  private int irh = -1;

  // This gets called before every awaitable is run.  It does only writes into thread-specific
  // memory
  // and in particular no JNI.
  private void beforeRun(Awaitable awaitable, long insertionID) {
    long hash = awaitable.getLauncherStackHash();
    int taskId = awaitable.getId();
    int a = 0;
    // If we're sampled, a-p will put this taskId into the next field.
    putAwaitData(a++, taskId);
    putAwaitData(a++, 0);

    irh += 1;
    if (irh < MAX_AWAIT_STACKS) {
      currentHashes[irh] = hash;
      currentTaskIds[irh] = taskId;
      insertionIds[irh] = insertionID;
      // Now fill the stackId array with hashes from each java frame-delimited section
      for (int i = 0; i <= irh; i++) putAwaitData(a++, currentHashes[irh - i]);
      // and terminate with zero
      putAwaitData(a++, 0);
    } else {
      putAwaitData(a++, hash);
      putAwaitData(a++, 0);
    }

    savedIfNecessary = false;
  }

  private void afterRun(Awaitable awaitable) {
    maybeSaveAwaitStack(awaitable);
    irh -= 1;
    int a = 0;
    if (irh >= 0 && irh < MAX_AWAIT_STACKS) {
      putAwaitData(a++, currentTaskIds[irh]);
      putAwaitData(a++, 0);
      for (int i = 0; i <= irh; i++) putAwaitData(a++, currentHashes[irh - i]);
      putAwaitData(a++, 0);
    } else {
      putAwaitData(a++, 0);
      putAwaitData(a++, 0);
    }
    savedIfNecessary = false;
  }

  void runChain(Awaitable awaitable) {
    beforeRun(awaitable, runChainMethodID);
    try {
      awaitable.launch(ec);
    } finally {
      afterRun(awaitable);
    }
  }

  private static final long LONG_PHI = 0x9E3779B97F4A7C15L;
  // From https://github.com/leventov/Koloboke via https://github.com/vigna/fastutil and
  // re-implemented locally for clarity
  private static long mix(final long x) {
    long h = x * LONG_PHI;
    h ^= h >>> 32;
    return h ^ (h >>> 16);
  }

  private static long combine(final long hash, final long rhs) {
    switch (DiagnosticSettings.awaitChainHashStrategy) {
      case 0:
        // boost::hash_combine but where rhs is fibonacci-mixed
        return hash ^ (mix(rhs) + LONG_PHI + (hash << 6) + (hash >> 2));
      case 1:
        // old-style, xor with the mixed rhs
        return hash ^ mix(rhs);
      case 3:
        // boost::hash_combine
        return hash ^ (rhs + LONG_PHI + (hash << 6) + (hash >> 2));
      case 4:
        // fibonacci mix the xor - analogous to what std::hash_append
        // would do if native long hash were Koloboke
        return mix(hash ^ rhs);
    }
    throw new RuntimeException(
        "Unknown hash combine strategy " + DiagnosticSettings.awaitChainHashStrategy);
  }

  private static final long baseStackHash = mix(TestableClock.nanoTime());
  private static final long javaCallStackHash = ~baseStackHash;

  public static void setLaunchData(
      Awaitable fromTask, Awaitable toTask, boolean inJavaStack, int implFlags) {
    if (Objects.isNull(fromTask)) return; // redundant, but might be called from cache methods
    Awaitable fromUnder = fromTask.underlyingAwaitable();
    if (DiagnosticSettings.repairLauncherChain && fromUnder == toTask) fromUnder = fromTask;

    // compute stack hash
    long hash = fromUnder.getLauncherStackHash();
    if (hash == 0L) hash = baseStackHash;
    hash = combine(hash, toTask.getProfileId());
    if (inJavaStack) {
      hash = combine(hash, javaCallStackHash) | 1L; // java boundary stack hashes are odd
    } else hash &= ~1L; // normal hashes are even
    toTask.setLaunchData(fromUnder, hash, implFlags);
  }
}
