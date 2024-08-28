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
package optimus.graph.tracking.ttracks;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;

/**
 * The caller of a TTrack, may be 0-n callers, and there are specialized implementations for
 * specific common counts to reduce memory load (0, 1, 2 callers).
 *
 * <p>See notes on TTrack for thread safety semantics.
 */
interface Ptracks {
  /**
   * @return the current size of this Ptrack, i.e. the number of Ttrack references it is currently
   *     holding
   */
  int currentSize();

  /**
   * @return the current capacity of this Ptrack, i.e. the maximum currentSize we could have without
   *     allocating memory
   */
  int currentCapacity();

  /**
   * Access caller at breadth i
   *
   * @param i must be between 0 (inclusive) and currentSize (exclusive)
   * @return the ttrack
   */
  TTrack at(int i);

  /**
   * Reset caller at i to point to newValue
   *
   * @param i - index
   * @param owner - only required for the case of a single TTrack (which has a different structure
   *     and points directly to a caller TTrack, rather than the Ptracks object)
   * @param newValue - this is usually a TTrack unless we are trying to compress down to Ptracks0 or
   *     Ptracks2
   */
  void set(int i, TTrack owner, Ptracks newValue);

  /**
   * Drop caller at breadth i (and update structure to compress down to smaller Ptracks
   * implementations)
   *
   * @param i - index
   * @param owner - only required for the case of a single TTrack
   */
  void drop(int i, TTrack owner);

  /**
   * If Ptracks contains null, we might be able to compress to Ptracks0 or Ptracks2
   *
   * @param owner - only required for the case of a single TTrack
   * @return the reduction in capacity as a result of the compression
   */
  int compress(TTrack owner);

  /**
   * attempt to add the caller Implementation note - this is a highly multi-threaded call, and
   * should run with minimal contention and no locking
   *
   * @param owner the caller
   * @param newCaller to add
   * @param rescan the minimum number of values to rescan. if the caller asserts that newCaller
   *     cannot exist in this, then this should be 0 if the caller wants to ensure that a full scan
   *     or all elements occurs then Integer.MAX_VALUE is appropriate
   * @return true if successful false if unsuccessful, and effected no change, and the caller should
   *     retry with the same parameters
   */
  boolean addCaller(TTrack owner, TTrack newCaller, int rescan);
}

/** specialised implementation for no callers */
final class Ptracks0 implements Ptracks {

  private Ptracks0() {}

  static final Ptracks0 instance = new Ptracks0();

  @Override
  public boolean addCaller(TTrack owner, TTrack newCaller, int minRescanIgnored) {
    return owner.callerCAS(this, newCaller);
  }

  @Override
  public int currentSize() {
    return 0;
  }

  @Override
  public int currentCapacity() {
    return 0;
  }

  @Override
  public TTrack at(int i) {
    return null;
  }

  @Override
  public void set(int i, TTrack owner, Ptracks newValue) {
    throw new IllegalArgumentException("Cannot set Ptracks0");
  }

  @Override
  public void drop(int i, TTrack owner) {}

  @Override
  public int compress(TTrack owner) {
    return 0;
  }
}

abstract class AbstractMultiPtracks implements Ptracks {
  protected final String debugString(String name, TTrack... callers) {
    StringBuilder sb = new StringBuilder();
    sb.append(name).append("[");
    for (TTrack caller : callers) {
      if (caller != null) sb.append(System.identityHashCode(caller)).append(" ");
    }
    sb.append(name).append("[");
    return sb.toString();
  }
}

/**
 * 2 field implementation of Ptracks Design guarantee - fields p0,p1 are never null, except during
 * cleanup if one is null following this operation the caller TTrack's Ptrack will be changed so as
 * not to use this value
 */
final class Ptracks2 extends AbstractMultiPtracks {

  private TTrack p0;
  private TTrack p1;

  public Ptracks2(TTrack p0, TTrack p1) {
    super();
    this.p0 = p0;
    this.p1 = p1;
  }

  @Override
  public int currentSize() {
    return 2;
  }

  @Override
  public int currentCapacity() {
    return 2;
  }

  @Override
  public boolean addCaller(TTrack owner, TTrack newCaller, int minRescanIgnored) {
    return newCaller == p0
        || newCaller == p1
        || owner.callerCAS(this, new PtracksArray(p0, p1, newCaller));
  }

  @Override
  public TTrack at(int i) {
    if (i == 0) return p0;
    else if (i == 1) return p1;
    else return null;
  }

  @Override
  public void set(int i, TTrack owner, Ptracks newValue) {
    // this is inelegant but lets us replace the Ptracks2 callers individually with a single TTrack
    // this should never be called with anything other than a TTrack newValue
    if (i == 0) p0 = newValue.at(0);
    else if (i == 1) p1 = newValue.at(0);
    else throw new IllegalArgumentException();
  }

  @Override
  public void drop(int i, TTrack owner) {
    if (i == 0) p0 = p1;
    p1 = null;
  }

  @Override
  public int compress(TTrack owner) {
    if (p0 == null)
      if (p1 == null) {
        owner.set(0, owner, Ptracks0.instance);
        return 2;
      } else {
        owner.set(0, owner, p1);
        return 1;
      }
    else if (p1 == null) {
      owner.set(0, owner, p0);
      return 1;
    } else {
      return 0;
    }
  }
}

/**
 * Array implementation of Ptracks Design guarantee - elements lower that nextFree are never null,
 * except during cleanup elements above nextFree may be null elements < 3 are never null if one is
 * null following this operation the caller TTracks Ptrack will be changed so as not to use this
 * value
 */
class PtracksArray extends AbstractMultiPtracks {
  private static final VarHandle data_h;
  private static final VarHandle dataElm_h;
  /**
   * special mode for unit tests which will resize the array linearly rather than exponentially -
   * this means that resizing is much more frequent which stresses the resizing code - never use in
   * production
   */
  static boolean LINEAR_GROWTH = false;

  static {
    var lookup = MethodHandles.lookup();
    try {
      data_h = lookup.findVarHandle(PtracksArray.class, "data", TTrack[].class);
      dataElm_h = MethodHandles.arrayElementVarHandle(TTrack[].class);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  // only updated via unsafe CAS, but needs to be volatile for the reads
  private volatile TTrack[] data;
  /** the next free slot in the array - all previous slots will always be non-null */
  private int nextFree;

  private static final int INITIAL_CAPACITY = 8;

  PtracksArray(TTrack p0, TTrack p1, TTrack p2) {
    super();
    data = new TTrack[INITIAL_CAPACITY];
    data[0] = p0;
    data[1] = p1;
    data[2] = p2;
    nextFree = 3;
  }

  @Override
  public boolean addCaller(TTrack owner, TTrack newCaller, int minRescan) {
    // read nextFree first, because if nextFree is lagging behind data we're safe but if it's ahead
    // then we can leave
    // gaps in data array
    int nextFree = this.nextFree;
    TTrack[] data = this.data;

    int start = Math.max(0, nextFree - minRescan);
    // Help with silly code ie. if(a > 3) a + 1 else a + 3 (in this case we find our caller in the
    // Ptracks already)
    for (int i = start; i < nextFree; i++) {
      if (data[i] == newCaller) return true;
    }
    while (true) {
      // If nextFree is past the end of our view of the array, we need to get hold of a bigger
      // array...
      if (nextFree >= data.length) {
        // (optimization - read it again to avoid allocating a new array if someone already resized
        // it)
        data = this.data;
        // If nextFree is past the end of the array, try to double the size. If we lose the race
        // it's fine because someone
        // else must have increased the size, so just refetch the array. Note that when we refetch
        // the array we might
        // still not see a sufficiently large array (e.g. we could lose the CAS from size 8 to 16,
        // but nextFree is up to
        // 32 and we reread the array and see it at size 16 while someone else is already updating
        // it to 32), so we might
        // need to try a few times.
        while (nextFree >= data.length) {
          int newLength = data.length * 2;
          // (only true in special test mode)
          if (LINEAR_GROWTH) newLength = data.length + 1;
          // note that data will have no nulls (because we don't leave any gaps) and therefore
          // nobody else can still be
          // writing values in, so it's effectively immutable at this point and therefore our copy
          // can't be stale unless
          // we lose the CAS
          TTrack[] newData = Arrays.copyOf(data, newLength);
          data_h.compareAndSet(this, data, newData);
          // refetch the array in case we lost the race - eventually we or someone else will have
          // grown it past nextFree
          data = this.data;
        }
      }

      // try to add at this position. We'll only fail if someone else added here, in which case they
      // will have incremented nextFree so we'll try another position next time around the loop
      if (dataElm_h.compareAndSet(data, nextFree, null, newCaller)) {
        // note that we only get to this line if we were able to assign in to the slot at nextFree
        // so our view of nextFree is definitely not stale (and nobody else can have written at
        // > nextFree yet because they haven't seen the write we're about to do, so there is no risk
        // of this write happening after some greater write)
        this.nextFree = nextFree + 1;
        return true;
      }
      // note that we don't re-read data here, because it can only have changed if nextFree is past
      // the end of data in which case we will re-read data at the top of the while loop
      nextFree = this.nextFree;
    }
  }

  @Override
  public int currentSize() {
    return nextFree;
  }

  @Override
  public int currentCapacity() {
    return data.length;
  }

  @Override
  public TTrack at(int i) {
    TTrack[] snap = data;
    return (i >= 0 && snap.length > i) ? snap[i] : null;
  }

  // this should never be called with anything other than a TTrack newValue
  @Override
  public void set(int i, TTrack owner, Ptracks newValue) {
    data[i] = newValue.at(0);
  }

  @Override
  public void drop(int i, TTrack owner) {
    data[i] = data[--nextFree];
    data[nextFree] = null;
  }

  @Override
  public int compress(TTrack owner) {
    // compress if we have zero, one or two non-null values
    if (nextFree <= 2) {
      if (nextFree == 0) {
        owner.set(0, owner, Ptracks0.instance);
        return data.length;
      } else if (nextFree == 1) {
        owner.set(0, owner, data[0]);
        return data.length - 1;
      } else {
        owner.set(0, owner, new Ptracks2(data[0], data[1]));
        return data.length - 2;
      }
    } else if (nextFree <= data.length / 2 && data.length > INITIAL_CAPACITY) {
      int originalLength = data.length;
      // shrink the array length to the smallest power of two greater than or equal to nextFree and
      // INITIAL_CAPACITY
      int oneBit = Integer.highestOneBit(nextFree);
      // (oneBit is the largest power of two less than or equal to nextFree)
      int newLength = Math.max(INITIAL_CAPACITY, oneBit == nextFree ? oneBit : oneBit * 2);
      if (newLength != data.length) {
        //noinspection NonAtomicOperationOnVolatileField
        data = Arrays.copyOf(data, newLength);
        return originalLength - data.length;
      }
    }

    return 0;
  }
}
