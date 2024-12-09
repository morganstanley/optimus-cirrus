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

import java.lang.ref.WeakReference;

import optimus.graph.GraphException;
import optimus.graph.NodeTask;
import scala.collection.immutable.List;
import scala.collection.mutable.ListBuffer;

/**
 * Allows us to reach all cached items so we can invalidate them. Don't want to hold onto nodes for
 * longer than needed.
 */
public class TTrackRef {
  // TODO (OPTIMUS-64658): TTrackRef used to extend WeakReference<NodeTask> but doing this creates
  //  an excessive amount for the ReferenceHandler thread / GC and leads to TTracks never actually
  //  being cleared because they are held in the "discovered" field of the Reference. Switching to
  //  encapsulation instead of inheritance solves this, but I'm still not sure why.
  private final WeakReference<NodeTask> weakRef;

  public NodeTask get() {
    return weakRef.get();
  }

  // This method should be used in tests only because it doesn't correctly update the count on the
  // referenceCounter.
  void unsafeForceClear() {
    weakRef.clear();
  }

  // non-cacheable node or no NodeTasks tracked
  public static final TTrackRef Nil = new TTrackRef(null, null, null);
  public static final TTrackRef Invalid = new TTrackRef(null, null, null); // invalidated node

  static int size(TTrackRef ref) {
    int size = 0;
    while (ref != null) {
      ref = ref.nref;
      size++;
    }
    return size;
  }

  /** Next reference in a single linked list. */
  TTrackRef nref;

  @Override
  public String toString() {
    if (this == Nil) return "TTrackRef.Nil";
    if (this == Invalid) return "TTrackRef.Invalid";
    return "TTrackRef[" + get() + "]";
  }

  /**
   * Convert the internal single-linked list into an actual list, checking for cycles using tortoise
   * and hare algorithm. This is used for debugging only - exception message will report start and
   * length of cycle if there is one https://en.wikipedia.org/wiki/Cycle_detection
   *
   * @return A List of TTrackRefs formed by traversing nref, or throws exception if there is a cycle
   */
  List<TTrackRef> toList() {
    ListBuffer<TTrackRef> buffer = new ListBuffer<>();
    TTrackRef x0 = this;
    buffer.$plus$eq(x0);

    TTrackRef tortoise = x0.nref; // f(x0)
    if (tortoise == null) return buffer.toList(); // list of size 1, no cycle
    TTrackRef hare = x0.nref.nref; // f(f(x0))

    //  tortoise builds the list as it goes, and either we hit a null and return the list, or the
    // hare catches the
    //  tortoise and we do the extra analysis
    while (tortoise != hare) {
      if (hare == null
          || hare.nref
              == null) { // if hare has reached the end, no cycle, so let tortoise catch up and
        // return
        while (tortoise != null) {
          buffer.$plus$eq(tortoise);
          tortoise = tortoise.nref;
        }
        return buffer.toList();
      }
      buffer.$plus$eq(tortoise);
      // f(tortoise) - note, we know tortoise is behind hare so shouldn't be null
      tortoise = tortoise.nref;
      hare = hare.nref.nref; // f(f(hare)), hare moves twice as fast
    }

    // found a cycle, retrace steps to get the start and the length
    // (we know there's a cycle, ain't no more null checks here)
    int cycleStart = 0 /* mu */;
    tortoise = x0;
    while (tortoise != hare) {
      tortoise = tortoise.nref; // f(tortoise)
      hare = hare.nref; // f(hare), same speed
      cycleStart++;
    }

    int cycleLength = 1 /* lambda */;
    hare = tortoise.nref; // f(tortoise)
    while (tortoise != hare) {
      hare = hare.nref; // f(hare)
      cycleLength++;
    }

    String msg =
        String.format(
            "Unexpected cycle in TTrackRef nodes for %s (start = %d, length = %d)",
            this.toString(), cycleStart, cycleLength);
    throw new GraphException(msg);
  }

  /**
   * Initialize a TTrackRef.
   *
   * @param ntsk The NodeTask to reference.
   * @param nref The next reference is the single-linked list.
   * @param refQ The ReferenceQueue to use.
   */
  public TTrackRef(NodeTask ntsk, TTrackRef nref, RefCounter<NodeTask> refQ) {
    if (refQ == null) {
      this.weakRef = new WeakReference<>(ntsk);
    } else {
      this.weakRef = refQ.createWeakReference(ntsk);
    }
    this.nref = nref;
  }
}
