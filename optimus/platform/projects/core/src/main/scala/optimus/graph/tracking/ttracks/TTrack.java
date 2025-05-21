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

import optimus.graph.GraphInInvalidState;
import optimus.graph.NodeExtendedInfo;
import optimus.graph.NodeTask;
import optimus.graph.PropertyNode;
import optimus.graph.Settings;
import optimus.graph.cache.NCSupport;
import optimus.graph.tracking.DependencyTrackerSettings;
import optimus.graph.tracking.TraversalIdSource;
import optimus.platform.util.PrettyStringBuilder;

/*
 * Keeps the dependency tree from the tweakable item to all the cacheable nodes - really a shadow dependency tree
 *
 * Concurrency
 *
 * A TTrack has some operations that operate concurrently, and some that only run during certain phases of operation
 * of the tracking scenario
 *
 * A dependency tracker (or tree) is in one of the following states:
 *  - idle, or other states that don't affect TTracks
 *  - evaluating
 *  - tweaking
 *  - cleaning
 *
 * Can run at any time (readonly): toString
 *
 * Can run when tweaking (which is single threaded):
 *   - invalidate & invalidatePreview - only runs due to a addTweak, so only runs during tweaking
 *
 * Can run when evaluating (which is multi threaded):
 *   - merge
 *   - nodeCompleted
 *
 * Can run when cleaning (which is single threaded):
 *   - set
 *   - drop
 *   - compress
 *
 * Future direction and opportunities
 * =======================
 * 1. allow clean to run in parallel as we have multiple cores
 * 2. allow mini-clean cycles (just part of the graph)
 * 3. separate clean cycle from normal cycle id
 * 4. better scheduling control
 * 5. investigate if it is useful to preview a clean - determine what could be cleaned as a read only
 *    task that can be scheduled during evaluates. This may direct the clean, or just quantify the
 *    benefit of a future clean
 *
 * Note: TTrack implements PTracks --- it serves double duty as the 1-element specialized PTracks.
 * This is so that chains of singly linked tracks (a very common pattern) don't need extra objects
 * added in the middle. This is dangerous because it means that a random (not single caller) ttrack
 * can be interpreted as an incorrect ptrack!
 */
public class TTrack extends NodeExtendedInfo implements Ptracks {
  public TTrack() {
    ptracks = Ptracks0.instance;
  }

  /**
   * Create a TTrack with a set of nodes. The nodes are a set of TTrackRefs, connected by their
   * internal linked list structure.
   *
   * @param nodes Nodes to include in the TTrack.
   */
  public TTrack(TTrackRef nodes) {
    this.nodes = nodes;
    ptracks = Ptracks0.instance;
  }

  /**
   * Create a TTrack with a set of nodes and a given caller. The nodes are a set of TTrackRefs,
   * connected by their internal linked list structure.
   *
   * @param nodes - Nodes to include in the TTrack.
   * @param caller - the caller TTrack.
   */
  public TTrack(TTrackRef nodes, TTrack caller) {
    this.nodes = nodes;
    this.ptracks = caller;
  }

  /*
   * LOCKING STRATEGY
   * 1. mutable fields are only updated or observed with a sync lock on this
   * 2. no callback to user code with lock in place (except walk)
   * 3. lock ordering - children first. Never lock a caller if a child may already be locked
   */

  /**
   * This indicates the highest traversalID in which we visited this node. This is a way of
   * detecting whether we've seen this node before.
   */
  public int lastUpdateTraversalID;

  /*
   * List of cached completed node and some special state.
   *
   * null - node is not complete
   * TTrackRef.Nil - node is non-cacheable or complete but no nodes to be invalidated
   * TTrackRef.Invalid - node has been invalidated, should not be added to and should be pruned on cleanup
   *
   * otherwise this is the head of a singly-liked list of nodes weakrefs
   */
  TTrackRef nodes;

  /** Callers. Never null. */
  Ptracks ptracks;

  final void addCaller(TTrack ttrack, int rescan) {
    Ptracks initial = ptracks;
    while (!initial.addCaller(this, ttrack, rescan)) {
      initial = ptracks;
    }
  }

  final boolean callerCAS(Ptracks expectedCurrent, Ptracks replace) {
    return ptracks_h.compareAndSet(this, expectedCurrent, replace);
  }

  private static final VarHandle ptracks_h;
  private static final VarHandle nodes_h;

  static {
    try {
      var lookup = MethodHandles.lookup();
      ptracks_h = lookup.findVarHandle(TTrack.class, "ptracks", Ptracks.class);
      nodes_h = lookup.findVarHandle(TTrack.class, "nodes", TTrackRef.class);
    } catch (Exception e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /**
   * ntsk is the node that is about to use the result of childNode (with child = childNode.info).
   * ntsk.info == this
   */
  @Override
  public NodeExtendedInfo merge(NodeExtendedInfo child, NodeTask ntsk) {
    TTrack childtt = (TTrack) child;
    if (nodes == null) {
      // we own this ttrack only if the node is not complete (nodes == null)
      // this means that this ttrack is not any other node's xinfo, and no one can call addCaller on
      // it
      // (in particular, it means that this ttrack cannot also be childtt's child's xinfo)
      childtt.addCaller(this, DependencyTrackerSettings.PTRACK_DEFAULT_RESCAN);
      return this;
    } else {
      // We've been reusing child's TTrack and now we are combining with yet another child
      // In this case we create a new TTrack and link up the previous ttracks
      TTrack r = new TTrack(ntsk.isDone() ? TTrackRef.Nil : null);
      this.addCaller(r, 0);
      childtt.addCaller(r, 0);
      return r;
    }
  }

  @Override
  public void nodeCompleted(NodeTask ntsk, boolean isCancelled) {
    if (isCancelled) return;
    // Technically, we could do this only if the ntsk was actually cached as opposed to cacheable,
    // but then we wouldn't be invalidating nodes that are captured by user code. This may or may
    // not be an issue.

    // @formatter:off

    // Consider (diamond shaped compute)
    // def a = b + d + e
    // def b = c(1)
    // def d = c(2)
    // def e = c(3)
    // (tweak=true) def c(i: Int) = ...

    // 1. a, b , d are in progress
    // 2. as part of looking up c we will attach TTrack to c (xinfo). Notice that TTrack does not
    // point to c at this point!
    // 3. c completes and c.completed() which calls here
    //    [i] c.completed never runs in multiple threads
    //    [ii] xinfo.nodeCompleted(c) will be called in parallel in the diamond reuse of tweakable c
    //        (if the argument is the same).
    // 4. b gets notification that c completed and it will merge (aka NodeTask.combineInfo) the info
    // of c.
    //    [i] NodeTask.combineInfo will not call TTrack.merge at this point, xinfo starts out as
    // Default which combines to this TTrack
    // 5. b completes and this.nodeCompleted will reuse this TTrack
    // 4-5 of d and e are the same as for b
    // 6. a gets notification that b is completed and takes a value of this TTrack as part of
    // combine (no-merge called)
    // 7. a gets notification that d is completed and merge is called with
    // c_track(1).merge(c_track(2))
    //    [i] Note: that 6 and 7 are not concurrent even though 4-5 of b and d are!
    //    [ii] This will cause the creation of a_track and calls to c_track(1).addCaller(a_track)
    // and c_track(2).addCaller(a_track)
    //    [iii] Notice that a_track's nodes is null at this point! Not Nil
    // 8. a gets notification that e is completed and merge is called with a_track.merge(c_track(3))
    //    [i] Because a_track.nodes == null the only call we need to do is
    // c_track(3).addCaller(a_track)
    // @formatter:on

    if (ntsk.effectivelyCacheable()) {
      if (Settings.schedulerAsserts) {
        if (ntsk.isStable()) throw new GraphInInvalidState("Stable nodes should not be tracked!");
        if (ntsk.scenarioStack().isRecordingTweakUsage() && !NCSupport.isDelayedProxy(ntsk))
          throw new GraphInInvalidState("Recorded nodes should not be tracked!");
      }
      TTrackRef initial;
      TTrackRef result;
      // set the initial to Nil, not null, as this just makes the first cycle faster for normal
      // nodes

      // TODO (OPTIMUS-65703): This reference queue is null in some tests, which can cause memory
      //  leaks. We should assert that it is not null here.
      var refQ = ntsk.scenarioStack().tweakableListener().refQ();

      do {
        initial = nodes;
        if (initial == null || initial == TTrackRef.Nil) result = new TTrackRef(ntsk, null, refQ);
        else {
          // current can become null if we have a race and the original value was Nil, then it will
          // not be top posted we don't need this reference, so a NO-OP
          if (initial.get() == ntsk) return;
          // in case we lose the CAS and have to look again, we only need to check as far as the
          // initial node that we have checked already, the tail cannot mutate
          // as we only add to the head, except in GC, and GC cannot occur now
          result = new TTrackRef(ntsk, initial, refQ);
        }
        // atomically set value of 'this' at position 'offset' to 'result' if current
      } while (!nodes_h.weakCompareAndSet(this, initial, result));
      // value is 'initial'
    } else if (nodes == null) {
      // not concerned if we lose a race, it must be not null at the end of this call
      nodes_h.compareAndSet(this, null, TTrackRef.Nil);
    }
  }

  @Override
  public String toString() {
    return "TTrack@"
        + System.identityHashCode(this)
        + " cycle="
        + lastUpdateTraversalID
        + " nodes="
        + nodes
        + " sizeOfNodes = "
        + TTrackRef.size(nodes)
        + ", callers="
        + ptracks.currentSize();
  }

  static String nameOf(NodeTask node) {
    if (node == null) return "null";
    else if (node instanceof PropertyNode<?>
        && ((PropertyNode<?>) node).propertyInfo().entityInfo != null)
      return ((PropertyNode<?>) node).propertyInfo().entityInfo.runtimeClass().getSimpleName()
          + "."
          + ((PropertyNode<?>) node).propertyInfo().name()
          + "@"
          + node.scenarioStack().toShortString();
    else return node.executionInfo().name() + " " + node.toString();
  }

  private static String scnName(NodeTask node) {
    return ":" + node.scenarioStack().name();
  }

  public PrettyStringBuilder toString(
      final PrettyStringBuilder sb, final TraversalIdSource idSource) {
    new AllTTrackRefsGraphTraverser() {
      @Override
      public void preVisit(TTrack ttrack) {
        sb.startBlock();
        super.preVisit(ttrack);
      }

      @Override
      public void postVisit(TTrack ttrack) {
        sb.endBlock();
      }

      @Override
      protected void visitRef(NodeTask task, TTrackRef tref) {
        sb.appendln(nameOf(task) + scnName(task));
      }
    }.visit(this, idSource);
    return sb;
  }

  // Implementation for the PTracks interface:
  // -----------------------------------------
  @Override
  public int currentSize() {
    return 1;
  }

  @Override
  public int currentCapacity() {
    return 1;
  }

  @Override
  public boolean addCaller(TTrack owner, TTrack newCaller, int minRescanIgnored) {
    return newCaller == this || owner.callerCAS(this, new Ptracks2(this, newCaller));
  }

  @Override
  public TTrack at(int i) {
    return i == 0 ? this : null;
  }

  @Override
  public void set(int i, TTrack owner, Ptracks newValue) {
    owner.ptracks = newValue;
  }

  @Override
  public void drop(int i, TTrack owner) {
    if (i != 0) throw new IllegalArgumentException();
    owner.ptracks = Ptracks0.instance;
  }

  @Override
  public int compress(TTrack owner) {
    return 0;
  }
}

final class TTrackRoot extends TTrack {
  TTrackRoot() {
    super(TTrackRef.Nil);
  }
}
