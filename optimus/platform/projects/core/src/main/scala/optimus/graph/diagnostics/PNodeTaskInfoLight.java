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
package optimus.graph.diagnostics;

import java.io.Serial;
import java.io.Serializable;

/* ~ half the size of full PNTI (108 bytes compared to 201) */
public class PNodeTaskInfoLight implements Serializable {
  @Serial private static final long serialVersionUID = 1L;
  public transient int id;
  public long start; // Number of starts

  public long cacheTime; // Time spent on lookup logic
  public long cacheHit; // Number of cache hits
  // Number of cache hits from different tasks that ran on this engine
  public long cacheHitFromDifferentTask;
  public long cacheMiss; // Number of cache misses
  // Node Used Time = selfAndANCTime that would be computed without ANY caching
  // aka Time 'saved by caching' (over counted by 1 of selfAndANCTime)
  public long nodeUsedTime;
  public long selfTime; // Total Self Time
  public long ancAndSelfTime; // Total absorbed non-cached self time + selfTime
  // Time between complete/suspend and stop (e.g notify waiters & process xinfo)
  public long postCompleteAndSuspendTime;
  public long tweakLookupTime; // Time spent on looking up tweaks of this type

  /* required for hotspotsLight, but not pgo mode */
  // Number of (hash) collisions encountered when looking up NodeTasks in NodeCache
  public int collisionCount;
  public long evicted;
  public int invalidated;
  public int childNodeLookupCount;
  public long childNodeLookupTime;

  public PNodeTaskInfoLight(int id) {
    this.id = id;
  }

  public void merge(PNodeTaskInfoLight t) {
    if (id == 0) id = t.id;
    start += t.start;
    selfTime += t.selfTime;
    cacheTime += t.cacheTime;
    cacheHit += t.cacheHit;
    cacheHitFromDifferentTask += t.cacheHitFromDifferentTask;
    cacheMiss += t.cacheMiss;
    tweakLookupTime += t.tweakLookupTime;
    nodeUsedTime += t.nodeUsedTime;
    ancAndSelfTime += t.ancAndSelfTime;
    postCompleteAndSuspendTime += t.postCompleteAndSuspendTime;
    collisionCount += t.collisionCount;
    evicted += t.evicted;
    invalidated += t.invalidated;
    childNodeLookupCount += t.childNodeLookupCount;
    childNodeLookupTime += t.childNodeLookupTime;
  }

  protected void reset() {
    start = 0;
    selfTime = 0;
    cacheTime = 0;
    cacheHit = 0;
    cacheHitFromDifferentTask = 0;
    cacheMiss = 0;
    tweakLookupTime = 0;
    nodeUsedTime = 0;
    ancAndSelfTime = 0;
    postCompleteAndSuspendTime = 0;
    collisionCount = 0;
    evicted = 0;
    invalidated = 0;
    childNodeLookupCount = 0;
    childNodeLookupTime = 0;
  }

  /** compare fields - for use in tests */
  public boolean fieldsEqual(PNodeTaskInfoLight other) {
    if (this == other) return true;
    else
      return start == other.start
          && selfTime == other.selfTime
          && cacheTime == other.cacheTime
          && cacheHit == other.cacheHit
          && cacheHitFromDifferentTask == other.cacheHitFromDifferentTask
          && cacheMiss == other.cacheMiss
          && tweakLookupTime == other.tweakLookupTime
          && nodeUsedTime == other.nodeUsedTime
          && ancAndSelfTime == other.ancAndSelfTime
          && postCompleteAndSuspendTime == other.postCompleteAndSuspendTime
          && collisionCount == other.collisionCount
          && evicted == other.evicted
          && invalidated == other.invalidated
          && childNodeLookupCount == other.childNodeLookupCount
          && childNodeLookupTime == other.childNodeLookupTime;
  }

  public final long cacheAccesses() {
    return cacheHit + cacheMiss;
  }

  /** Returns true if there is something useful to display */
  public final boolean hasValuesOfInterest() {
    return start + cacheMiss + cacheHit + selfTime + postCompleteAndSuspendTime + tweakLookupTime
        > 0;
  }
}
