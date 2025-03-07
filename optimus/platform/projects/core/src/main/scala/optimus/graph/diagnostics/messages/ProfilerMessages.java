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
package optimus.graph.diagnostics.messages;

public abstract class ProfilerMessages {
  // In order to avoid writing the same string multiple times
  public void stringID(int id, String text) {}

  public void threadName(long id, String name) {}

  public void enterGraph(long time) {}

  public void exitGraph(long time) {}

  public void enterWait(int causalityID, long time) {}

  public void exitWait(long time) {}

  public void enterSpin(long time) {}

  public void exitSpin(long time) {}

  // Notes: the API below is too verbose with respect to chainedID
  public void publishNodeSending(
      int id, int pid, int blk, String chainedID, String label, long timestamp) {}

  public void publishNodeReceived(int id, int pid, int blk, String chainedID, long timestamp) {}

  public void publishNodeReceivedLocally(
      int id, int pid, int blk, String chainedID, long timestamp) {}

  public void publishNodeStarted(int id, String chainedID, long timestamp) {}

  public void publishTaskCompleted(int id, String chainedID, long timestamp) {}

  public void publishNodeResultReceived(int id, String chainedID, long timestamp) {}

  public void publishSerializedNodeResultArrived(int id, String chainedID, long timestamp) {}

  public void postComplete(int blk, int pid, long postCompleteTime) {}

  public void cacheHit(int blk, int pid, int collisionCount, long time) {}

  public void cacheMiss(int blk, int pid, int collisionCount, long time) {}

  public void cacheProxy(int blk, int pid, boolean hit, boolean countMiss, long ownerTime) {}

  public void cacheEvicted(int blk, int pid) {}

  public void cacheInvalidated(int blk, int pid) {}

  public void cacheReuse(int blk, int pid, long ancTime) {}

  public void tweakLookup(int pid, long tweakLookupTime) {}

  public void cacheLookup(int blk, int iid, long time) {}

  public void reuseCycle(int pid, int rcount) {}

  public void reuseStats(int pid, long rcount) {}

  public void nodeHashCollision(int pid) {}

  public void counterEvent(byte[] serializedEvent) {}

  public void counterEventComplete(int counterID, int id, long timestamp) {}

  public void summary(
      int blk,
      int taskId,
      int pid,
      long firstStartTime,
      long completedTime,
      long selfTime,
      long ancSelfTime,
      long suspendingTime,
      int childNodeLookupCount,
      long childNodeLookupTime,
      int enqueuingPropertyId,
      boolean initAsCompleted) {}

  public void summaryHashes(
      int tid,
      int pid,
      int ssHash,
      int entityHash,
      int argsHash,
      int resultHash,
      int ssIdHash,
      int entityIdHash,
      int argsIdHash,
      int resultIdHash) {}

  public void profileDesc(
      int id,
      long flags,
      String name,
      String pkgName,
      String modifier,
      String cacheName,
      String cachePolicy) {}

  public void profileData(
      int id,
      long start,
      long cacheHit,
      long cacheMiss,
      int cacheHitTrivial,
      long evicted,
      int invalidated,
      int reuseCycle,
      long reuseStats,
      long selfTime,
      long ancAndSelfTime,
      long postCompleteAndSuspendTime,
      long tweakLookupTime,
      long wallTime,
      long cacheTime,
      long nodeUsedTime,
      int tweakID,
      String tweakDependencyMask) {}
}
