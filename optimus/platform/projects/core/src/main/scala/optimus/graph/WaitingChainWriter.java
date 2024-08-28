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

import java.util.IdentityHashMap;

import optimus.platform.util.PrettyStringBuilder;

public final class WaitingChainWriter {
  /**
   * Check that we're not going to print the exact same link (note: it's not enough just to compare
   * the NodeTasks by reference since they're generally not exactly the same task unless we have a
   * cycle). This method compares fields that are written out in tsk.writePrettyString
   */
  private static boolean sameChainLink(NodeTask t, NodeTask next) {
    return next.executionInfo().name().equals(t.executionInfo().name())
        && next.stateAsString().equals(t.stateAsString());
  }

  private static void writeArrow(PrettyStringBuilder sb) {
    sb.append(" -> ");
  }

  private static void writeWeightedArrow(PrettyStringBuilder sb, int n) {
    sb.append(" -[").append(n).append("]-> ");
  }

  private static void writeCollapsedLinks(
      PrettyStringBuilder sb, NodeTask tsk, int duplicatedTaskCount) {
    if (duplicatedTaskCount >= 2) {
      writeWeightedArrow(sb, duplicatedTaskCount);
      tsk.writePrettyString(sb);
    } else if (duplicatedTaskCount == 1) { // just unroll
      writeArrow(sb);
      tsk.writePrettyString(sb);
      writeArrow(sb);
      tsk.writePrettyString(sb);
    } else if (duplicatedTaskCount == 0) { // negatives are possible, don't do anything in that case
      writeArrow(sb);
      tsk.writePrettyString(sb);
    }
  }

  private static int writeLink(
      PrettyStringBuilder sb,
      NodeTask prevTsk,
      NodeTask tsk,
      NodeTask nextTsk,
      int duplicatedTaskCount) {
    boolean endChain = nextTsk == null || !sameChainLink(tsk, nextTsk);
    if (endChain) {
      if (prevTsk == null) // start of chain
      tsk.writePrettyString(sb);
      else {
        if (!sb.isEmpty()) writeArrow(sb);
        tsk.writePrettyString(sb);
        // subtract 1 for link just written
        if (nextTsk != null || duplicatedTaskCount > 0)
          writeCollapsedLinks(sb, tsk, duplicatedTaskCount - 1);
        duplicatedTaskCount = 0;
      }
    } else duplicatedTaskCount++; // keep accumulating
    return duplicatedTaskCount;
  }

  /** extracted for testing */
  public static void writeWaitingChain(NodeTask tsk, PrettyStringBuilder sb) {
    writeWaitingChain(tsk, sb, new IdentityHashMap<>(), new IdentityHashMap<>());
  }

  /** write waiting (forward) chain, with cycle detection and deduplication */
  static void writeWaitingChain(
      NodeTask tsk,
      PrettyStringBuilder sb,
      IdentityHashMap<NodeTask, Integer> syncStackHash,
      IdentityHashMap<NodeTask, Integer> waitChainHash) {
    int sandwichedTaskCount = 0; // for deduplication (e.g. long proxy chains)
    NodeTask prevTsk = null;

    NodeTask tsk2 = tsk == null ? null : tsk.getWaitingOn(); // Walking 2x
    int chainCount = 0;

    while (tsk != null) {
      waitChainHash.put(tsk, chainCount++);
      NodeTask nextTsk = tsk.getWaitingOn();
      sandwichedTaskCount = writeLink(sb, prevTsk, tsk, nextTsk, sandwichedTaskCount);
      Integer stackLoc = syncStackHash.get(tsk);
      if (stackLoc != null) {
        sb.append(" [STACK=" + stackLoc + " (sync stack deadlock?)]");
      }
      if (tsk == tsk2) {
        sb.append(" -> Cycle detected");
        break;
      }
      prevTsk = tsk;
      tsk = nextTsk;
      tsk2 = tsk2 == null ? null : tsk2.getWaitingOn(); // Walking 2x
      tsk2 = tsk2 == null ? null : tsk2.getWaitingOn();
    }
  }
}
