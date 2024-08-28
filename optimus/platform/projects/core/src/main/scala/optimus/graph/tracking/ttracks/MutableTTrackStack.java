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

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// be careful if we move to running cleanups in parallel, this is shared between TTrackRoots
public class MutableTTrackStack {
  private static final Logger log = LoggerFactory.getLogger(MutableTTrackStack.class);

  static class Frame {
    TTrack ttrack;
    int breadth; // points to the ptrack we are currently processing
    int initID; // traversal ID before visit
    int minCallerID; // minimum ID of callers traversed (used to determine redundant duplicate
    // edges)

    @Override
    public String toString() {
      return ttrack.toString() + " minCallerID:" + minCallerID + " initID:" + initID;
    }
  }

  static final int INITIAL_SIZE = 40;
  static final int WARN_IF_GROWS_ABOVE = 1000;
  protected Frame[] stack;
  protected int depth;

  public MutableTTrackStack() {
    stack = new Frame[INITIAL_SIZE];
    depth = 0;
  }

  // Increment stack depth counter, reset breadth and minCallerID for the new ttrack.
  // At each new level of the TTrack graph we keep track of the level we came from and increment the
  // lastUpdateTraversalID.
  final Frame push(TTrack ttrack) {
    ensureSize();
    Frame frame = stack[depth];
    if (frame == null) frame = stack[depth] = new Frame();
    depth++;
    frame.ttrack = ttrack;
    frame.breadth = -1;
    frame.initID = ttrack.lastUpdateTraversalID;
    frame.minCallerID = Integer.MAX_VALUE;
    return frame;
  }

  // If we are resizing often, it means we have a very deep call chain - consider restructuring the
  // code
  private void ensureSize() {
    int size = stack.length;
    if (depth == size) {
      int newSize = size * 2;
      if (size > WARN_IF_GROWS_ABOVE) {
        log.warn("Stack size reached " + depth + ", resizing to " + newSize);
      }
      stack = Arrays.copyOf(stack, newSize);
    }
  }

  protected final Frame top() {
    return stack[depth - 1];
  }

  protected final void pop() {
    --depth;
  }

  final Frame popAndGet() {
    if (--depth == 0) return null;
    else return stack[depth - 1];
  }

  protected final boolean isEmpty() {
    return depth == 0;
  }
}
