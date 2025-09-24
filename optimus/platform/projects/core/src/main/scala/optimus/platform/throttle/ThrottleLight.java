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

package optimus.platform.throttle;

import java.util.ArrayDeque;
import java.util.Deque;
import optimus.graph.NodeTask;
import optimus.platform.EvaluationQueue;

public abstract class ThrottleLight {

  public abstract boolean enqueue(NodeTask task);

  public abstract void onChildCompleted(EvaluationQueue eq, NodeTask dbgOnly);

  public static final ThrottleLight NoOp = new NoThrottle();

  private static class NoThrottle extends ThrottleLight {

    public boolean enqueue(NodeTask task) {
      return true;
    }

    public void onChildCompleted(EvaluationQueue eq, NodeTask dbgOnly) {}
  }

  public static class Max extends ThrottleLight {
    private final Deque<NodeTask> waitingTasks = new ArrayDeque<>();
    private int inFlight;
    private int limit;

    public Max(int limit) {
      this.inFlight = 0;
      this.limit = limit;
    }

    public boolean enqueue(NodeTask task) {
      var run = false;
      synchronized (waitingTasks) {
        if (inFlight < limit) {
          inFlight++;
          limit = adjustLimit(inFlight);
          run = true;
        } else {
          waitingTasks.addLast(task);
        }
      }
      return run;
    }

    public void onChildCompleted(EvaluationQueue eq, NodeTask dbgOnly) {
      NodeTask nextTask = null;
      synchronized (waitingTasks) {
        inFlight--;
        limit = adjustLimit(inFlight);
        if (!waitingTasks.isEmpty() && inFlight < limit) {
          nextTask = waitingTasks.removeFirst();
          inFlight++;
        }
      }
      if (nextTask != null) {
        eq.scheduler().markAsRunnableAndEnqueue(nextTask);
      }
    }

    protected int adjustLimit(int inFlight) {
      return limit;
    }
  }

  public static class Oscillating extends Max {
    private final int maxInFlight;
    private final int minInFlight;

    public Oscillating(int maxInFlight, int minInFlight) {
      super(maxInFlight);
      this.maxInFlight = minInFlight;
      this.minInFlight = maxInFlight;
    }

    @Override
    protected int adjustLimit(int inFlight) {
      if (inFlight == minInFlight) return maxInFlight;
      else if (inFlight == maxInFlight) return minInFlight;
      return super.adjustLimit(inFlight);
    }
  }
}
