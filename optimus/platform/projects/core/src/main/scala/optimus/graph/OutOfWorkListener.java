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

import java.util.ArrayList;

/** Used in on stalled callbacks */
public interface OutOfWorkListener {
  /**
   * If this delay is greater than zero, the scheduler must attempt to call this callback after the
   * timeout. This is used to force batches to trigger even if there are no stalls and the batch
   * isn't full.
   *
   * <p>The call back won't be called if it has been deregistered.
   */
  default long mustCallDelay() {
    return -1L;
  }

  /**
   * If this delay is greater than zero, the scheduler will not register this callback until the
   * delay has passed.
   *
   * <p>If mustCallDelay() is also true, the must-call clock will start after this callback is
   * registered.
   */
  default long mustWaitDelay() {
    return -1L;
  }

  /**
   * Called once each time the graph stalls (i.e. runs out of nodes to run) on callbacks which have
   * been registered with the Scheduler. A typical implementation might schedule more nodes for
   * execution.
   *
   * <p>The callback will be deregistered once it is called. This method is currently always called
   * under the scheduler's lock, so the callback may atomically re-register if required.
   *
   * <p>TODO (OPTIMUS-17080): Change this API so the method is not called under the lock
   *
   * @param scheduler scheduler that is reporting 'out of work' event
   * @param outstandingTasks if not null, run cycle detection on BatchScope awaited tasks
   */
  void onGraphStalled(Scheduler scheduler, ArrayList<NodeTask> outstandingTasks);

  /** Allow some external aka test clients to call without outstanding tasks collection */
  default void onGraphStalled(Scheduler scheduler) {
    onGraphStalled(scheduler, null);
  }

  // return true if scheduler plugin is ready for onGraphStalled callback...
  default boolean ready(ArrayList<NodeTask> outstandingTasks) {
    return true;
  }

  default int checkForNewWork(Scheduler scheduler) {
    return 0;
  }

  /**
   * Returns tag that is used to id a given callback Used to allow for semi-manual control of
   * batched nodes
   */
  default Object limitTag() {
    return null;
  }

  /**
   * Returns priority of the scheduler callback Notes: 1. Callbacks will be raised in the order of
   * priority 2. If any callback adds nodes to the scheduler queue(s) only the remaining callbacks
   * with the same priority will be called in the current stall notification pass
   */
  default int priority() {
    return 0;
  }
}
