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
package optimus.platform.util

import optimus.platform._

import java.util.concurrent.locks.ReentrantLock

/**
 * A lazily initialized value which notifies the scheduler while it blocks, thus avoiding deadlocks if the factory
 * lambda calls into graph and ends up waiting for a batcher to release (which won't happen until the graph thinks
 * all threads are "waiting"/"idle" not "working").
 *
 * DO NOT USE THIS without discussing with Optimus Graph team, since it almost always indicates problematic application
 * code. Although it avoids one type of deadlock, it doesn't help with other lazy val related issues such as usage /
 * capture of arbitrary scenario stack.
 *
 * Note: to match the behavior of normal lazy vals, the current (un)initialized state is serialized, i.e. if
 * the value is initialized, it is serialized, otherwise it is not and it will be initialized on demand after
 * deserialization.
 */
class SchedulerAwareBlockingLazy[T](factory: () => T) extends Serializable {
  @volatile private var _lock = new ReentrantLock() // if null, indicates that _value has been initialized
  @volatile private var _value: T = _ // n.b. value would be null even after initialization if factory returns null
  def value: T = {
    val lock = _lock // snapshot lock in case another thread nulls it out
    if (lock eq null) _value // null lock means that initialization is complete
    else {
      // if we're on a graph thread, notify scheduler that we are in "waiting" state while we wait for the lock
      // to avoid deadlocks if factory calls back into graph
      if (EvaluationContext.isInitialised) EvaluationContext.current.doBlockingAction(() => lock.lock())
      else lock.lock()

      try {
        // recheck underlying _lock field in case some other thread has initialized us while we waited on the lock
        if (_lock ne null) {
          _value =
            factory() // call factory outside of the doBlockingAction because it counts as "working" not "waiting"
          _lock = null // clear to a) to save memory, and b) indicate to callers that initialization is complete
        }
        _value
      } finally lock.unlock()
    }
  }
}
