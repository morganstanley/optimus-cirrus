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

// Used to manage the state of an object:
//uninitialized {isInitialized()} = false, {isDestroyed()} = false, {isRunning()} = false
//active        {isInitialized()} = true,  {isDestroyed()} = false, {isRunning()} = false
//running       {isInitialized()} = true,  {isDestroyed()} = false, {isRunning()} = true
//stopped       {isInitialized()} = true,  {isDestroyed()} = false, {isRunning()} = false
//destroyed:    {isInitialized()} = true,  {isDestroyed()} = true,  {isRunning()} = false
class ObjectState {
  private object State extends Enumeration {
    type State = Value
    val UNINITIALIZED, ACTIVE, RUNNING, STOPPED, DESTROYED = Value
  }

  @volatile private var state = State.UNINITIALIZED
  private def isInitialized: Boolean = state != State.UNINITIALIZED
  private def isDestroyed: Boolean = state == State.DESTROYED
  def isRunning: Boolean = state == State.RUNNING
  private def isStopped: Boolean = state == State.STOPPED
  private def isActive: Boolean = {
    synchronized {
      state != State.UNINITIALIZED && state != State.DESTROYED
    }
  }

  def initializeIfNotInitialized(): Unit = {
    synchronized {
      throwIfDestroyed()
      if (!isInitialized) {
        state = State.ACTIVE
      }
    }
  }

  def stopIfRunning(): Unit = {
    synchronized {
      if (State.RUNNING == state) {
        state = State.STOPPED
      }
    }
  }

  def destroyIfNotDestroyed(): Unit = {
    synchronized {
      state = State.DESTROYED
    }
  }

  def startIfNotRunning(): Unit = {
    synchronized {
      throwIfUninitialised()
      throwIfDestroyed()
      state = State.RUNNING
    }
  }

  private def throwIfUninitialised(): Unit = {
    if (!isInitialized) {
      throw new IllegalStateException("Not initialized")
    }
  }

  def throwIfNotRunning(): Unit = {
    if (!isRunning) {
      throw new IllegalStateException("Not running")
    }
  }

  private def throwIfDestroyed(): Unit = {
    if (isDestroyed) {
      throw new IllegalStateException("Destroyed")
    }
  }
}
