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
package optimus.platform.internal

import java.util.concurrent.locks.ReentrantLock

final class StateAlreadyInitializedException private[internal] (clazz: Class[_])
    extends Exception(s"The state holder on ${clazz.getName} has already been initialized")

trait StateHolder[State <: AnyRef] {
  protected def newState: State
  protected def getState: State

  protected final def inTestMode: Boolean = isInTestMode

  private[optimus] def beforeTest: Unit = {
    require(isInTestMode, "Should be in test mode before calling beforeTest")
  }

  private[optimus] def afterTest: Unit = {
    require(isInTestMode, "Should be in test mode before calling afterTest")
  }

  private[optimus] def beforeTestClass: Unit = {
    setInTestMode(true)
  }

  private[optimus] def afterTestClass: Unit = {
    setInTestMode(false)
  }

  private def setInTestMode(inTestMode: Boolean): Unit = {
    val st = Thread.currentThread.getStackTrace
    require(
      st.exists { s =>
        s.getClassName.startsWith("org.junit") || s.getClassName.startsWith("org.scalatest")
      } || Thread
        .currentThread()
        .getName
        .startsWith("Mock DAL Server Transport"))
    // Should only set test mode to the inverse of the current state
    // or be setting it false (regardless of the current state)
    // if you hit this requires you probably tried to set the state to inTestMode=true more than exactly once.
    // We do not allow testMode to be set true more than once as this may imply the state is reset,
    // so the prior setting will be overwritten with the default and then it'll be hard to debug the tests
    // when the state winds up being partially set but appears as if it should be fully set in the IDE
    require(inTestMode != isInTestMode || !inTestMode, "Tests should set state holders into test mode exactly once")
    isInTestMode = inTestMode
  }
  private var isInTestMode: Boolean = false
}

// A StateHolder which permits MT test runners by having a ThreadLocal for state objects to prevent different threads
// from getting the same instance
class SimpleStateHolder[State <: AnyRef](stateCreator: () => State) extends StateHolder[State] {
  private lazy val oneState = newState
  private lazy val testLocalState = new ThreadLocal[State]

  override def newState: State = stateCreator()

  private[optimus] override def beforeTest: Unit = {
    super.beforeTest
    if (testLocalState.get ne null)
      throw new StateAlreadyInitializedException(getClass)
    testLocalState.set(newState)
  }

  private[optimus] override def afterTest: Unit = {
    super.afterTest
    require(testLocalState.get ne null)
    testLocalState.remove()
  }

  override protected def getState: State = {
    if (inTestMode) {
      // Local state for each thread to support MT test runners
      if (testLocalState.get eq null) {
        testLocalState.set(newState)
      }
      testLocalState.get
    } else
      oneState
  }
}

// A StateHolder which can only have a single state object at a time. Multiple suites will create and destroy this
// object (and must do so serially). However, for MT access to state, this is simpler to manage as different threads
// will observe the same instance for the lifecycle of a suite.
class SimpleGlobalStateHolder[State <: AnyRef](stateCreator: () => State) extends StateHolder[State] {
  private lazy val oneState = newState
  private var testLocalState: Option[State] = None

  private val stateLock = new ReentrantLock()

  override private[optimus] def beforeTestClass: Unit = {
    super.beforeTestClass
    stateLock.lock()
    testLocalState = Some(newState)
  }

  override private[optimus] def afterTestClass: Unit = {
    super.afterTestClass
    testLocalState = None
    stateLock.unlock()
  }

  override protected def newState: State = stateCreator()

  override protected def getState: State =
    if (inTestMode) testLocalState.get
    else oneState

  def requireTestMode(): Unit = require(inTestMode, "State holder required to be in test mode but is not")
}
