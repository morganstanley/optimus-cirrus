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
package optimus.platform.inputs

import com.google.common.annotations.VisibleForTesting
import optimus.dist.inputs.ProcessStateSnapshot
import optimus.platform.inputs.EngineForwarding.Serialization
import optimus.platform.inputs.StateApplicators.StateApplicator
import optimus.platform.inputs.loaders.ProcessSINodeInputMap
import optimus.platform.inputs.NodeInputMapValues._
import optimus.platform.inputs.loaders.LoaderSource
import optimus.platform.inputs.loaders.OptimusLoaders

import java.util.function.BiConsumer
import scala.collection.mutable

/**
 * This object stores the current state of all of the [[ProcessSINodeInput]]s. We need this because these are inputs
 * that affect the entire process, not how specific nodes run (eg: ogthread.ideal, XSFT, etc). The inputs that are
 * Scoped go on the ScenarioStack of the node that is running. We need a place to collect and store all of the process
 * inputs which is the purpose of this object
 */
object ProcessState {
  private[optimus] def snapshotCurrentState: ProcessStateSnapshot =
    withProcessStateSetIfAbsent {
      ProcessStateSnapshot(currentState)
    }

  /**
   * lazily sets the process state whenever we need it
   * @implNote f does not need to be synchronized here since if it requires synchronization then that function should synchronize itself, onus is on the caller
   */
  private def withProcessStateSetIfAbsent[T](f: => T): T = {
    this.synchronized {
      if (currentState eq null) setState(OptimusLoaders.defaults.processSINodeInputMap)
    }
    f
  }

  /**
   * the variable that actually stores the current process state
   * @implNote
   *   This will be None if the state has not been set yet. The variable needs an @volatile annotation because otherwise
   *   if one thread writes to this variable it might stay CPU's cache and never be flushed to main memory so another
   *   thread reading it might not see the updated value. This could also have been solved by synchronizing the reads
   *   (see @apiNote)
   * @apiNote
   *   any function that only reads the value does not need to be synchronized. This is because the value should only be
   *   changing once when the first optimus task runs and once when an engine starts a task, so there's no race about
   *   reading it since it will not be changing once the state is set for that task
   */
  @volatile
  private var currentState: ProcessSINodeInputMap = null

  @VisibleForTesting
  private[inputs] def TEST_ONLY_clearCurrentState(): Unit = if (currentState ne null) {
    currentState.localInputs.keySet.foreach(_.stateApplicator.resetState())
    currentState = null
  }

  private[inputs] def getIfPresentWithoutDefault[T](input: ProcessSINodeInput[T]): Option[T] =
    withProcessStateSetIfAbsent {
      currentState.getInputWithoutDefault(input)
    }

  private[inputs] def getSourceIfPresentWithoutDefault[T](input: ProcessSINodeInput[T]): Option[LoaderSource] =
    withProcessStateSetIfAbsent {
      currentState.getInputSourceWithoutDefault(input)
    }

  private[inputs] def getIfPresent[T](input: ProcessSINodeInput[T]): Option[T] =
    withProcessStateSetIfAbsent {
      currentState.getInput(input)
    }

  private[inputs] def engineForwardingForEach(
      callback: BiConsumer[NodeInput[AnyRef], NodeInputMapValue[AnyRef]]): Unit =
    if (currentState ne null) currentState.engineForwardingForEach(callback)

  /**
   * sets the current state of the application if it has not already been set. This should be called at the start of any
   * OptimusTask
   *
   * @implNote
   *   this needs to be synchronized so that if you have a process that starts two optimus tasks then only 1 of them
   *   will set the state and there will not be a race condition depending on which one wins
   * @deepImplNote
   *   the issue with setting the state is that it is not an atomic operation. This is because we can have optimized
   *   inputs (see [[NonStandardProcessSINodeInput]]) which are stored in a global settings object. Therefore, while we
   *   are changing the state some other thread might be able to read a should-be-outdated value from that object. This
   *   would be fine if we were able to guarantee that no other thread will be reading these values while the state is
   *   being set but we are not able to make that guarantee.
   *
   * Imagine if we have two threads that start optimus-task1 and optimus-task2 respectively, and task1 sets the
   * currentState var and then task2 runs and doesn't set the state (since it has already been set with task1) and then
   * starts doing its work and tries reading something from the processState before task1 has run setState. This is a
   * race condition and bad since task2 might say an input it is looking for is not present when it should be. We need a
   * lock or synchronized block to avoid this
   *
   * If we didn't have any optimized inputs then we could do this atomically since there would be nowhere for another
   * thread to find any outdated values
   * sets the current state of the application to the specification set in the map passed in. This should be called only
   * on an engine at the start of a task
   *
   * @implNote2
   *   resetting serializable process inputs that require restarts are not supported because they require explicit
   *   support in distribution (which is currently not available)
   *
   *   adding inputs is incremental, you can always add new state but can't remove state that requires restart
   * @apiNote
   *   This function should only be called at the start of an OptimusTask or after a node is sent to an engine
   */
  private[optimus] def setState(nextState: ProcessSINodeInputMap): Unit =
    this.synchronized {
      if (currentState ne null)
        currentState.localInputs.keySet
          .filter(nextState.getInputWithoutDefault(_).isEmpty)
          .foreach(input => {
            if (input.requiresRestart())
              throw new IllegalStateException(s"Cannot remove property ${input.name} that requires restart!")
            input.stateApplicator.resetState()
          })

      /* all inputs in a given dependency chain will share the same [[StateApplicatorScala]] */
      val inNextNotCurrentApplicators: mutable.Set[StateApplicator] = mutable.Set()
      nextState.localInputs
        .filter { case (input, VisibleValue(_, value, _)) =>
          currentState match {
            case null => true
            case cs =>
              val currentStateInput = cs.getInputWithoutDefault(input)
              currentStateInput.isEmpty || currentStateInput.get != value
          }
        }
        .map { case (input, _) =>
          // remove this condition when dist supports non-resettable serializable inputs
          if (input.serializationStyle == Serialization.JAVA && input.requiresRestart)
            throw new IllegalStateException(s"Can't change the value of ${input.name} that requires restart")
          inNextNotCurrentApplicators.add(input.stateApplicator)
        }

      val firstTime = currentState eq null
      inNextNotCurrentApplicators.foreach(_.apply(nextState, firstTime = firstTime))
      currentState = nextState
    }

  // only called within withProcessStateIfAbsent blocks
  private def changingInputChecks(input: ProcessSINodeInput[_]): Unit =
    if (currentState.getInput(input).isEmpty)
      throw new IllegalStateException(s"Can't change the value of an input (${input.name}) that is not set!")
    else if (input.requiresRestart())
      throw new IllegalStateException(s"Can't change the value of ${input.name} that requires restart")

  private[optimus] def reapplyApplicator(applicator: StateApplicator): Unit =
    this.synchronized {
      applicator.apply(currentState, firstTime = false)
    }

  private[inputs] def changeValueOfInputsTo(inputs: Seq[(ProcessSINodeInput[_], _)]): Unit =
    withProcessStateSetIfAbsent {
      inputs.foreach(t => changingInputChecks(t._1))
      this.synchronized {
        var newLocalInputs = currentState.localInputs
        inputs.foreach({ case (input, value) =>
          newLocalInputs = newLocalInputs.updated(input, VisibleValue(LoaderSource.CODE, value, forwardToEngine = true))
        })
        val newState = currentState.copyClass(localInputs = newLocalInputs, currentState.engineSpecificInputs)
        setState(newState)
      }
    }

  private[inputs] def changeValueOfInputTo[T](input: ProcessSINodeInput[T], value: T): Unit =
    changeValueOfInputsTo(Seq((input, value)))

  private[inputs] def removeInputs(inputs: Set[ProcessSINodeInput[_]]): Unit = withProcessStateSetIfAbsent {
    inputs.foreach(changingInputChecks)
    this.synchronized {
      setState(
        currentState
          .copyClass(localInputs = currentState.localInputs -- inputs, currentState.engineSpecificInputs))
    }
  }

  private[inputs] def removeInput[T](input: ProcessSINodeInput[T]): Unit =
    removeInputs(Set(input))

  private[inputs] def mergeStateWithPropFile(processState: ProcessSINodeInputMap): Unit = withProcessStateSetIfAbsent {
    val newState = currentState.mergeWith(processState)
    setState(newState)
  }
}
