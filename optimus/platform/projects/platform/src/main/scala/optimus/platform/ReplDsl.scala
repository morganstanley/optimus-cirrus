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
package optimus.platform

/**
 * DSL intended for use in REPL.
 */
object ReplDsl {

  private lazy val replThread: Thread = Thread.currentThread

  /**
   * Enforces assumption that REPL behaves like single thread of execution.
   */
  private def checkReplThread(): Unit = {
    val currentThread = Thread.currentThread
    require(
      currentThread eq replThread,
      "optimus.platform.ReplDsl assumes that REPL env behaves like a single thread of execution. But REPL execution has switched threads:" +
        "\n  1st use of ReplDsl was from Thread:    " + replThread.threadId + " " + replThread.getName() +
        "\n  Now ReplDsl is being used from Thread: " + currentThread.threadId + " " + currentThread.getName()
    )
  }

  /**
   * @todo
   *   Enhance to be ThreadLocal if necessary for REPL use.
   */
  private val scenarioStack = {
    // TODO (OPTIMUS-0000): Workaround for ArrayStack grow not working when size zero
    // Have mailed typesafe
    val s = scala.collection.mutable.ArrayStack[Scenario](null.asInstanceOf[Scenario])
    s.pop()
    s
  }

  def push(scen: Scenario): Unit = {
//    checkReplThread()
//
//    scenarioStack.push(EvaluationContext.scenario)
//    EvaluationContext.scenario = scen
  }

  def popScenario(): Unit = {
    checkReplThread()

    if (scenarioStack.isEmpty) throw new IllegalStateException("ReplDsl.popScenario: No scenario to pop to")
//    EvaluationContext.scenario = scenarioStack.pop
  }

}
