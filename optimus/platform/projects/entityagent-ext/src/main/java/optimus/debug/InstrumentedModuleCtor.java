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
package optimus.debug;

import java.util.function.Consumer;

public class InstrumentedModuleCtor {
  private static final ThreadLocal<CallBracketingState> state =
      ThreadLocal.withInitial(CallBracketingState::new);
  public static Consumer<Integer> callback;

  @SuppressWarnings("unused")
  public static void enterReporting() {
    state.get().recurseCount += 1;
  }

  @SuppressWarnings("unused")
  public static void exitReporting() {
    state.get().recurseCount -= 1;
  }

  @SuppressWarnings("unused")
  public static void exitReporting(Throwable ex) {
    exitReporting();
  }

  @SuppressWarnings("unused")
  public static int pauseReporting() {
    var cstate = state.get();
    var prevCount = cstate.recurseCount;
    state.get().recurseCount = 0;
    return prevCount;
  }

  @SuppressWarnings("unused")
  public static void resumeReporting(int prevValue) {
    state.get().recurseCount = prevValue;
  }

  private static void dumpWarning() {
    System.err.println(
        ">> RT violation detected (module constructor)! Accessing the graph while constructing a scala object/singleton");
    Thread.dumpStack();
  }

  public static void trigger() {
    var threadState = state.get();
    if (threadState.recurseCount > 0) {
      if (callback != null) callback.accept(threadState.recurseCount);
      // this happens when we detect violations before we have initialized the callback
      else dumpWarning();
    }
  }
}
