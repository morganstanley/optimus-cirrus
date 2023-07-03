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

public class InstrumentedExceptionCtor {
  private static final ThreadLocal<CallBracketingState> state =
      ThreadLocal.withInitial(CallBracketingState::new);
  public static Consumer<Exception> callback;

  @SuppressWarnings("unused")
  public static int pauseReporting() {
    var cstate = state.get();
    var prevCount = cstate.recurseCount;
    state.get().recurseCount++;
    return prevCount;
  }

  @SuppressWarnings("unused")
  public static void resumeReporting(int prevValue) {
    state.get().recurseCount = prevValue;
  }

  @SuppressWarnings("unused") // Call to this methods will be injected by InstrumentationInjector
  public static void exceptionInitializing(Object _this) {
    if (callback != null && state.get().recurseCount == 0) callback.accept((Exception) _this);
  }
}
