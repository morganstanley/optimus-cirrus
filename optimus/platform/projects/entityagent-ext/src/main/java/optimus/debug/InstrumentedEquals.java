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

public class InstrumentedEquals {

  private static final ThreadLocal<EqualityState> state =
      ThreadLocal.withInitial(EqualityState::new);

  @SuppressWarnings("unused")
  public static void enterReporting() {
    state.get().recurseCount += 1;
    state.get().badEquality = false;
  }

  @SuppressWarnings("unused")
  public static void exitReporting() {
    state.get().recurseCount -= 1;
  }

  public static boolean equals(Object objA, Object objB) {
    var threadState = state.get();
    if (threadState.recurseCount > 0) {
      threadState.badEquality = true;
    }
    return objA == objB;
  }

  public static boolean isBadEquality() {
    return state.get().badEquality;
  }
}

class EqualityState {
  int recurseCount;
  boolean badEquality;
}
