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

public class InstrumentedHashCodes {
  static InstrumentationConfig.MethodRef mrHashCode =
      new InstrumentationConfig.MethodRef("optimus/debug/InstrumentedHashCodes", "hashCode");

  private static class HashCodeState {
    int recurseCount;
  }

  private static ThreadLocal<HashCodeState> state = ThreadLocal.withInitial(HashCodeState::new);

  public static void enterReporting() {
    state.get().recurseCount += 1;
  }

  public static void exitReporting() {
    state.get().recurseCount -= 1;
  }

  public static int hashCode(Object o) {
    var threadState = state.get();
    if (threadState.recurseCount > 0) {
      System.err.println(">>> hash/recursive" + o.getClass().getName());
    }
    return System.identityHashCode(0);
  }
}
