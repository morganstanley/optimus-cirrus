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
package optimus.patches;

public class MiscPatches {
  @SuppressWarnings("unused") // Call to this methods will be injected by InstrumentationInjector
  public static void remove_LD_PRELOAD(Object _this) {
    ((ProcessBuilder) _this).environment().remove("LD_PRELOAD");
  }

  /**
   * Call to this methods will be injected by InstrumentationInjector for example one can add to
   * trace.sc the following line and can start modifying the code the function below
   * suffixCall("java.lang.String.<init>([BB)V", "optimus.patches.MiscPatches.genericSuffixCall")
   *
   * @param _this
   */
  @SuppressWarnings("unused")
  public static void genericSuffixCall(Object _this) {
    if ("some_text".equals(_this)) System.err.println("Collect and print stacks?");
  }
}
