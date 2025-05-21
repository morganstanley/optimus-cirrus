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
package optimus.platform;

/** Allows user to control *uncommon* cases where more precise flow control is needed */
public final class FlowControl {
  /**
   * Entire @node will have re-ordering of nodes disabled.
   *
   * @see optimus.platform.PluginHelpers#setCompilerLevelZero()
   */
  public static void turnOffNodeReorder() {}
  /**
   * Entire @node reordering will assume that all non-immutable functions mutate some global state
   */
  public static void mutatesGlobalState() {}

  /** Allow user to 'declare' dependency in flow */
  public static <T> boolean waitFor(T v) {
    return true;
  }
}
