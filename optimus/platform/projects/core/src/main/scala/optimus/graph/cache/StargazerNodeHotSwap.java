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
package optimus.graph.cache;

import optimus.graph.PropertyNode;

public class StargazerNodeHotSwap {
  private static final boolean STARGAZER_ENABLED =
      System.getProperty("optimus.stargazer.live.enabled", "false").equalsIgnoreCase("true");

  private static StargazerNodeHotSwapLogic logic = null;
  private static StargazerNodeHotSwapData data = null;

  public static void setLogic(StargazerNodeHotSwapLogic hsLogic) {
    logic = hsLogic;
  }

  public static void update(StargazerNodeHotSwapData d) {
    if (data == null) {
      data = d;
    } else {
      data = data.extendWith(d);
    }
  }

  public static <T> PropertyNode<T> getMagicKey(PropertyNode<T> key) {
    // ultra fast short circuit if Stargazer is not enabled
    if (!STARGAZER_ENABLED) {
      return key;
    }

    if (logic != null && data != null) {
      return logic.replaceWith(key, data);
    }

    return key;
  }
}
