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
package optimus.systemexit;

import java.util.HashMap;
import java.util.function.Consumer;

public class SystemExitReplacement {
  private static final HashMap<String, Consumer<Integer>> hooks = new HashMap<>();

  static public void setHook(String name, Consumer<Integer> hook) {
    synchronized (hooks) {
      if (hook == null)
        hooks.remove(name);
      else
        hooks.put(name, hook);
    }
  }

  public static void exitImpl(int status) {
    StandardSystemExitLogger logger = new StandardSystemExitLogger();
    SystemExitGetOsInfo getOsInfo = new SystemExitGetOsInfo(logger);

    if (getOsInfo.doIntercept()) {
      logger.debug("[EXIT-INTERCEPT] logged exit with exception");
      getOsInfo.getInfo();
      synchronized(hooks) {
        for (Consumer<Integer> hook : hooks.values()) {
          hook.accept(status);
        }
      }
      throw new SystemExitInterceptedException(status);
    }
    else {
      logger.debug("[EXIT-INTERCEPT] normal exit");
      Runtime.getRuntime().exit(status);
    }
  }
}
