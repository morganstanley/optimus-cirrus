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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.function.Consumer;

public class SystemExitReplacement {

  static HashMap<String, Consumer<Integer>> hooks = new HashMap<>();
  static public synchronized void setHook(String name, Consumer<Integer> hook) {
    if(hook == null)
      hooks.remove(name);
    else
      hooks.put(name, hook);
  }

  // we replace System.exit with this code
  public static void exit(int status) {
    Method exitImpl;
    try {
      // exitImpl relies on various optimus.systemexit.* classes which are not available in the boot classloader
      // which System is in. Rather than adding to the bootclasspath in every test, we simply pick ourselves up
      // from the system (i.e. regular) classloader where we have access to everything we want
      Class<?> thisInSystemClassloader = ClassLoader.getSystemClassLoader().loadClass(
          "optimus.systemexit.SystemExitReplacement");
      exitImpl = thisInSystemClassloader.getMethod("exitImpl", Integer.TYPE);
    }
    catch (Exception ex) {
      throw new RuntimeException(
          "Unable to find optimus.systemexit.SystemExitReplacement.exitImpl in System classloader",ex);
    }
    try {
      exitImpl.invoke(null, status);
    }
    catch (IllegalAccessException ex) {
      throw new RuntimeException(ex);
    }
    catch (InvocationTargetException ex) {
      // this will be the optimus.systemexit.SystemExitInterceptedException that we are expecting
      throw (RuntimeException)ex.getTargetException();
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
      throw new SystemExitInterceptedException();
    }
    else {
      logger.debug("[EXIT-INTERCEPT] normal exit");
      Runtime.getRuntime().exit(status);
    }
  }
}
