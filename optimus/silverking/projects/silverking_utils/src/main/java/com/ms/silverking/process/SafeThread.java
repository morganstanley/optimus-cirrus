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
package com.ms.silverking.process;

import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.util.PropertiesHelper.UndefinedAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Thread class with pre-configured uncaught exception handler */
public class SafeThread extends Thread {
  private static final Logger log = LoggerFactory.getLogger(SafeThread.class);
  private static final String defaultUncaughtExceptionHandlerProperty =
      SafeThread.class.getName() + ".DefaultUncaughtExceptionHandler";

  static {
    String handlerName;
    UncaughtExceptionHandler handler;

    /*
     * We should never have uncaught exceptions. The default exception handler
     * logs the exception and exits. The purpose is to fail fast and visibly so
     * that any uncaught exceptions are noticed and fixed, rather than
     * silently ignored.
     */

    handlerName =
        PropertiesHelper.systemHelper.getString(
            defaultUncaughtExceptionHandlerProperty, UndefinedAction.ZeroOnUndefined);
    log.info("UncaughtExceptionHandler: {}", handlerName == null ? "<none>" : handlerName);
    if (handlerName != null) {
      try {
        handler = (UncaughtExceptionHandler) Class.forName(handlerName).newInstance();
        setDefaultUncaughtExceptionHandler(handler);
      } catch (Exception e) {
        log.error("Unable to create UncaughtExceptionHandler: {}", handlerName, e);
        // TODO (OPTIMUS-40540): revisit System.exit as this isn't the correct thing to do either on
        // the client or
        // on the server
        System.exit(-1);
      }
    }
  }

  public static void setDefaultUncaughtExceptionHandler(UncaughtExceptionHandler handler) {
    log.info("Setting default uncaught exception handler to {}", handler);
    Thread.setDefaultUncaughtExceptionHandler(handler);
  }

  public SafeThread(Runnable target, String name) {
    this(target, name, null, false);
  }

  public SafeThread(Runnable target, String name, boolean daemon) {
    this(target, name, null, daemon);
  }

  public SafeThread(Runnable target, String name, UncaughtExceptionHandler customHandler) {
    this(target, name, customHandler, false);
  }

  public SafeThread(
      Runnable target, String name, UncaughtExceptionHandler customHandler, boolean daemon) {
    super(target, name);
    this.setDaemon(daemon);
    if (customHandler != null) {
      this.setUncaughtExceptionHandler(customHandler);
    }
  }
}
