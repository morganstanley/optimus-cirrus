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
package com.ms.silverking.thread.lwt;

import java.lang.Thread.UncaughtExceptionHandler;

import com.ms.silverking.thread.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LWTUncaughtExceptionHandler implements UncaughtExceptionHandler {
  private static final int uncaughtExceptionExitDelayMillis = 4 * 1000;

  private static Logger log = LoggerFactory.getLogger(LWTUncaughtExceptionHandler.class);

  /**
   * Display information about the uncaught exception and then force the JVM to exit. We prefer
   * fail-stop behavior to unexpected behavior.
   */
  @Override
  public void uncaughtException(Thread t, Throwable e) {
    log.error("Thread {} threw an uncaught exception", t.getName(), e);
    ThreadUtil.sleep(uncaughtExceptionExitDelayMillis);
    System.exit(1);
  }
}
