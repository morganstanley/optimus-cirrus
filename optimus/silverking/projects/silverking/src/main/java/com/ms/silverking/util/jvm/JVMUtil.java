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
package com.ms.silverking.util.jvm;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.security.Permission;

import com.ms.silverking.thread.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JVMUtil {

  private static Logger log = LoggerFactory.getLogger(JVMUtil.class);

  public static void dumpStackTraces() {
    ThreadInfo[] threads;

    threads = ManagementFactory.getThreadMXBean().dumpAllThreads(true, true);

    ThreadUtil.printStackTraces();
    log.info("");
    for (ThreadInfo thread : threads) {
      long threadID;

      threadID = thread.getThreadId();
      log.info(
          "ThreadId: {}  ThreadName: {}  ThreadState: {}   BlockedCount: {}  WaitedCount:  {}",
          thread.getThreadId(),
          thread.getThreadName(),
          thread.getThreadState(),
          thread.getBlockedCount(),
          thread.getWaitedCount());

      if (thread.getLockOwnerId() > -1) {
        log.info("LOCK INFORMATION For: {}", threadID);
        log.info(
            "LockName: {}  LockOwnerName: {}", thread.getLockOwnerName(), thread.getLockName());
      }
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    dumpStackTraces();
  }
}
