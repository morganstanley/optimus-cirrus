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
package com.ms.silverking.util;

import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps TimerTask instances to ensure that all executions do not break the timer due to a thrown exception
 */
public class SafeTimerTask extends TimerTask {
  private static Logger log = LoggerFactory.getLogger(SafeTimerTask.class);

  private final TimerTask task;

  public SafeTimerTask(TimerTask task) {
    this.task = task;
  }

  public void run() {
    try {
      task.run();
    } catch (RuntimeException e) {
      log.error("Exception during timed task execution", e);
    }
  }
}