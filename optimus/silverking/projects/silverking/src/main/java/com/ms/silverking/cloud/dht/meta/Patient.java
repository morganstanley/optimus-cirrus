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
package com.ms.silverking.cloud.dht.meta;

public class Patient {
  private final String name;
  private long lastRestartAttempt;
  private int numRestartAttempts;

  private static final long restartTimeoutMillis = 1 * 60 * 1000;

  public Patient(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public long getLastRestartAttempt() {
    return lastRestartAttempt;
  }

  public boolean restartTimedOut(long absTimeMillis) {
    if (!restartPending()) {
      throw new RuntimeException("No restart pending");
    } else {
      return absTimeMillis - lastRestartAttempt > restartTimeoutMillis;
    }
  }

  public void handleTimeout() {
    if (!restartPending()) {
      throw new RuntimeException("No restart pending");
    } else {
      lastRestartAttempt = 0;
    }
  }

  public boolean restartPending() {
    return lastRestartAttempt > 0;
  }

  public void markRestarted(long absTimeMillis) {
    if (restartPending()) {
      throw new RuntimeException("Restart already pending");
    } else {
      ++numRestartAttempts;
      lastRestartAttempt = absTimeMillis;
    }
  }

  public int getNumRestartAttempts() {
    return numRestartAttempts;
  }

  @Override
  public String toString() {
    return name + ":" + lastRestartAttempt + ":" + numRestartAttempts;
  }
}
