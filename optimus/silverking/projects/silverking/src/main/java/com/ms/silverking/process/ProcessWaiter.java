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

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

public class ProcessWaiter implements Runnable {
  private final Process p;
  private final SynchronousQueue<Integer> sq;

  public static final int TIMEOUT = Integer.MAX_VALUE;

  public ProcessWaiter(Process p) {
    this.p = p;
    sq = new SynchronousQueue<Integer>();
    new Thread(this, "ProcessWaiter").start();
  }

  public int waitFor(int millis) {
    try {
      Integer result;

      result = sq.poll(millis, TimeUnit.MILLISECONDS);
      if (result != null) {
        return result;
      } else {
        return TIMEOUT;
      }
    } catch (InterruptedException ie) {
      throw new RuntimeException("panic");
    }
  }

  public void run() {
    try {
      int result;

      result = p.waitFor();
      sq.put(result);
    } catch (InterruptedException ie) {
    }
  }
}
