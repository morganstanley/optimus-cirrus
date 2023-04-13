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

/**
 * Associates work to be done with the worker that will do the work.
 */
public class AssignedWork implements Comparable<AssignedWork> {
  private final BaseWorker worker;
  private final Object work;
  private final int priority;

  public AssignedWork(BaseWorker worker, Object work, int priority) {
    this.worker = worker;
    this.work = work;
    this.priority = priority;
  }

  public BaseWorker getWorker() {
    return worker;
  }

  public Object getWork() {
    return work;
  }

  public void doWork() {
    worker.callDoWork(work);
  }

  @Override
  public int compareTo(AssignedWork other) {
    if (priority < other.priority) {
      return -1;
    } else if (priority > other.priority) {
      return 1;
    } else {
      return 0;
    }
  }

  public String toString() {
    StringBuilder sb;

    sb = new StringBuilder();
    sb.append(worker);
    sb.append(':');
    sb.append(work);
    return sb.toString();
  }
}
