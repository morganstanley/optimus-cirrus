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
package com.ms.silverking.numeric;

import java.io.Serializable;

public class Priority implements Comparable<Priority>, Serializable {
  protected int priority;

  public Priority(int priority) {
    this.priority = priority;
  }

  public Priority(Priority priority) {
    this.priority = priority.priority;
  }

  public boolean lessThan(Priority other) {
    return priority < other.priority;
  }

  public boolean lessThanEq(Priority other) {
    return priority <= other.priority;
  }

  public boolean greaterThan(Priority other) {
    return priority > other.priority;
  }

  public boolean greaterThanEq(Priority other) {
    return priority >= other.priority;
  }

  public int compareTo(Priority other) {
    if (priority < other.priority) {
      return -1;
    } else if (priority > other.priority) {
      return 1;
    } else {
      return 0;
    }
  }

  public int hashCode() {
    return priority;
  }

  public boolean equals(Object other) {
    Priority otherP;

    otherP = (Priority) other;
    return priority == otherP.priority;
  }

  public int toInt() {
    return priority;
  }

  public String toString() {
    return new Integer(priority).toString();
  }
}
