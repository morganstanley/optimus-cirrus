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
package com.ms.silverking.cloud.dht.daemon.storage.convergence.management;

import com.ms.silverking.id.UUIDBase;

public abstract class Action implements Comparable<Action> {
  private final long msl;
  private final long lsl;
  private final Action[] upstreamDependencies;
  private Action[] downstreamDependencies;
  private Action lastIncompleteUpstream;

  private static final Action[] emptyDependencies = new Action[0];

  public Action(long msl, long lsl, Action[] upstreamDependencies) {
    this.msl = msl;
    this.lsl = lsl;
    if (upstreamDependencies != null) {
      this.upstreamDependencies = upstreamDependencies;
      for (Action a : upstreamDependencies) {
        if (a.equals(this)) {
          throw new RuntimeException("Can't add self as upstream dependency: " + a);
        }
      }
    } else {
      this.upstreamDependencies = emptyDependencies;
    }
    this.downstreamDependencies = emptyDependencies;
  }

  public void addDownstreamDependencies(Action[] downstreamDependencies) {
    this.downstreamDependencies = downstreamDependencies;
  }

  public Action[] getUpstreamDependencies() {
    return upstreamDependencies;
  }

  public Action[] getDownstreamDependencies() {
    return downstreamDependencies;
  }

  public Action getLastIncompleteUpstream() {
    return lastIncompleteUpstream;
  }

  public void setLastIncompleteUpstream(Action lastIncompleteUpstream) {
    this.lastIncompleteUpstream = lastIncompleteUpstream;
  }

  @Override
  public int compareTo(Action o) {
    if (msl < o.msl) {
      return -1;
    } else if (msl > o.msl) {
      return 1;
    } else {
      if (lsl < o.lsl) {
        return -1;
      } else if (lsl > o.lsl) {
        return 1;
      } else {
        return 0;
      }
    }
  }

  @Override
  public int hashCode() {
    return (int) lsl;
  }

  @Override
  public boolean equals(Object obj) {
    Action a;

    a = (Action) obj;
    return msl == a.msl && lsl == a.lsl;
  }

  public UUIDBase getUUID() {
    return new UUIDBase(msl, lsl);
  }
}
