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
package com.ms.silverking.cloud.dht.trace;

import optimus.dsi.trace.TraceId;

public class SkTraceId {
  private TraceId traceId;
  private SkForwardState forwardState;

  public SkTraceId(TraceId traceId, SkForwardState forwardState) {
    this.traceId = traceId;
    this.forwardState = forwardState;
  }

  public TraceId getTraceId() {
    return traceId;
  }

  public SkForwardState getForwardState() {
    return forwardState;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else {
      if (obj instanceof SkTraceId) {
        SkTraceId id = (SkTraceId) obj;
        return this.traceId.equals(id.traceId) && this.forwardState.equals(id.forwardState);
      } else {
        return false;
      }
    }
  }
}
