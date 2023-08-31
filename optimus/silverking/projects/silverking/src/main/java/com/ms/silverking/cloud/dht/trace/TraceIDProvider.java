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

import com.ms.silverking.cloud.dht.common.MessageType;

/** Provide customizable trace information for each MessageGroup */
public interface TraceIDProvider {
  SkTraceId noTraceID = null;
  SkTraceId emptyTraceID = null;

  TraceIDProvider emptyTraceIDProvider = new EmptyTraceIDProvider();
  TraceIDProvider noTraceIDProvider = new NoTraceIDProvider();

  default boolean isEnabled() {
    return true;
  }

  SkTraceId traceID();

  static boolean isValidTraceID(SkTraceId maybeTraceID) {
    return maybeTraceID != noTraceID;
  }

  static boolean hasTraceID(MessageType msgType) {
    // TODO (OPTIMUS-43373): Remove legacy put_trace
    switch (msgType) {
      case PUT_TRACE:
      case LEGACY_PUT_TRACE:
      case RETRIEVE_TRACE:
      case PUT_RESPONSE_TRACE:
      case PUT_UPDATE_TRACE:
      case RETRIEVE_RESPONSE_TRACE:
        return true;
      default:
        return false;
    }
  }
}
