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

import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Map;

import com.ms.silverking.cloud.dht.client.FailureCause;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.daemon.storage.protocol.RetrievalResult;
import com.ms.silverking.cloud.dht.net.IPAliasMap;
import com.ms.silverking.net.IPAndPort;

public class SkNoopTracer implements Tracer {

  @Override
  public void setAliasMap(IPAliasMap aliasMap) {}

  @Override
  public void onBothReceiveRequest(SkTraceId traceID, MessageType msgType) {}

  @Override
  public void onBothHandleRetrievalRequest(SkTraceId traceID) {}

  @Override
  public void onAuthorizationFailure(SkTraceId traceID) {}

  @Override
  public SkTraceId issueForwardTraceID(SkTraceId maybeTraceID, IPAndPort replica, MessageType msgType,
      byte[] originator) {
    return maybeTraceID;
  }

  @Override
  public void onLocalHandleRetrievalRequest(SkTraceId traceID) {}

  @Override
  public void onLocalEnqueueRetrievalResult(SkTraceId traceID) {}

  @Override
  public void onBothDequeueAndAsyncSendRetrievalResult(SkTraceId traceID, List<RetrievalResult> results) {}

  @Override
  public void onProxyHandleRetrievalResponse(SkTraceId traceID, Map<DHTKey, OpResult> results,
      OpResult responseState) {}

  @Override
  public void onProxyHandleRetrievalResultComplete(SkTraceId traceID) {}

  @Override
  public void onProxyHandleRetrievalResultIncomplete(SkTraceId traceID) {}

  @Override
  public void onProxyHandleRetrievalResultFailed(SkTraceId traceID, FailureCause cause) {}

  @Override
  public void onLocalReap(long elapsedTime) {}

  @Override
  public void onForceReap(long elapsedTime) {}

  @Override
  public void onSegmentReap(int segNum, String cTime) {}

  @Override
  public void onKeysReap(int segNum, int keys, String cTime) {}

  @Override
  public void onSegmentRollover(int newSegNum) {}

  @Override
  public void onPreambleMismatch(SocketChannel channel) {}

  @Override
  public void onInternalRetry(DHTKey key) {}

  @Override
  public void onQueueLengthInterval(int queueLength) {}

  @Override
  public String getStringTraceId(SkTraceId traceId) {
    return "Untraced";
  }

  @Override
  public TracerContext getContext() {
    return new EmptyTracerContext();
  }
}
