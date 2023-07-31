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
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.net.IPAndPort;

public interface Tracer extends HasTracerContext {
  /* the API naming conversion for its prefix:
   *  onBoth: this callback func could be called on both proxy and final replica
   *  onProxy: this callback func is only called on proxy
   *  onLocal: this callback fun is only called on final replica
   */
  // Alias map to enhance trace logs
  void setAliasMap(IPAliasMap aliasMap);

  // Phase 0: scheduling of receiving and handling request
  void onBothReceiveRequest(SkTraceId skTraceId, MessageType msgType);

  void onBothHandleRetrievalRequest(SkTraceId traceID);

  void onAuthorizationFailure(SkTraceId traceID);

  // Phase 1: if proxy is going to forward message
  SkTraceId issueForwardTraceID(SkTraceId maybeTraceID, IPAndPort replica, MessageType msgType,
      byte[] originator); // Tracer will issue a new traceID for final replica

  // Phase 2: get value from storage and put them in queue
  void onLocalHandleRetrievalRequest(SkTraceId traceID);

  void onLocalEnqueueRetrievalResult(SkTraceId traceID);

  // Phase 3: Take the results from queue and send them to destination (can be client or proxy)
  // TODO (OPTIMUS-0000): consider distinguish if result is sent from proxy or final replica
  void onBothDequeueAndAsyncSendRetrievalResult(SkTraceId traceID, List<RetrievalResult> results);

  // Phase 4: Proxy handles response messages
  void onProxyHandleRetrievalResponse(SkTraceId traceID, Map<DHTKey, OpResult> results, OpResult responseState);

  // Phase 5: Proxy check the complete status of result
  // Note here the result is a decision that encompasses >1 OpResult covered in onProxyHandleRetrievalResponse
  void onProxyHandleRetrievalResultComplete(SkTraceId traceID);

  void onProxyHandleRetrievalResultIncomplete(SkTraceId traceID);

  void onProxyHandleRetrievalResultFailed(SkTraceId traceID, FailureCause cause);

  void onLocalReap(long elapsedTime);

  void onForceReap(long elapsedTime);

  // On reap of a full segment via segment level retention policy
  void onSegmentReap(int segNum, String cTime);

  // On reap of a segment via key level retention policy
  void onKeysReap(int segNum, int keys, String cTime);

  void onSegmentRollover(int newSegNum);

  void onPreambleMismatch(SocketChannel channel);

  void onInternalRetry(DHTKey key);

  void onQueueLengthInterval(int queueLength);

  String getStringTraceId(SkTraceId traceBytes);

  default void onAsyncSendTimeout(UUIDBase sendUuid, String traceIdStr, long lastPollTime, long currPollTime, int currQueueSize, long creationTime, long deadline, long currTime) {}
}
