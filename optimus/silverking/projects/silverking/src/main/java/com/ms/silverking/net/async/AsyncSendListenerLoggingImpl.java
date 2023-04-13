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
package com.ms.silverking.net.async;

import com.ms.silverking.id.UUIDBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncSendListenerLoggingImpl implements AsyncSendListener {
  private static Logger log = LoggerFactory.getLogger(AsyncSendListenerLoggingImpl.class);

  private String traceIdStr;

  public AsyncSendListenerLoggingImpl(String traceIdStr) {
    this.traceIdStr = traceIdStr;
  }

  @Override
  public void sent(UUIDBase uuid) {
    log.trace(String.format("Async request has been sent: sendId=[%s] traceId=[%s]", uuid.toString(), traceIdStr));
  }

  @Override
  public void failed(UUIDBase uuid) {
    log.warn(String.format("Async request failed: sendId=[%s] traceId=[%s]", uuid.toString(), traceIdStr));
  }

  @Override
  public void timeout(UUIDBase uuid, long lastPollTime, long currPollTime, int currQueueSize, long creationTime, long deadline, long currTime) {
    log.warn(String.format("Async request timeout: sendId=[%s] traceId=[%s]", uuid.toString(), traceIdStr));
  }

  @Override
  public void pushIntoOutputQueue(UUIDBase uuid, int queueSize) {
    log.info(String.format("Async request pushIntoOutputQueue: sendId=[%s] traceId=[%s] queueSize=[%d]", uuid.toString(), traceIdStr, queueSize));
  }
}
