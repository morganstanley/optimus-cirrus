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

import java.io.IOException;
import java.nio.channels.SocketChannel;

import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.time.AbsMillisTimeSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OutgoingData base implementation. Associates data with a unique send id,
 * the deadline by which the data should be sent, and a callback to
 * be informed upon successful send or upon failure.
 */
public abstract class OutgoingData {
  private final UUIDBase sendUUID; // FUTURE - probably change this from uuid to long
  // uuid is really overkill for how this is used
  private final AsyncSendListener sendListener;
  private final long creationTime;
  private final long deadline;
  private final Priority priority;

  private static Logger log = LoggerFactory.getLogger(OutgoingData.class);

  private static AbsMillisTimeSource absMillisTimeSource;

  public static final int minRelativeDeadline = 5000;

  public static long currAbsMillis() {
    return absMillisTimeSource.absTimeMillis();
  }

  /**
   * Must be called to set the local time source.
   *
   * @param _absMillisTimeSource
   */
  public static void setAbsMillisTimeSource(AbsMillisTimeSource _absMillisTimeSource) {
    absMillisTimeSource = _absMillisTimeSource;
  }

  public enum Priority {NORMAL, HIGH}

  public OutgoingData(UUIDBase sendUUID, AsyncSendListener sendListener, long deadline, Priority priority) {
    this.sendUUID = sendUUID;
    this.sendListener = sendListener;
    this.priority = priority;
    this.creationTime = absMillisTimeSource.absTimeMillis();
    if (deadline <= creationTime) {
      log.warn("deadline <= creationTime; {} < {} with time source {}", deadline, creationTime,
          absMillisTimeSource.name());
      //throw new RuntimeException("deadline <= creationTime");
      Thread.dumpStack();
      for (StackTraceElement stack : Thread.currentThread().getStackTrace()) {
        log.warn(stack.toString());
      }
      deadline = creationTime + minRelativeDeadline;
    }
    this.deadline = deadline;
  }

  public OutgoingData(UUIDBase sendUUID, AsyncSendListener sendListener, long deadline) {
    this(sendUUID, sendListener, deadline, Priority.NORMAL);
  }

  public final UUIDBase getSendUUID() {
    return sendUUID;
  }

  public final long getDeadline() {
    return deadline;
  }

  public final boolean deadlineExpired() {
    return absMillisTimeSource.absTimeMillis() > deadline;
  }

  public final Priority getPriority() {
    return priority;
  }

  public abstract boolean writeToChannel(SocketChannel channel) throws IOException;

  public abstract long getTotalBytes();

  protected final void successful() {
    if (sendListener != null && sendUUID != null) {
      sendListener.sent(sendUUID);
    }
  }

  protected final void failed() {
    if (sendListener != null && sendUUID != null) {
      sendListener.failed(sendUUID);
    }
    displayForDebug();
  }

  protected final void timeout(long lastPollTime, long currPollTime, int currQueueSize) {
    if (sendListener != null && sendUUID != null) {
      sendListener.timeout(sendUUID, lastPollTime, currPollTime, currQueueSize, creationTime, deadline, absMillisTimeSource.absTimeMillis());
    }
    displayForDebug();
  }

  protected final void pushIntoOutputQueue(int queueSize) {
    if (sendListener != null && sendUUID != null) {
      sendListener.pushIntoOutputQueue(sendUUID, queueSize);
    }
  }

  public void displayForDebug() {
    log.warn("sendUuid {} creationTime {} deadline {} curTimeMillis {} {}", sendUUID, creationTime, deadline,
        absMillisTimeSource.absTimeMillis(),
        absMillisTimeSource.absTimeMillis() <= deadline ? "Data has NOT timed out" : "Data has timed out!");
  }
}
