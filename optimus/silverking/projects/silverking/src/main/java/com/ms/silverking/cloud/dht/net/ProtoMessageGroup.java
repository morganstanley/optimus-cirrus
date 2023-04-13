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
package com.ms.silverking.cloud.dht.net;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.ms.silverking.cloud.dht.ForwardingMode;
import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.io.util.BufferUtil;
import com.ms.silverking.text.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * ProtoMessageGroup and its descendants are used to create actual MessageGroup instances.
 */
public abstract class ProtoMessageGroup {
  private final MessageType type;
  private final int options;
  private final UUIDBase uuid;
  protected final long context;
  private final byte[] originator;
  protected final List<ByteBuffer> bufferList;
  private final int deadlineRelativeMillis;
  private final ForwardingMode forward;

  private static Logger log = LoggerFactory.getLogger(ProtoMessageGroup.class);

  protected static final boolean debug = false;

  private static final int bufferListInitialSize = 4;

  public ProtoMessageGroup(MessageType type, UUIDBase uuid, long context, byte[] originator, int deadlineRelativeMillis,
      ForwardingMode forward) {
    this.type = type;
    this.options = 0; // options are currently only used between peers; set to zero here
    this.uuid = uuid;
    this.context = context;
    assert originator != null && originator.length == ValueCreator.BYTES;
    this.originator = originator;
    this.deadlineRelativeMillis = deadlineRelativeMillis;
    this.forward = forward;
    bufferList = new ArrayList<>(bufferListInitialSize);
  }

  public UUIDBase getUUID() {
    return uuid;
  }

  public final List<ByteBuffer> getBufferList() {
    return bufferList;
  }

  public abstract boolean isNonEmpty();

  public MessageGroup toMessageGroup() {
    return toMessageGroup(true);
  }

  protected MessageGroup toMessageGroup(boolean flip) {
    MessageGroup mg;

    if (debug) {
      log.debug("toMessageGroup: {}" , flip);
      displayForDebug();
    }
    mg = new MessageGroup(type, options, uuid, context, flip ? BufferUtil.flip(getBufferList()) : getBufferList(),
        originator, deadlineRelativeMillis, forward);
    if (debug) {
      mg.displayForDebug();
    }
    return mg;
  }

  protected void displayForDebug() {
    for (int i = 0; i < bufferList.size(); i++) {
      log.debug("{} {}",i ,bufferList.get(i));
      if (bufferList.get(i).limit() < 128) {
        System.out.println(StringUtil.byteBufferToHexString((ByteBuffer) bufferList.get(i).duplicate().position(0)));
      } else {
        log.info("");
      }
    }
  }

  /**
   * Convert this ProtoMessageGroup to a MessageGroup and add it to the given list
   *
   * @param messageGroups list to add this to
   */
  public void addToMessageGroupList(List<MessageGroup> messageGroups) {
    if (isNonEmpty()) {
      MessageGroup messageGroup;

      messageGroup = toMessageGroup();
      messageGroups.add(messageGroup);
      if (log.isDebugEnabled()) {
        messageGroup.displayForDebug();
      }
    }
  }
}
