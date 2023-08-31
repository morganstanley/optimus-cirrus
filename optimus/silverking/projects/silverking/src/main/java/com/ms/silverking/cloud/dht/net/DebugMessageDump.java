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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.ms.silverking.cloud.dht.ForwardingMode;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.io.StreamParser;
import com.ms.silverking.text.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebugMessageDump {
  private final MessageType messageType;

  public DebugMessageDump(MessageType messageType) {
    this.messageType = messageType;
  }

  private static Logger log = LoggerFactory.getLogger(IncomingMessageGroup.class);

  public void debug(File dumpFile) throws IOException {
    MessageGroup mg;

    mg = parse(dumpFile);
    // System.out.println(mg);
    // mg.displayForDebug(true);

    for (MessageGroupKVEntry entry : mg.getPutValueKeyIterator(ChecksumType.MD5)) {
      log.debug("Entry: {}", entry);
    }
  }

  public MessageGroup parse(File dumpFile) throws IOException {
    return parse(StreamParser.parseFileLines(dumpFile));
  }

  public MessageGroup parse(List<String> lines) {
    List<ByteBuffer> buffers;

    buffers = getBuffers(lines);
    return new MessageGroup(
        messageType,
        0, // options
        new UUIDBase(0, 0),
        0L, // context
        buffers.toArray(new ByteBuffer[0]),
        new byte[6], // originator
        0, // deadlineRelativeMillis
        ForwardingMode.DO_NOT_FORWARD); // forward
  }

  private List<ByteBuffer> getBuffers(List<String> lines) {
    List<ByteBuffer> buffers;
    int i;

    buffers = new ArrayList<>(lines.size() / 2);
    i = 0;
    while (i < lines.size()) {
      BufferInfo bufferInfo;
      ByteBuffer buf;

      bufferInfo = new BufferInfo(lines.get(i));
      buf = StringUtil.hexStringToByteBuffer(lines.get(i + 1));
      // System.out.println(bufferInfo +"\t"+ buf);
      buffers.add(buf);
      i += 2;
    }
    return buffers;
  }

  class BufferInfo {
    final int index;
    final int pos;
    final int lim;
    final int cap;

    // 0       java.nio.HeapByteBuffer[pos=0 lim=1794 cap=1794]
    BufferInfo(String s) {
      int i;

      s = s.replace(']', ' ').replace('[', ' ');
      index = Integer.parseInt(s.split("\\s+")[0]);
      i = s.indexOf("=") + 1;
      pos = Integer.parseInt(s.substring(i).split("\\s+")[0]);
      i = s.indexOf("=", i) + 1;
      lim = Integer.parseInt(s.substring(i).split("\\s+")[0]);
      i = s.indexOf("=", i) + 1;
      cap = Integer.parseInt(s.substring(i).split("\\s+")[0]);
    }

    @Override
    public String toString() {
      return String.format("%d\tpos=%d lim=%d cap=%d", index, pos, lim, cap);
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      if (args.length != 2) {
        System.out.println("Usage: <MessageType> <dumpFile>");
      } else {
        DebugMessageDump dmd;
        MessageType messageType;
        File dumpFile;

        messageType = MessageType.valueOf(args[0]);
        dumpFile = new File(args[1]);
        dmd = new DebugMessageDump(messageType);
        dmd.debug(dumpFile);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
