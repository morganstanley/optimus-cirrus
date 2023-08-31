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
package com.ms.silverking.cloud.dht.client.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.cloud.dht.net.ProtoMessageGroup;
import com.ms.silverking.net.AddrAndPort;
import com.ms.silverking.net.async.QueueingConnectionLimitListener;
import com.ms.silverking.thread.lwt.GroupingPausingBaseWorker;
import com.ms.silverking.thread.lwt.LWTPool;
import com.ms.silverking.thread.lwt.LWTPoolProvider;
import com.ms.silverking.util.PropertiesHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class OpSender extends GroupingPausingBaseWorker<AsyncOperationImpl>
    implements QueueingConnectionLimitListener {
  private final MessageGroupBase mgBase;
  private final AddrAndPort dest;
  private final AtomicLong doWorkCalls;
  private final AtomicLong doGroupedWorkCalls;

  private static Logger log = LoggerFactory.getLogger(OpSender.class);
  private static final boolean trackCallStats = false;

  public static final String opGroupingEnabledProperty =
      OpSender.class.getPackage().getName() + ".OpGroupingEnabled";
  public static final boolean defaultOpGroupingEnabled = true;
  static final boolean opGroupingEnabled;

  static {
    opGroupingEnabled =
        PropertiesHelper.systemHelper.getBoolean(
            opGroupingEnabledProperty, defaultOpGroupingEnabled);
  }

  private static final int idleThreadsThreshold =
      Integer.MAX_VALUE; // Don't queue work when possible
  // private static final int idleThreadsThreshold = LWTConstants.defaultIdleThreadThreshold; //
  // Queue most work

  private static LWTPool senderPool;

  static {
    /*
    LWTPoolParameters   params;

    params = LWTPoolParameters.create("senderPool").targetSize(2).workUnit(512);
    senderPool = LWTPoolProvider.createPool(params);
    //senderPool.dumpStatsOnShutdown();
     */
    senderPool = LWTPoolProvider.defaultConcurrentWorkPool;
  }

  OpSender(AddrAndPort dest, MessageGroupBase mgBase) {
    super(senderPool, true, Integer.MAX_VALUE, idleThreadsThreshold);
    // super(senderPool, true, 0, LWTConstants.defaultIdleThreadThreshold);
    // super(senderPool, true, LWTConstants.defaultMaxDirectCallDepth,
    // LWTConstants.defaultIdleThreadThreshold);
    this.dest = dest;
    this.mgBase = mgBase;
    doWorkCalls = new AtomicLong();
    doGroupedWorkCalls = new AtomicLong();
  }

  @Override
  public AsyncOperationImpl[] newWorkArray(int size) {
    return new AsyncOperationImpl[size];
  }

  @Override
  public void queueAboveLimit() {
    super.pause();
  }

  @Override
  public void queueBelowLimit() {
    super.unpause();
  }

  @Override
  public void doWork(AsyncOperationImpl asyncOpImpl) {
    AsyncOperationImpl[] asyncOpImpls;

    if (trackCallStats) {
      doWorkCalls.incrementAndGet();
    }
    asyncOpImpls = new AsyncOperationImpl[1];
    asyncOpImpls[0] = asyncOpImpl;
    createMessagesForIncomplete(asyncOpImpls);
  }

  @Override
  public void doWork(AsyncOperationImpl[] asyncOpImpls) {
    if (trackCallStats) {
      doGroupedWorkCalls.incrementAndGet();
    }
    // showQ();
    createMessagesForIncomplete(asyncOpImpls);
  }

  /**
   * Breaks asyncOpImpls into groups of same-typed operations calling createMessages() for each
   * group.
   *
   * @param asyncOpImpls
   */
  /*
   * this implementation was before we took care of incompatible ops
  private void createMessagesForIncomplete(AsyncOperationImpl[] asyncOpImpls) {
      if (opGroupingEnabled) {
          createMessages(asyncOpImpls, 0, asyncOpImpls.length - 1);
      } else {
          for (int i = 0; i < asyncOpImpls.length; i++) {
              createMessages(asyncOpImpls, i, i);
          }
      }
  }
  */
  /*
   * this implementation allowed for messages of different types, we now assume the same type
   * since we have one OpSender per type
  private void createMessagesForIncomplete(AsyncOperationImpl[] asyncOpImpls) {
      int startIndex;
      int endIndex;

      //System.out.printf("single %d\tgrouped%d\n", doWorkCalls.get(), doGroupedWorkCalls.get());
      startIndex = 0;
      endIndex = startIndex + 1;
      while (startIndex < asyncOpImpls.length) {
          if (opGroupingEnabled) {
              while (endIndex < asyncOpImpls.length && asyncOpImpls[endIndex].getType() == asyncOpImpls[startIndex]
              * .getType()) {
                  endIndex++;
              }
              createMessages(asyncOpImpls, startIndex, endIndex - 1);
              startIndex = endIndex;
          } else {
              createMessages(asyncOpImpls, startIndex, startIndex);
              startIndex++;
          }
      }
  }
  */

  private static final OpGroupingComparator opGroupingComparator = new OpGroupingComparator();

  private static class OpGroupingComparator implements Comparator<AsyncOperationImpl> {
    @Override
    public int compare(AsyncOperationImpl o1, AsyncOperationImpl o2) {
      if (o1.getClass() == o2.getClass()) {
        if (o1 instanceof AsyncPutOperationImpl) {
          return AsyncPutOperationImplComparator.instance.compare(
              (AsyncPutOperationImpl) o1, (AsyncPutOperationImpl) o2);
        } else if (o1 instanceof AsyncRetrievalOperationImpl) {
          return AsyncRetrievalOperationImplComparator.instance.compare(
              (AsyncRetrievalOperationImpl) o1, (AsyncRetrievalOperationImpl) o2);
        } else {
          return 0;
        }
      } else {
        if (o1.getClass() == AsyncPutOperationImpl.class) {
          return 1;
        } else {
          return -1;
        }
      }
    }
  }

  private static void groupOps(AsyncOperationImpl[] asyncOpImpls) {
    Arrays.sort(asyncOpImpls, opGroupingComparator);
  }

  private void createMessagesForIncomplete(AsyncOperationImpl[] asyncOpImpls) {
    int startIndex;
    int endIndex;

    if (asyncOpImpls.length > 2) {
      groupOps(asyncOpImpls);
    }

    // System.out.printf("single %d\tgrouped%d\n", doWorkCalls.get(), doGroupedWorkCalls.get());
    startIndex = 0;
    endIndex = startIndex + 1;
    while (startIndex < asyncOpImpls.length) {
      if (opGroupingEnabled) {
        while (endIndex < asyncOpImpls.length
            && asyncOpImpls[startIndex].canBeGroupedWith(asyncOpImpls[endIndex])) {
          endIndex++;
        }
        createMessages(asyncOpImpls, startIndex, endIndex - 1);
        startIndex = endIndex;
      } else {
        createMessages(asyncOpImpls, startIndex, startIndex);
        startIndex++;
      }
    }
  }

  private MessageEstimate estimateMessage(
      AsyncOperationImpl[] asyncOpImpls, int startIndex, int endIndex) {
    MessageEstimate estimate;

    estimate = asyncOpImpls[startIndex].createMessageEstimate();
    for (int i = startIndex; i <= endIndex; i++) {
      asyncOpImpls[i].addToEstimate(estimate);
    }
    return estimate;
  }

  /**
   * Given a group of same-typed operations, create and send messages.
   *
   * @param asyncOpImpls
   * @param startIndex
   * @param endIndex
   */
  private void createMessages(AsyncOperationImpl[] asyncOpImpls, int startIndex, int endIndex) {
    ProtoMessageGroup protoMG;
    List<MessageGroup> messageGroups;
    MessageEstimate estimate;

    estimate = estimateMessage(asyncOpImpls, startIndex, endIndex);
    protoMG = asyncOpImpls[startIndex].createProtoMG(estimate);

    messageGroups = new ArrayList<>();
    // Walk through all of the compatible operations.
    // For each operation, add to the current protoMG when possible. If not possible,
    // then create a new protoMG and add the old to the messageGroups list.
    for (int i = startIndex; i <= endIndex; i++) {
      protoMG = asyncOpImpls[i].createMessagesForIncomplete(protoMG, messageGroups, estimate);
    }
    // Add the final protoMG to the list if it is non-empty
    protoMG.addToMessageGroupList(messageGroups);
    // Send all MessageGroups
    // FUTURE - consider sending sooner, in creation loop
    for (MessageGroup messageGroup : messageGroups) {
      send(messageGroup);
    }
  }

  void send(MessageGroup messageGroup) {
    mgBase.send(messageGroup, dest);
  }
}
