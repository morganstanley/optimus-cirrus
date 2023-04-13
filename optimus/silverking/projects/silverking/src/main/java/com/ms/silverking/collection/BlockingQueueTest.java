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
package com.ms.silverking.collection;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class BlockingQueueTest implements Runnable {
  private final int readers;
  private final int writers;
  private final long readerDelayNanos;
  private final long writerDelayNanos;
  private final BlockingQueue<Integer> q;
  private final AtomicInteger threadID;
  private boolean running;
  private final int[] workComplete;
  private final Thread[] threads;
  private final boolean[] isReader;
  private final Stopwatch sw;

  private static Logger log = LoggerFactory.getLogger(BlockingQueueTest.class);

  public BlockingQueueTest(int readers, int writers, long readerDelayNanos, long writerDelayNanos,
      BlockingQueue<Integer> q) {
    this.readers = readers;
    this.writers = writers;
    this.readerDelayNanos = readerDelayNanos;
    this.writerDelayNanos = writerDelayNanos;
    this.q = q;
    threadID = new AtomicInteger();
    workComplete = new int[readers + writers];
    threads = new Thread[readers + writers];
    isReader = new boolean[readers + writers];
    sw = new SimpleStopwatch();
  }

  public void runTest() {
    running = true;
    sw.reset();
    for (int i = 0; i < readers + writers; i++) {
      threads[i] = new Thread(this);
      threads[i].start();
    }
  }

  public void stopTest() {
    running = false;
    for (int i = 0; i < workComplete.length; i++) {
      try {
        threads[i].join();
      } catch (InterruptedException ie) {
      }
    }
    sw.stop();
  }

  public void run() {
    int myID;
    boolean _isReader;
    int myWorkComplete;
    boolean useTransfer;

    //useTransfer = q instanceof TransferQueue;
    useTransfer = false;
    myID = threadID.getAndIncrement();
    _isReader = myID < readers;
    myWorkComplete = 0;
    while (running) {
      int iterationWork;

      if (_isReader) {
        iterationWork = read();
      } else {
        if (useTransfer) {
          iterationWork = transfer();
        } else {
          iterationWork = write();
        }
      }
      myWorkComplete += iterationWork;
    }
    workComplete[myID] = myWorkComplete;
    isReader[myID] = _isReader;
  }

  private int read() {
    Integer myInt;
    int myWorkComplete;

    try {
      myInt = q.poll(1, TimeUnit.SECONDS);
    } catch (InterruptedException ie) {
      myInt = null;
    }
    delayNanos(readerDelayNanos);
    if (myInt == null) {
      myWorkComplete = 0;
    } else {
      myWorkComplete = 1;
    }
    return myWorkComplete;
  }

  private int write() {
    try {
      q.put(0);
      delayNanos(writerDelayNanos);
    } catch (InterruptedException ie) {
    }
    return 1;
  }

  private int transfer() {
    try {
      ((TransferQueue) q).tryTransfer(1, 1, TimeUnit.SECONDS);
    } catch (InterruptedException ie) {
    }
    return 1;
  }

  private void delayNanos(long nanos) {
    if (nanos > 0) {
      long t1;
      long t2;

      // ignore wrap for this simple test
      t1 = System.nanoTime();
      do {
        t2 = System.nanoTime();
      } while (t2 - t1 < nanos);
    }
  }

  private void displayResults() {
    int totalComplete;
    int readsComplete;

    readsComplete = 0;
    totalComplete = 0;
    for (int i = 0; i < workComplete.length; i++) {
      totalComplete += workComplete[i];
      if (isReader[i]) {
        readsComplete += workComplete[i];
      }
      log.info("{} {} {} {}", i, isReader[i], workComplete[i],
          (sw.getElapsedSeconds() / (double) workComplete[i]));
      //System.out.println(i +"\t"+ (isReader[i] ? "reader" : "writer") +"\t"+ workComplete[i]);
    }
    log.info("{} {}", readsComplete, (sw.getElapsedSeconds() / (double) readsComplete));
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      BlockingQueueTest bqt;
      int readers;
      int writers;
      long readerDelayNanos;
      long writerDelayNanos;
      int duration;
      BlockingQueue<Integer> q;
      String[] qDefs;

      if (args.length != 6) {
        log.info(
            "args: <readers> <writers> <duration> <readerDelayNanos> <writerDelayNanos> " +
                "<LightLinkedBlockingQueue|LinkedBlockingQueue|SpinningTransferQueue|SynchronousQueue>");
        return;
      }
      readers = Integer.parseInt(args[0]);
      writers = Integer.parseInt(args[1]);
      duration = Integer.parseInt(args[2]);
      readerDelayNanos = Long.parseLong(args[3]);
      writerDelayNanos = Long.parseLong(args[4]);
      qDefs = args[5].split(",");
      for (String qDef : qDefs) {
        log.info("Queue type: {}" , qDef);
        if (qDef.equals("LightLinkedBlockingQueue")) {
          q = new LightLinkedBlockingQueue<Integer>(0);
          //q = new LightLinkedBlockingQueue<Integer>(10000);
          //q = new LightLinkedBlockingQueue<Integer>(20000);
        } else if (qDef.equals("LinkedBlockingQueue")) {
          q = new LinkedBlockingQueue<Integer>();
        } else if (qDef.equals("LinkedTransferQueue")) {
          q = new LinkedTransferQueue<Integer>();
        } else if (qDef.equals("SpinningTransferQueue")) {
          q = new SpinningTransferQueue<Integer>();
        } else if (qDef.equals("SynchronousQueue")) {
          q = new LinkedBlockingQueue<Integer>();
          if (readers != 1 || writers != 1) {
            throw new RuntimeException("readers/writers must be 1 for SynchronousQueue");
          }
        } else {
          throw new RuntimeException("Unknown queue:" + qDef);
        }
        bqt = new BlockingQueueTest(readers, writers, readerDelayNanos, writerDelayNanos, q);
        log.info("Running test");
        bqt.runTest();
        ThreadUtil.sleepSeconds(duration);
        bqt.stopTest();
        bqt.displayResults();
        log.info("Test complete");
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
