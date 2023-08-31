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
package com.ms.silverking.net.test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import com.ms.silverking.net.async.NewIncomingBufferedData;
import com.ms.silverking.net.async.OutgoingData;
import com.ms.silverking.net.async.PersistentAsyncServer;
import com.ms.silverking.numeric.NumConversion;
import com.ms.silverking.numeric.RingInteger;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.thread.lwt.LWTPoolProvider;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;
import com.ms.silverking.time.TimerDrivenTimeSource;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class PingPongTest implements BufferedDataReceiver {
  private final InetSocketAddress serverAddr;
  private final Mode mode;
  private final PersistentAsyncServer<BufferedDataConnection> paServer;
  private Semaphore[] semaphores;
  private AtomicInteger messagesSent;
  private AtomicInteger messagesReceived;
  private RingInteger nextSemaphore;
  private int clientPort;
  private byte[] payloadBuffer;

  private static Logger log = LoggerFactory.getLogger(PingPongTest.class);

  public enum Test {
    BufferedDataConnection
  };

  public enum Mode {
    client,
    server
  };

  public enum BatchMode {
    PingPong,
    BatchPingPong
  };

  private static final int testServerPort = 7627;
  private static final int numSelectorControllers = 8;
  private static final String selectorControllerClass = "PingPong";
  private static final int iterations = 1;
  private static final double displayIntervalSeconds = 10.0;

  // private static final int   payloadSize = 100;
  private static final int payloadSize = 100 * 1000;

  private static final double extraSeconds = 60.0;

  private static final BatchMode batchMode = BatchMode.BatchPingPong;

  static {
    OutgoingData.setAbsMillisTimeSource(new TimerDrivenTimeSource());
  }

  public PingPongTest(Mode mode, String serverHost) throws IOException {
    messagesSent = new AtomicInteger();
    messagesReceived = new AtomicInteger();
    this.mode = mode;
    switch (mode) {
      case client:
        serverAddr = new InetSocketAddress(serverHost, testServerPort);
        paServer =
            new PersistentAsyncServer<>(
                0,
                new BufferedDataConnectionCreator(this),
                numSelectorControllers,
                selectorControllerClass);
        clientPort = paServer.getPort();
        break;
      case server:
        serverAddr = null;
        paServer =
            new PersistentAsyncServer<>(
                testServerPort,
                new BufferedDataConnectionCreator(this),
                numSelectorControllers,
                selectorControllerClass);
        break;
      default:
        throw new RuntimeException("panic");
    }
    payloadBuffer = new byte[payloadSize];
    paServer.enable();
  }

  public void runTest(Test test, double durationSeconds, int index) throws IOException {
    switch (test) {
      case BufferedDataConnection:
        runBufferedDataConnectionTest(durationSeconds, index);
        break;
      default:
        throw new RuntimeException("panic");
    }
  }

  private void runBufferedDataConnectionTest(double durationSeconds, int index) throws IOException {
    switch (mode) {
      case client:
        runQueueingConnectionTestClient(durationSeconds, index);
        break;
      case server:
        runQueueingConnectionTestServer(durationSeconds);
        break;
      default:
        throw new RuntimeException("panic");
    }
  }

  private void runQueueingConnectionTestServer(double durationSeconds) {
    ThreadUtil.sleepSeconds(durationSeconds + extraSeconds);
  }

  private void runQueueingConnectionTestClient(double durationSeconds, int index)
      throws IOException {
    ByteBuffer[] msg;
    Stopwatch sw;
    Stopwatch displaySW;
    Semaphore semaphore;

    semaphore = semaphores[index];
    // msg = createMessage(NumConversion.intToBytes(index));
    sw = new SimpleStopwatch();
    displaySW = new SimpleStopwatch();
    do {
      // msg = createMessage(NumConversion.intToBytes(clientPort));
      NumConversion.intToBytes(clientPort, payloadBuffer);
      msg = createMessage(payloadBuffer);
      // System.out.println("Sending");
      messagesSent.incrementAndGet();
      paServer.send(serverAddr, msg, false, Long.MAX_VALUE);
      if (batchMode == BatchMode.PingPong) {
        try {
          semaphore.acquire();
        } catch (InterruptedException ie) {
        }
      }
      // System.out.println("past semaphore");
      // rewindBuffers(msg);
      if (displaySW.getSplitSeconds() > displayIntervalSeconds) {
        displayStats(sw.getSplitSeconds());
        displaySW.reset();
      }
    } while (sw.getSplitSeconds() < durationSeconds);
    if (batchMode == BatchMode.BatchPingPong) {
      try {
        semaphore.acquire(messagesSent.get());
      } catch (InterruptedException ie) {
      }
    }
    sw.stop();
    displayStats(sw.getElapsedSeconds());
  }

  private void displayStats(double time) {
    log.info("messagesReceived: {}  duration {}", messagesReceived.get(), time);
    log.info("messagesSent: {}  duration {}", messagesReceived.get(), time);
    log.info("messageRate (msgs/s): {}", (double) messagesReceived.get() / time);
    log.info("messageLatency (s):  {}", (double) time / (double) messagesReceived.get());
  }

  private void rewindBuffers(ByteBuffer[] buffers) {
    for (ByteBuffer buffer : buffers) {
      buffer.rewind();
    }
  }

  private ByteBuffer[] createMessage(byte[] payload) {
    ByteBuffer[] msg;

    msg = new ByteBuffer[4];
    /*
    msg[0] = ByteBuffer.wrap(NewIncomingBufferedData.preamble);
    msg[1] = ByteBuffer.wrap(NumConversion.intToBytes(1));
    msg[2] = ByteBuffer.wrap(NumConversion.intToBytes(payload.length));
    msg[3] = ByteBuffer.wrap(payload);
    */
    /**/
    msg[0] = wrapInDirect(NewIncomingBufferedData.preamble);
    msg[1] = wrapInDirect(NumConversion.intToBytes(1));
    msg[2] = wrapInDirect(NumConversion.intToBytes(payload.length));
    msg[3] = wrapInDirect(payload);
    /**/
    return msg;
  }

  private ByteBuffer wrapInDirect(byte[] a) {
    ByteBuffer buf;

    buf = ByteBuffer.allocateDirect(a.length);
    buf.put(a);
    buf.rewind();
    return buf;
  }

  @Override
  public void receive(ByteBuffer[] bufferedData, BufferedDataConnection connection) {
    IntBuffer intBuffer;
    int id;

    // System.out.println(bufferedData[0].capacity() +" "+ bufferedData[0]);
    bufferedData[0].rewind();
    intBuffer = bufferedData[0].asIntBuffer();
    id = intBuffer.get();
    // System.out.printf("Received message %x with id %d\n", id, id);
    // System.out.println("Received data");
    // System.out.flush();
    switch (mode) {
      case client:
        /*
        int semaphoreIndex;

        synchronized (this) {
            semaphoreIndex = nextSemaphore.getValue();
            nextSemaphore.inc();
        }
        */
        // System.out.println("release semaphoreIndex: "+ id);
        semaphores[0].release();
        messagesReceived.incrementAndGet();
        break;
      case server:
        try {
          InetSocketAddress other;
          ByteBuffer[] msg;
          byte[] b;
          byte[] b2;

          other = new InetSocketAddress(connection.getRemoteSocketAddress().getHostName(), id);
          // msg = createMessage(NumConversion.intToBytes(id));
          NumConversion.intToBytes(id, payloadBuffer);
          msg = createMessage(payloadBuffer);
          paServer.send(other, msg, false, Long.MAX_VALUE);
          // connection.sendAsynchronous(bufferedData, Long.MAX_VALUE);
        } catch (IOException ioe) {
          ioe.printStackTrace();
        }
        break;
      default:
        throw new RuntimeException("panic");
    }
  }

  public void threadedTest(Test test, double durationSeconds, int threads) {
    nextSemaphore = new RingInteger(0, threads - 1, 0);
    semaphores = new Semaphore[threads];
    for (int i = 0; i < threads; i++) {
      semaphores[i] = new Semaphore(0);
    }
    for (int i = 0; i < threads; i++) {
      new TestRunner(test, durationSeconds, i);
    }
  }

  class TestRunner implements Runnable {
    private final Test test;
    private final double durationSeconds;
    private final int index;

    public TestRunner(Test test, double durationSeconds, int index) {
      this.test = test;
      this.durationSeconds = durationSeconds;
      this.index = index;
      new Thread(this).start();
    }

    @Override
    public void run() {
      try {
        runTest(test, durationSeconds, index);
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      if (args.length != 3 && args.length != 5) {
        System.out.println("server <test> <durationSeconds>");
        System.out.println("or");
        System.out.println("client <test> <durationSeconds> <threads> <serverHost>");
      } else {
        Mode mode;
        Test test;
        PingPongTest pingPongTest;
        double durationSeconds;

        LWTPoolProvider.createDefaultWorkPools();
        mode = Mode.valueOf(args[0]);
        test = Test.valueOf(args[1]);
        durationSeconds = Double.parseDouble(args[2]);
        // Log.setLevelAll();
        switch (mode) {
          case server:
            pingPongTest = new PingPongTest(mode, null);
            pingPongTest.runTest(test, durationSeconds, 0);
            break;
          case client:
            String serverHost;
            int threads;

            serverHost = args[4];
            pingPongTest = new PingPongTest(mode, serverHost);
            threads = Integer.parseInt(args[3]);
            pingPongTest.threadedTest(test, durationSeconds, threads);
            break;
          default:
            throw new RuntimeException("panic");
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
