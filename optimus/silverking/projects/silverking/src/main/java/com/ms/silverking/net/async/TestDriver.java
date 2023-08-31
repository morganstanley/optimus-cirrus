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

/** For development purposes. */
public class TestDriver {
  /*implements Runnable {
  private final SelectorController    selectorController;
  private final BlockingQueue<ServerSocketChannel>    acceptQueue;
  private final BlockingQueue<SocketChannel>    connectQueue;
  private final BlockingQueue<SocketChannel>    readQueue;
  private final BlockingQueue<SocketChannel>    writeQueue;
  private final ByteBuffer    readBuffer;
  private final ConcurrentHashMap<SocketChannel,Queue<ByteBuffer>>    outputQueues;

  public TestDriver() throws IOException {
      acceptQueue = new LinkedBlockingQueue<ServerSocketChannel>();
      connectQueue = new LinkedBlockingQueue<SocketChannel>();
      readQueue = new LinkedBlockingQueue<SocketChannel>();
      writeQueue = new LinkedBlockingQueue<SocketChannel>();
      readBuffer = ByteBuffer.allocate(8192);
      outputQueues = new ConcurrentHashMap<SocketChannel,Queue<ByteBuffer>>();
      selectorController = new SelectorController(acceptQueue, connectQueue, readQueue, writeQueue, false);
  }

  public void start() throws IOException {
      ServerSocketChannel    serverChannel;
      InetAddress            hostAddress;

      // Create a new non-blocking server socket channel
      serverChannel = ServerSocketChannel.open();
      serverChannel.configureBlocking(false);

      hostAddress = null;
      // Bind the server socket to the specified address and port
      InetSocketAddress isa = new InetSocketAddress(hostAddress, 9090);
      serverChannel.socket().bind(isa);

      selectorController.addServerChannel(serverChannel);
      new Thread(this, "TestDriver").start();
  }

  // this is for test purposes only, so we spin on all queues
  public void run() {
      while (true) {
          ServerSocketChannel    serverSocketChannel;
          SocketChannel    channel;

          serverSocketChannel = acceptQueue.poll();
          if (serverSocketChannel != null) {
              accept(serverSocketChannel);
          }
          channel = connectQueue.poll();
          if (channel != null) {
              connect(channel);
          }
          channel = readQueue.poll();
          if (channel != null) {
              read(channel);
          }
          channel = writeQueue.poll();
          if (channel != null) {
              write(channel);
          }
      }
  }

  private void accept(ServerSocketChannel channel) {
      try {
          SocketChannel        socketChannel;
          LinkedList<ByteBuffer>    outputQueue;

          socketChannel = channel.accept();
          socketChannel.configureBlocking(false);
          outputQueue = new LinkedList<ByteBuffer>();
          outputQueues.put(socketChannel, outputQueue);
          selectorController.addKeyChangeRequest(
                  new KeyChangeRequest(socketChannel,
                                      Type.ADD_AND_CHANGE_OPS,
                                      SelectionKey.OP_READ));
      } catch (IOException ioe) {
          ioe.printStackTrace();
      }
  }

  private void connect(SocketChannel channel) {
  }

  private void read(SocketChannel channel) {
      int                numRead;

      // Clear out our read buffer so it's ready for new data
      readBuffer.clear();
      // Attempt to read off the channel
      try {
          numRead = channel.read(readBuffer);
      } catch (IOException e) {
          selectorController.addKeyChangeRequest(
                  new KeyChangeRequest(channel,
                                      Type.CANCEL_AND_CLOSE));
          return;
      }

      if (numRead < 0) {
          // Remote entity shut the socket down cleanly. Do the
          // same from our end and cancel the channel.
          selectorController.addKeyChangeRequest(
                  new KeyChangeRequest(channel,
                                      Type.CANCEL_AND_CLOSE));
          return;
      }

      // Hand the data off to our worker thread
      receive(channel, readBuffer.array(), numRead);
  }

  private void receive(SocketChannel channel, byte[] buf, int length) {
      */
  /*
  Queue<ByteBuffer>    queue;
  String                msg;
  String                outMsg;

  msg = new String(buf, 0, length);
  System.out.println(msg);
  outMsg = msg;
  queue = outputQueues.get(channel);
  queue.add(ByteBuffer.wrap(outMsg.getBytes()));
  selectorController.addKeyChangeRequest(
          new KeyChangeRequest(channel,
                              Type.CHANGE_OPS,
                              SelectionKey.OP_READ | SelectionKey.OP_WRITE));
  */
  /*
  }

  private void write(SocketChannel channel) {
      */
  /*
  Queue<ByteBuffer>    queue;

  queue = outputQueues.get(channel);
  // Write until there's not more data ...
  try {
      while (!queue.isEmpty()) {
          ByteBuffer buf;

          buf = (ByteBuffer)queue.remove();
          channel.write(buf);
          if (buf.remaining() > 0) {
              // ... or the socket's buffer fills up
              break;
          }
          queue.remove(0);
      }

      if (queue.isEmpty()) {
          // We wrote away all data, so we're no longer interested
          // in writing on this socket. Switch back to waiting for
          // data.
          selectorController.addKeyChangeRequest(
                  new KeyChangeRequest(channel,
                                      Type.CHANGE_OPS,
                                      SelectionKey.OP_READ));
      }
  } catch (IOException ioe) {
      Log.logErrorWarning(ioe);
      selectorController.addKeyChangeRequest(
              new KeyChangeRequest(channel,
                                  Type.CHANGE_OPS,
                                  SelectionKey.OP_READ));
  }
  */
  /*
  }

  public static void main(String[] args) {
      try {
          TestDriver    testDriver;

          testDriver = new TestDriver();
          testDriver.start();
      } catch (Exception e) {
          e.printStackTrace();
      }
  }
  */
}
