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
import java.util.concurrent.BlockingQueue;

import com.ms.silverking.collection.LightLinkedBlockingQueue;
import com.ms.silverking.thread.lwt.LWTThreadUtil;

/**
 * Active send. Allows waiting on the send and returning any error.
 */
public final class ActiveSend /*implements AsyncSendListener*/ {
  private final BlockingQueue<IOException> queue;
  //private final SynchronousQueue<IOException>    synchronousQueue;
  //    private final AsyncSendListener                listener;

  private static final IOException SUCCESS = new IOException();
  private static final int activeSendOfferTimeoutMillis = 1000;

  public ActiveSend(/*AsyncSendListener listener*/) {
    queue = new LightLinkedBlockingQueue<IOException>();
    //synchronousQueue = new SynchronousQueue<IOException>();
    //this.listener = listener;
  }

  public void setException(IOException exception) {
    try {
      queue.put(exception);
      //synchronousQueue.offer(exception, activeSendOfferTimeoutMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ie) {
    }
  }

  public void setSentSuccessfully() {
    try {
      queue.put(SUCCESS);
      //synchronousQueue.offer(SUCCESS, activeSendOfferTimeoutMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ie) {
    }
  }

  public void waitForCompletion() throws IOException {
    IOException result;

    LWTThreadUtil.setBlocked();
    try {
      result = queue.take();
      if (result != SUCCESS) {
        throw result;
      }
    } catch (InterruptedException ie) {
    } finally {
      LWTThreadUtil.setNonBlocked();
    }
  }
}
