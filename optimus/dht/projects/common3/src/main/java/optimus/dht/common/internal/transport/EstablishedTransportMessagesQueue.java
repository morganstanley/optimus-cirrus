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
package optimus.dht.common.internal.transport;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.jctools.queues.MpscUnboundedArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import optimus.dht.common.api.transport.MessageStream;
import optimus.dht.common.api.transport.SentMessageMetrics;

public class EstablishedTransportMessagesQueue implements FailableTransportMessagesQueue {

  private static final Logger logger =
      LoggerFactory.getLogger(EstablishedTransportMessagesQueue.class);

  public static class QueueElement {

    private final short moduleId;
    private final MessageStream messageStream;
    private final MutableSentMessageMetrics metrics;
    private final Consumer<SentMessageMetrics> metricsCallback;

    public QueueElement(
        short moduleId,
        MessageStream messageStream,
        MutableSentMessageMetrics metrics,
        Consumer<SentMessageMetrics> metricsCallback) {
      this.moduleId = moduleId;
      this.messageStream = messageStream;
      this.metrics = metrics;
      this.metricsCallback = metricsCallback;
    }

    public MessageStream messageStream() {
      return messageStream;
    }

    public short moduleId() {
      return moduleId;
    }

    public long size() {
      return messageStream.size();
    }

    public MutableSentMessageMetrics metrics() {
      return metrics;
    }

    public Consumer<SentMessageMetrics> metricsCallback() {
      return metricsCallback;
    }
  }

  protected final MpscUnboundedArrayQueue<QueueElement> messages =
      new MpscUnboundedArrayQueue<>(1024);
  protected final AtomicLong queueElementsSize = new AtomicLong(0);
  protected QueueElement mostRecentElement;

  public void addMessage(
      short moduleId,
      MessageStream messageStream,
      MutableSentMessageMetrics metrics,
      Consumer<SentMessageMetrics> metricsCallback) {
    QueueElement queueElement = new QueueElement(moduleId, messageStream, metrics, metricsCallback);
    // we can update messages and queueElementsSize not atomically, as queueElementsSize is just a
    // hint for
    // upcoming backpressure handling mechanism and when allocating new write buffer
    messages.add(queueElement);
    queueElementsSize.addAndGet(queueElement.size());
  }

  public boolean offerMessage(
      short moduleId,
      MessageStream messageStream,
      MutableSentMessageMetrics metrics,
      Consumer<SentMessageMetrics> metricsCallback) {
    // To be added later as part of backpressure handling premium package
    throw new UnsupportedOperationException("Not yet supported");
  }

  public QueueElement poll() {
    mostRecentElement = messages.poll();
    if (mostRecentElement != null) {
      queueElementsSize.addAndGet(-mostRecentElement.messageStream().size());
    }
    return mostRecentElement;
  }

  public void clearMostRecentElement() {
    mostRecentElement = null;
  }

  public boolean isEmpty() {
    return messages.isEmpty();
  }

  public long queueElementsSize() {
    return queueElementsSize.get();
  }

  @Override
  public void failAll(Throwable exception) {
    try {
      if (mostRecentElement != null) {
        mostRecentElement.messageStream().messageFailed(exception);
      }
    } catch (Exception e) {
      logger.warn("Exception while calling messageFailed", e);
    }
    QueueElement element;
    while ((element = messages.poll()) != null) {
      try {
        element.messageStream.messageFailed(exception);
      } catch (Exception e) {
        logger.warn("Exception while calling messageFailed", e);
      }
    }
  }
}
