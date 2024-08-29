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

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import optimus.dht.common.api.transport.MessageGenerator;
import optimus.dht.common.api.transport.SentMessageMetrics;

public class InitialTransportMessagesQueue implements FailableTransportMessagesQueue {

  private static final Logger logger = LoggerFactory.getLogger(InitialTransportMessagesQueue.class);

  public static class QueueElement {
    private final short moduleId;
    private final MessageGenerator messageGenerator;
    private final long estimatedSize;
    private final MutableSentMessageMetrics metrics = new MutableSentMessageMetrics();
    private final Consumer<SentMessageMetrics> metricsCallback;

    public QueueElement(
        short moduleId,
        MessageGenerator messageGenerator,
        Consumer<SentMessageMetrics> metricsCallback) {
      this.moduleId = moduleId;
      this.messageGenerator = messageGenerator;
      this.metricsCallback = metricsCallback;
      this.estimatedSize = messageGenerator.estimatedSize();
    }

    public MessageGenerator messageGenerator() {
      return messageGenerator;
    }

    public short moduleId() {
      return moduleId;
    }

    public long estimatedSize() {
      return estimatedSize;
    }

    public MutableSentMessageMetrics metrics() {
      return metrics;
    }

    public Consumer<SentMessageMetrics> metricsCallback() {
      return metricsCallback;
    }
  }

  protected final BlockingDeque<QueueElement> messages = new LinkedBlockingDeque<>();
  protected final AtomicLong estimatedQueueSize = new AtomicLong(0);

  public void addMessage(
      short moduleId,
      MessageGenerator messageGenerator,
      Consumer<SentMessageMetrics> metricsCallback) {
    QueueElement queueElement = new QueueElement(moduleId, messageGenerator, metricsCallback);
    messages.addLast(queueElement);
    estimatedQueueSize.addAndGet(queueElement.estimatedSize());
  }

  public boolean offerMessage(
      short moduleId,
      MessageGenerator messageGenerator,
      Consumer<SentMessageMetrics> metricsCallback) {
    QueueElement queueElement = new QueueElement(moduleId, messageGenerator, metricsCallback);
    boolean added = messages.offerLast(queueElement);
    if (added) {
      estimatedQueueSize.addAndGet(queueElement.estimatedSize());
    }
    return added;
  }

  public QueueElement poll() {
    QueueElement queueElement = messages.pollFirst();
    if (queueElement != null) {
      estimatedQueueSize.addAndGet(-queueElement.estimatedSize());
    }
    return queueElement;
  }

  public boolean isEmpty() {
    return messages.isEmpty();
  }

  public Stream<QueueElement> elements() {
    return messages.stream();
  }

  @Override
  public void failAll(Throwable exception) {
    messages.forEach(
        m -> {
          try {
            m.messageGenerator.messageFailed(exception);
          } catch (Exception e) {
            logger.warn("Exception caught while calling messageFailed", e);
          }
        });
  }
}
