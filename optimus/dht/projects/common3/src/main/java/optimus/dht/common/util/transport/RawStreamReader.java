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
package optimus.dht.common.util.transport;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

import io.netty.buffer.ByteBuf;
import optimus.dht.common.api.transport.DataStreamConsumer;

public interface RawStreamReader {

  long size();

  long remaining();

  void read(ByteBuf buf, int length);

  static RawStreamReader create(byte[] array) {
    return new ByteArrayStreamReader(array);
  }

  static RawStreamReader create(byte[][] nestedArrays) {
    return new NestedByteArraysStreamReader(nestedArrays);
  }

  static RawStreamReader create(ByteBuffer buffer) {
    return new ByteBufferStreamReader(buffer);
  }

  static RawStreamReader create(
      DataStreamConsumer streamConsumer, long size, Consumer<Exception> userExceptionCallback) {
    return new DataStreamConsumerStreamReader(streamConsumer, size, userExceptionCallback);
  }
}
