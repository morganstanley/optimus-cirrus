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

import io.netty.buffer.ByteBuf;

public class ByteArrayStreamReader implements RawStreamReader {

  private final byte[] array;

  private int position;

  public ByteArrayStreamReader(byte[] array) {
    this.array = array;
  }

  @Override
  public long size() {
    return array.length;
  }

  @Override
  public long remaining() {
    return size() - position;
  }

  @Override
  public void read(ByteBuf buf, int length) {
    buf.readBytes(array, position, length);
    position += length;
  }
}