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
package com.ms.silverking.io.test;

import java.nio.ByteBuffer;

public class SamplePutMessage {
  private final ByteBuffer buf;

  public SamplePutMessage(ByteBuffer buf, int offset, int length) {
    this.buf = buf.asReadOnlyBuffer();
    this.buf.position(offset);
    this.buf.limit(offset + length);
  }

  public ByteBuffer[] toBuffers() {
    ByteBuffer[] bufs;

    bufs = new ByteBuffer[1];
    bufs[0] = buf;
    return bufs;
  }
}
