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

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

/** */
public class ByteBufferOutputStream extends ByteArrayOutputStream {
  private List<ByteBuffer> byteBuffers;

  public ByteBufferOutputStream() {
    byteBuffers = new LinkedList<ByteBuffer>();
  }

  @Override
  public synchronized void write(byte[] b, int off, int len) {
    super.write(b, off, len);
  }

  public ByteBuffer[] toBuffers() {
    /*
    byte[]        objBytes;
    ByteBuffer    objByteBuffer;

    objBytes = toByteArray();
    */
    return null;
  }
}
