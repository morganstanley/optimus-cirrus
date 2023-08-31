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

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class PureBufferedSerializationTest {
  public enum Test {
    serialization
  };

  public enum AllocationMethod {
    byteBuffer,
    directBuffer
  };

  public PureBufferedSerializationTest() {}

  private static Logger log = LoggerFactory.getLogger(PureBufferedSerializationTest.class);

  public void runTest(Test test, AllocationMethod allocationMethod, int size, int iterations) {
    Stopwatch sw;
    SamplePutMessage samplePutMessage;
    long length;
    double secondsPerIteration;
    ByteBuffer buf;

    length = 0;
    switch (allocationMethod) {
      case byteBuffer:
        buf = ByteBuffer.allocate(size);
        break;
      case directBuffer:
        buf = ByteBuffer.allocateDirect(size);
        break;
      default:
        throw new RuntimeException("panic");
    }
    while (buf.hasRemaining()) {
      buf.put((byte) 7);
    }
    samplePutMessage = new SamplePutMessage(buf, 0, size);
    sw = new SimpleStopwatch();
    switch (test) {
      case serialization:
        sw.reset();
        for (int i = 0; i < iterations; i++) {
          ByteBuffer[] buffers;

          buffers = samplePutMessage.toBuffers();
          length += buffers[0].limit();
        }
        break;
      default:
        throw new RuntimeException("");
    }
    sw.stop();
    log.info(length + " " + sw);
    secondsPerIteration = sw.getElapsedSecondsBD().divide(new BigDecimal(iterations)).doubleValue();
    log.info("Time per iteration: %.2e\n", secondsPerIteration);
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      if (args.length < 4) {
        log.info("args: <test> <allocationMethod> <size> <iterations>");
      } else {
        Test test;
        AllocationMethod allocationMethod;
        int size;
        int iterations;

        test = Test.valueOf(args[0]);
        allocationMethod = AllocationMethod.valueOf(args[1]);
        size = Integer.parseInt(args[2]);
        iterations = Integer.parseInt(args[3]);
        new PureBufferedSerializationTest().runTest(test, allocationMethod, size, iterations);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
