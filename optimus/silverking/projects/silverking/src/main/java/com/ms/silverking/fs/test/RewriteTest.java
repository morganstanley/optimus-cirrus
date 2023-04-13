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
package com.ms.silverking.fs.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;

import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class RewriteTest {
  private RandomAccessFile raf;

  static final int blockSize = 262144;
  private static final byte[][] buf = new byte[26][blockSize];
  private static final byte[] buf1 = new byte[blockSize];
  private static final byte[] buf2 = new byte[blockSize];
  private static final byte[] buf3 = { '1', '2', '3', '4', '5', '6' };

  private static Logger log = LoggerFactory.getLogger(RewriteTest.class);

  private enum Test {Current, Past, CurrentAndPast, CurrentAndPastAndExtension, Random, Last, LastExtension}

  ;

  enum Mode {Write, Verify}

  ;

  static void fillBuf(byte[] buf, int offset, int length, byte b) {
    for (int i = 0; i < length; i++) {
      buf[offset + i] = b;
    }
  }

  static {
    fillBuf(buf1, 0, buf1.length, (byte) 'A');
    fillBuf(buf2, 0, buf2.length, (byte) 'B');
    for (int i = 0; i < 26; i++) {
      fillBuf(buf[i], 0, buf[i].length, (byte) ('A' + i));
    }
  }

  public RewriteTest(File file, Mode mode) throws FileNotFoundException {
    raf = new RandomAccessFile(file, mode == Mode.Write ? "rw" : "r");
  }

  public void testCurrent() throws IOException {
    raf.write(buf1, 0, 128);
    raf.seek(64);
    raf.write(buf2, 0, 32);
    raf.close();
  }

  public void testPast() throws IOException {
    raf.write(buf1, 0, blockSize);
    raf.write(buf1, 0, blockSize);
    raf.write(buf1, 0, blockSize);
    raf.seek(64);
    raf.write(buf2, 0, blockSize);
    raf.close();
  }

  public void testCurrentAndPast() throws IOException {
    raf.write(buf1, 0, blockSize);
    raf.write(buf1, 0, 128);
    raf.seek(64);
    raf.write(buf2, 0, blockSize);
    raf.close();
  }

  public void testCurrentAndPastAndExtension() throws IOException {
    raf.write(buf1, 0, blockSize);
    raf.write(buf1, 0, 64);
    raf.seek(128);
    raf.write(buf2, 0, blockSize);
    raf.close();
  }

  public void testLast() throws IOException {
    raf.write(buf1, 0, blockSize);
    raf.write(buf1, 0, 128);
    raf.seek(blockSize + 64);
    raf.write(buf2, 0, 64);
    raf.close();
  }

  public void testLastAndExtension() throws IOException {
    raf.write(buf1, 0, blockSize);
    raf.write(buf1, 0, 128);
    raf.seek(blockSize + 64);
    raf.write(buf2, 0, 256);
    raf.close();
  }

  public void testBlockBorderOneByteEachSide() throws IOException {
    raf.write(buf1, 0, blockSize);
    raf.write(buf2, 0, blockSize);
    raf.seek(blockSize - 1);
    raf.write(buf3, 0, 2);
    raf.close();
  }

  public void testBlockBorder() throws IOException {
    raf.write(buf1, 0, blockSize);
    raf.write(buf2, 0, blockSize);
    raf.seek(blockSize - (buf3.length / 2));
    raf.write(buf3, 0, buf3.length);
    raf.close();
  }

  public void testRandom(int numWrites, Mode mode) throws IOException {
    Random r;
    Stopwatch sw;
    long fileLength = blockSize * 100;
    int maxSize = blockSize;
    //long    fileLength = blockSize * 2;
    //int        maxSize = 24;
    byte[] readBuf;

    readBuf = new byte[blockSize];
    r = new Random(0);
    sw = new SimpleStopwatch();
    for (int i = 0; i < numWrites; i++) {
      long offset;
      int size;

      offset = r.nextLong() % fileLength;
      if (offset < 0) {
        offset = -offset;
      }
      size = r.nextInt(maxSize - 1) + 1;
      log.info("%d %d\n", offset, size);
      raf.seek(offset);
      if (mode == Mode.Write) {
        raf.write(buf[i % 26], 0, size);
      } else {
        int numRead;

        // Note - this verification only works for cases where writes
        // don't overlap
        numRead = raf.read(readBuf, 0, size);
        if (numRead != size) {
          throw new RuntimeException("numRead != size");
        }
        for (int j = 0; j < size; j++) {
          if (readBuf[j] != buf[i % 26][j]) {
            //                        System.out.printf("%d\t%x != %x\n", offset + j, readBuf[j], buf[i % 26][j]);
          }
        }
      }
    }
    raf.close();
    sw.stop();
    log.info("Elapsed: %s\n", sw.getElapsedSeconds());
  }

  public RandomAccessFile getFile() {
    return raf;
  }

  public static void main(String[] args) {
    if (args.length < 2 || args.length > 4) {
      log.error("args: <file> <test> [numWrites] [mode]");
    } else {
      try {
        RewriteTest rt;
        String file;
        Test test;
        Mode mode;

        if (args.length == 4) {
          mode = Mode.valueOf(args[3]);
        } else {
          mode = Mode.Write;
        }
        file = args[0];
        test = Test.valueOf(args[1]);
        rt = new RewriteTest(new File(file), mode);
        switch (test) {
        case Current:
          rt.testCurrent();
          break;
        case Past:
          rt.testPast();
          break;
        case CurrentAndPast:
          rt.testCurrentAndPast();
          break;
        case CurrentAndPastAndExtension:
          rt.testCurrentAndPastAndExtension();
          break;
        case Last:
          rt.testLast();
          break;
        case LastExtension:
          rt.testLastAndExtension();
          break;
        case Random:
          if (args.length != 4) {
            log.error("Random requires numWrites and mode");
          } else {
            int numWrites;

            numWrites = Integer.parseInt(args[2]);
            rt.testRandom(numWrites, mode);
          }
          break;
        default:
          throw new RuntimeException("panic");
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
