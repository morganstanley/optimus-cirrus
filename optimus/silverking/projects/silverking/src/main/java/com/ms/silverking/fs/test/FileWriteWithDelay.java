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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import com.ms.silverking.collection.Pair;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.SimpleTimer;
import com.ms.silverking.time.Stopwatch;
import com.ms.silverking.time.Timer;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class FileWriteWithDelay {
  private static final int bufferSize = 64 * 1024;
  private static final byte[] buffer;
  private static final byte[] buffer2;
  private static final double displayIntervalSeconds = 1.0;
  static final byte buffer2byteValue = 'A';

  private static Logger log = LoggerFactory.getLogger(FileWriteWithDelay.class);

  static {
    buffer = new byte[bufferSize];
    ThreadLocalRandom.current().nextBytes(buffer);

    buffer2 = new byte[bufferSize];
    for (int i = 0; i < buffer2.length; i++) {
      buffer2[i] = buffer2byteValue;
    }
  }

  public static void write(
      File file, long size, double rateLimitMBs, boolean randomBytes, Pair<Double, Double> delay)
      throws IOException {
    OutputStream out;
    long totalBytesWritten;
    Stopwatch sw;
    Timer displayTimer;
    double pause;
    boolean delayExecuted;

    byte[] buff;
    if (randomBytes) {
      buff = buffer;
    } else {
      buff = buffer2;
    }

    delayExecuted = false;
    totalBytesWritten = 0;
    out = new FileOutputStream(file);
    displayTimer = new SimpleTimer(TimeUnit.SECONDS, 1);
    sw = new SimpleStopwatch();
    do {
      int bytesToWrite;

      bytesToWrite = (int) Math.min(size - totalBytesWritten, buff.length);
      out.write(buff, 0, bytesToWrite);
      totalBytesWritten += bytesToWrite;

      if (!delayExecuted && delay != null && sw.getSplitSeconds() > delay.getV1()) {
        delayExecuted = true;
        pause = delay.getV2();
      } else {
        double targetTime;

        targetTime = totalBytesWritten / (rateLimitMBs * 1000000.0);
        pause = targetTime - sw.getSplitSeconds();
      }
      if (displayTimer.hasExpired()) {
        log.info(
            "{} {}  {}",
            sw.getSplitSeconds(),
            totalBytesWritten,
            ((double) totalBytesWritten / sw.getSplitSeconds() / 1000000.0));
        displayTimer.reset();
      }
      if (pause > 0.0) {
        ThreadUtil.sleepSeconds(pause);
      }
    } while (totalBytesWritten < size);
    out.close();
  }

  public static void main(String[] args) {
    try {
      if (args.length != 4 && args.length != 5) {
        log.info("args: <file> <size> <rateLimit (MB/s)> [delay seconds,seconds]");
      } else {
        File file;
        long size;
        double rateLimitMBs;
        boolean randomBytes;

        file = new File(args[0]);
        size = Long.parseLong(args[1]);
        rateLimitMBs = Double.parseDouble(args[2]);
        randomBytes = Boolean.valueOf(args[3]);

        write(file, size, rateLimitMBs, randomBytes, null);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
