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
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ImmutableList;
import com.ms.silverking.io.FileUtil;
import com.ms.silverking.net.NetUtil;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.time.SimpleTimer;
import com.ms.silverking.time.Timer;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class FSStress implements Runnable {
  private final List<File> files;
  private final int durationSeconds;
  private final AtomicInteger nextThreadID;
  private final AtomicLong totalFiles;
  private final AtomicLong totalBytes;

  private static Logger log = LoggerFactory.getLogger(FSStress.class);

  private static final boolean displayPerThreadTotals = false;
  private static final int displayIntervalSeconds = 1;
  private static final PrintStream out = System.out;
  private static final PrintStream err = System.err;

  public FSStress(List<File> files, int durationSeconds) {
    this.files = files;
    this.durationSeconds = durationSeconds;
    nextThreadID = new AtomicInteger();
    totalFiles = new AtomicLong();
    totalBytes = new AtomicLong();
  }

  public FSStress(File dir, int durationSeconds, boolean recursive) {
    this(recursive ? ImmutableList.copyOf(FileUtil.listFilesRecursively(dir)) : ImmutableList.copyOf(dir.listFiles()),
        durationSeconds);
  }

  public int getNumFiles() {
    return files.size();
  }

  public void stressMultiple(int numThreads) {
    Thread[] threads;
    Thread displayThread;

    threads = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      threads[i] = new Thread(this);
      threads[i].start();
    }
    displayThread = new Thread(new TotalDisplayer());
    displayThread.start();
    for (int i = 0; i < numThreads; i++) {
      try {
        threads[i].join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    try {
      displayThread.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    displayTotals(durationSeconds);
  }

  private class TotalDisplayer implements Runnable {
    public void run() {
      Timer runTimer;

      runTimer = new SimpleTimer(TimeUnit.SECONDS, durationSeconds);
      while (!runTimer.hasExpired()) {
        ThreadUtil.sleepSeconds(displayIntervalSeconds);
        displayTotals(runTimer.getSplitSeconds());
      }
    }
  }

  private long lastBytes;
  private double lastElapsedSeconds;

  private void displayTotals(double elapsedSeconds) {
    long bytes;

    bytes = totalBytes.get();
    out.printf("Elapsed %f\tf %d\tb %d\tGB %d\tMB/s %f %f\n", elapsedSeconds, totalFiles.get(), bytes,
        bytes / 1000000000, NetUtil.calcMBps(bytes, elapsedSeconds),
        NetUtil.calcMBps(bytes - lastBytes, elapsedSeconds - lastElapsedSeconds));
    lastBytes = bytes;
    lastElapsedSeconds = elapsedSeconds;
  }

  private void stressOne(int id) throws IOException {
    Timer runTimer;
    Timer displayTimer;
    long filesRead;
    long bytesRead;

    displayTimer = new SimpleTimer(TimeUnit.SECONDS, displayIntervalSeconds);
    runTimer = new SimpleTimer(TimeUnit.SECONDS, durationSeconds);
    filesRead = 0;
    bytesRead = 0;
    while (!runTimer.hasExpired()) {
      long fileBytes;

      filesRead++;
      fileBytes = readRandomFile();
      bytesRead += fileBytes;
      totalFiles.incrementAndGet();
      totalBytes.addAndGet(fileBytes);
      if (displayPerThreadTotals && displayTimer.hasExpired()) {
        long bytes;

        bytes = bytesRead;
        out.printf("id %d\tduration %f\tfiles %d\tbytes %d\tGB %d\tMB/s %f %f\n", id, runTimer.getSplitSeconds(),
            filesRead, bytes, bytes / 1000000000, NetUtil.calcMBps(bytes, runTimer.getSplitSeconds()),
            NetUtil.calcMBps(bytes, displayIntervalSeconds));
        displayTimer.reset();
      }
    }
  }

  private long readRandomFile() throws IOException {
    do {
      File target;

      target = files.get(ThreadLocalRandom.current().nextInt(files.size()));
      if (target.isFile() && target.length() <= Integer.MAX_VALUE) {
        try {
          return FileUtil.readFileAsBytes(target).length;
        } catch (NegativeArraySizeException nase) {
          err.printf("Error reading: %s\n", target);
          nase.printStackTrace();
        } catch (IOException ioe) {
          if (!ioe.getMessage().equals("Operation not permitted")) {
            err.printf("Error reading: %s\n", target);
            ioe.printStackTrace();
          }
        }
      }
    } while (true);
  }

  public void run() {
    try {
      stressOne(nextThreadID.getAndIncrement());
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }

  public static void main(String[] args) {
    if (args.length < 3 || args.length > 4) {
      err.printf("<targetDir> <numThreads> <durationSeconds> [recursive]\n");
    } else {
      File targetDir;
      int numThreads;
      int durationSeconds;
      boolean recursive;
      FSStress fsStress;

      targetDir = new File(args[0]);
      if (!targetDir.exists()) {
        err.printf("No such directory: %s\n", targetDir);
        return;
      }
      if (!targetDir.isDirectory()) {
        err.printf("Not a directory: %s\n", targetDir);
        return;
      }
      numThreads = Integer.parseInt(args[1]);
      durationSeconds = Integer.parseInt(args[2]);
      if (args.length == 4) {
        recursive = Boolean.parseBoolean(args[3]);
      } else {
        recursive = false;
      }
      out.printf("Creating file list...");
      fsStress = new FSStress(targetDir, durationSeconds, recursive);
      out.printf("Found %d files.\n", fsStress.getNumFiles());
      out.printf("\nRunning test\n");
      fsStress.stressMultiple(numThreads);
      out.printf("\nTest complete\n");
    }
  }
}
