// Terminator.java

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
package com.ms.silverking.process;

import java.io.File;

import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.thread.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Terminator implements Runnable {
  protected int seconds;
  protected long startTime;
  protected long termTime;
  protected int exitCode;
  protected File termFile;
  protected TerminatorListener listener;

  protected static final int TERM_FILE_CHECK_SECS = 5;

  private static Logger log = LoggerFactory.getLogger(Terminator.class);

  public Terminator(int seconds, int exitCode, TerminatorListener listener) {
    this.seconds = seconds;
    this.exitCode = exitCode;
    this.listener = listener;
    termTime = SystemTimeUtil.skSystemTimeSource.absTimeMillis() + seconds * 1000;
    ThreadUtil.newDaemonThread(this, "Terminator").start();
  }

  public Terminator(int seconds, int exitCode) {
    this(seconds, exitCode, null);
  }

  public Terminator(int seconds, TerminatorListener listener) {
    this(seconds, 0, listener);
  }

  public Terminator(int seconds) {
    this(seconds, 0);
  }

  protected Terminator(int seconds, File termFile) {
    this(seconds, 0);
    this.termFile = termFile;
  }

  public Terminator(File termFile) {
    this(TERM_FILE_CHECK_SECS, termFile);
  }

  protected Terminator(int seconds, File termFile, TerminatorListener listener) {
    this(seconds, 0, listener);
    this.termFile = termFile;
  }

  public Terminator(File termFile, TerminatorListener listener) {
    this(TERM_FILE_CHECK_SECS, termFile, listener);
  }

  public void postpone(int pSeconds) {
    int pMillis;

    log.info("Terminator postponing: {}", pSeconds);
    pMillis = pSeconds * 1000;
    synchronized (this) {
      termTime += pMillis;
    }
  }

  protected long calcSleepMillis() {
    synchronized (this) {
      return Math.max(termTime - SystemTimeUtil.skSystemTimeSource.absTimeMillis(), 0);
    }
  }

  public void run() {
    int sleepMillis;

    sleepMillis = seconds * 1000;
    if (termFile == null) {
      log.info("Terminator will execute in  {}  ms", calcSleepMillis());
    }
    if (termFile != null) {
      log.info("Terminator will execute if exists: {}", termFile);
    }
    while (System.currentTimeMillis() < termTime
        && (termFile == null || (termFile != null && !termFile.exists()))) {
      // termTime = SystemTime.currentTimeMillis() + sleepMillis;
      do {
        try {
          Thread.sleep(calcSleepMillis());
        } catch (InterruptedException ie) {
        }
      } while (SystemTimeUtil.skSystemTimeSource.absTimeMillis() < termTime);
    }
    if (listener != null) {
      listener.terminationTriggered(this);
    }
    log.info("Terminator stopping system.");
    System.exit(exitCode);
  }
}
