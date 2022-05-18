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
package com.ms.silverking.os.linux.proc;

import java.util.Comparator;

/**
 * Provides information from /proc/<pid>/stat
 */
public class ProcessStat {
  private static final int pageSize = 4096;

  /**
   * The process ID.
   */
  public int pid;

  /**
   * The filename of the executable, in parentheses.  This is
   * visible whether or not the executable is swapped out.
   */
  public String comm;

  /**
   * One character from the string "RSDZTW" where
   *   R is running,
   *   S is sleeping in an interruptible wait,
   *   D is waiting in uninterruptible disk sleep,
   *   Z is zombie,
   *   T is traced or stopped (on a signal), and
   *   W is paging.
   */
  public char state;

  /**
   * The PID of the parent.
   */
  public int ppid;

  /**
   * The process group ID of the process.
   */
  public int pgrp;

  /**
   * The session ID of the process.
   */
  public int session;

  /**
   * The controlling terminal of the process.  (The minor device
   * number is contained in the combination of bits 31 to 20 and
   * 7 to 0; the major device number is in bits 15 to 8.)
   */
  public int tty_nr;

  /**
   * The ID of the foreground process group of the controlling
   * terminal of the process.
   */
  public int tpgid;

  /**
   * The kernel flags word of the process.  For bit meanings,
   * see the PF_* defines in <linux/sched.h>.  Details depend on
   * the kernel version.
   */
  public int flags;

  /**
   * The number of minor faults the process has made which have
   * not required loading a memory page from disk.
   */
  public long minflt;

  /**
   * The number of minor faults that the process's waited-for
   * children have made.
   */
  public long cminflt;

  /**
   * The number of major faults the process has made which have
   * required loading a memory page from disk.
   */
  public long majflt;

  /**
   * The number of major faults that the process's waited-for
   * children have made.
   */
  public long cmajflt;

  /**
   * Amount of time that this process has been scheduled in user
   * mode, measured in clock ticks (divide by
   * sysconf(_SC_CLK_TCK).  This includes guest time, guest_time
   * (time spent running a virtual CPU, see below), so that
   * applications that are not aware of the guest time field do
   * not lose that time from their calculations.
   */
  public long utime;

  /**
   * Amount of time that this process has been scheduled in
   * kernel mode, measured in clock ticks (divide by
   * sysconf(_SC_CLK_TCK).
   */
  public long stime;

  /**
   * Amount of time that this process's waited-for children have
   * been scheduled in user mode, measured in clock ticks
   * (divide by sysconf(_SC_CLK_TCK).  (See also times(2).)
   * This includes guest time, cguest_time (time spent running a
   * virtual CPU, see below).
   */
  public long cutime;

  /**
   * Amount of time that this process's waited-for children have
   * been scheduled in kernel mode, measured in clock ticks
   * divide by sysconf(_SC_CLK_TCK).
   */
  public long cstime;

  /**
   * (Explanation for Linux 2.6) For processes running a real-
   * time scheduling policy (policy below; see
   * sched_setscheduler(2)), this is the negated scheduling
   * priority, minus one; that is, a number in the range -2 to
   * -100, corresponding to real-time priorities 1 to 99.  For
   * processes running under a non-real-time scheduling policy,
   * this is the raw nice value (setpriority(2)) as represented
   * in the kernel.  The kernel stores nice values as numbers in
   * the range 0 (high) to 39 (low), corresponding to the user-
   * visible nice range of -20 to 19.
   * Before Linux 2.6, this was a scaled value based on the
   * scheduler weighting given to this process.
   */
  public long priority;

  /**
   * The nice value (see setpriority(2)), a value in the range
   * 19 (low priority) to -20 (high priority).
   */
  public long nice;

  /**
   * Number of threads in this process (since Linux 2.6).
   * Before kernel 2.6, this field was hard coded to 0 as a
   * placeholder for an earlier removed field.
   */
  public long num_threads;

  /**
   * The time in jiffies before the next SIGALRM is sent to the
   * process due to an interval timer.  Since kernel 2.6.17,
   * this field is no longer maintained, and is hard coded as 0.
   */
  public long itrealvalue;

  /**
   * The time in jiffies the process started after system boot.
   */
  public long starttime;

  /**
   * Virtual memory size in bytes.
   */
  public long vsize;

  /**
   * Resident Set Size: number of pages the process has in real
   * memory.  This is just the pages which count toward text,
   * data, or stack space.  This does not include pages which
   * have not been demand-loaded in, or which are swapped out.
   */
  public long rss;

  /**
   * Current soft limit in bytes on the rss of the process; see
   * the description of RLIMIT_RSS in getpriority(2).
   */
  public long rsslim;

  /**
   * The address above which program text can run.
   */
  public long startcode;

  /**
   * The address below which program text can run.
   */
  public long endcode;

  /**
   * The address of the start (i.e., bottom) of the stack.
   */
  public long startstack;

  /**
   * The current value of ESP (stack pointer), as found in the
   * kernel stack page for the process.
   */
  public long kstkesp;

  /**
   * The current EIP (instruction pointer).
   */
  public long kstkeip;

  /**
   * The bitmap of pending signals, displayed as a decimal
   * number.  Obsolete, because it does not provide information
   * on real-time signals; use /proc/[pid]/status instead.
   */
  public long signal;

  /**
   * The bitmap of blocked signals, displayed as a decimal
   * number.  Obsolete, because it does not provide information
   * on real-time signals; use /proc/[pid]/status instead.
   */
  public long blocked;

  /**
   * The bitmap of ignored signals, displayed as a decimal
   * number.  Obsolete, because it does not provide information
   * on real-time signals; use /proc/[pid]/status instead.
   */
  public long sigignore;

  /**
   * The bitmap of caught signals, displayed as a decimal
   * number.  Obsolete, because it does not provide information
   * on real-time signals; use /proc/[pid]/status instead.
   */
  public long sigcatch;

  /**
   * This is the "channel" in which the process is waiting.  It
   * is the address of a system call, and can be looked up in a
   * namelist if you need a textual name.  (If you have an up-
   * to-date /etc/psdatabase, then try ps -l to see the WCHAN
   * field in action.)
   */
  public long wchan;

  /**
   * Number of pages swapped (not maintained).
   */
  public long nswap;

  /**
   * Cumulative nswap for child processes (not maintained).
   */
  public long cnswap;

  /**
   * Signal to be sent to parent when we die.
   */
  public int exit_signal;

  /**
   * CPU number last executed on.
   */
  public int processor;

  /**
   * Real-time scheduling priority, a number in the range 1 to
   * 99 for processes scheduled under a real-time policy, or 0,
   * for non-real-time processes (see sched_setscheduler(2)).
   */
  public int rt_priority;

  /**
   * Scheduling policy (see sched_setscheduler(2)).  Decode
   * using the SCHED_* constants in linux/sched.h.
   */
  public int policy;

  /**
   * Aggregated block I/O delays, measured in clock ticks
   * (centiseconds).
   */
  public long delayacct_blkio_ticks;

  /**
   * Guest time of the process (time spent running a virtual CPU
   * for a guest operating system), measured in clock ticks
   * (divide by sysconf(_SC_CLK_TCK).
   */
  public long guest_time;

  /**
   * Guest time of the process's children, measured in clock
   */
  public long cguest_time;

  public ProcessStat() {
  }

  public long getRSSBytes() {
    return rss * pageSize;
  }

  public String toString() {
    return pid + ":" + getRSSBytes();
  }

  private long getFieldForComparison(String fieldName) {
    long value;

    try {
      Long lval = (Long) getClass().getField(fieldName).get(this);
      value = (long) lval;
      return value;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  //////////////////

  public static Comparator<ProcessStat> fieldComparator(String fieldName) {
    return new FieldComparator(fieldName);
  }

  static class FieldComparator implements Comparator<ProcessStat> {
    private final String fieldName;

    public FieldComparator(String fieldName) {
      this.fieldName = fieldName;
    }

    @Override
    public int compare(ProcessStat p1, ProcessStat p2) {
      if (p1.getFieldForComparison(fieldName) < p2.getFieldForComparison(fieldName)) {
        return -1;
      } else if (p1.getFieldForComparison(fieldName) > p2.getFieldForComparison(fieldName)) {
        return 1;
      } else {
        return 0;
      }
    }
  }
}
