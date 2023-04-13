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
package com.ms.silverking.collection.test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class ConcurrentMapTest {
  public enum Sharing {Global, PerThread}

  ;

  public enum MapType {ConcurrentHashMap, ConcurrentSkipListMap, HashMap, TreeMap, RWLockMap}

  ;

  public ConcurrentMapTest() {
  }

  private Map<Integer, Integer> createMap(MapType mapType, int numThreads) {
    switch (mapType) {
    case ConcurrentHashMap:
      return new ConcurrentHashMap<Integer, Integer>();
    case ConcurrentSkipListMap:
      return new ConcurrentSkipListMap<Integer, Integer>();
    case HashMap:
      return new HashMap<Integer, Integer>();
    case TreeMap:
      return new TreeMap<Integer, Integer>();
    case RWLockMap:
      return new RWLockMap<Integer, Integer>();
    default:
      throw new RuntimeException("panic");
    }
  }

  private static Logger log = LoggerFactory.getLogger(ConcurrentMapTest.class);

  public void runTest(Sharing sharing, MapType mapType, int threads, int iterations, double putFraction) {
    Worker[] workers;
    AtomicBoolean start;
    Stopwatch sw;

    start = new AtomicBoolean(false);
    workers = new Worker[threads];
    switch (sharing) {
    case Global:
      Map<Integer, Integer> globalMap;

      globalMap = createMap(mapType, threads);
      for (int i = 0; i < workers.length; i++) {
        workers[i] = new Worker(i, globalMap, iterations, start, putFraction);
      }
      break;
    case PerThread:
      for (int i = 0; i < workers.length; i++) {
        Map<Integer, Integer> workerMap;

        workerMap = createMap(mapType, 1);
        workers[i] = new Worker(i, workerMap, iterations, start, putFraction);
      }
      break;
    default:
      throw new RuntimeException("panic");
    }
    sw = new SimpleStopwatch();
    start.set(true);
    for (int i = 0; i < workers.length; i++) {
      workers[i].waitForCompletion();
    }
    sw.stop();

    log.info("Complete");
    log.info("Global elapsed {}accesses/s {}", sw.getElapsedSeconds(),
        (double) (iterations * workers.length) / sw.getElapsedSeconds());
    for (int i = 0; i < workers.length; i++) {
      log.info("Worker {}elapsed {} accesses/s {}", i, workers[i].getStopwatch().getElapsedSeconds(),
          (double) iterations / workers[i].getStopwatch().getElapsedSeconds());
    }
  }

  class Worker implements Runnable {
    private final int id;
    private final Random random;
    private final Map<Integer, Integer> map;
    private final int iterations;
    private final Stopwatch sw;
    private final AtomicBoolean start;
    private final Semaphore semaphore;
    private final int putFraction;

    private static final int randomRange = 1000;

    Worker(int id, Map<Integer, Integer> map, int iterations, AtomicBoolean start, double putFraction) {
      this.id = id;
      this.random = new Random(id);
      this.map = map;
      this.iterations = iterations;
      sw = new SimpleStopwatch();
      this.start = start;
      this.putFraction = (int) (putFraction * (double) randomRange);
      semaphore = new Semaphore(0);
      new Thread(this).start();
    }

    int getID() {
      return id;
    }

    Stopwatch getStopwatch() {
      return sw;
    }

    void waitForCompletion() {
      try {
        semaphore.acquire();
      } catch (InterruptedException ie) {
      }
    }

    public void run() {
      while (!start.get()) {
      }
      sw.reset();
      for (int i = 0; i < iterations; i++) {
        int k;

        k = Math.abs(random.nextInt()) % randomRange;
        if (k < putFraction) {
          map.put(k, k);
        } else {
          // NOTE - CURRENTLY WE DON'T ENSURE THAT THESE ARE ACTUALLY PRESENT,
          // HENCE THIS TEST WILL BE BIASED FOR MAPS THAT PERFORM DIFFERENTLY FOR
          // PRESENT VS. ABSENT KEYS
          map.get(k);
        }
      }
      sw.stop();
      semaphore.release();
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      if (args.length != 5) {
        log.info("<sharing> <mapType> <threads> <iterations> <putFraction>");
      } else {
        ConcurrentMapTest test;
        Sharing sharing;
        MapType mapType;
        int threads;
        int iterations;
        double putFraction;

        test = new ConcurrentMapTest();
        sharing = Sharing.valueOf(args[0]);
        mapType = MapType.valueOf(args[1]);
        threads = Integer.parseInt(args[2]);
        iterations = Integer.parseInt(args[3]);
        putFraction = Double.parseDouble(args[4]);
        test.runTest(sharing, mapType, threads, iterations, putFraction);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
