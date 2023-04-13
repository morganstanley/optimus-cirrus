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
import java.util.concurrent.ThreadLocalRandom;

import com.google.caliper.Benchmark;
import com.google.caliper.Param;
import com.google.caliper.runner.CaliperMain;

public class SimpleMapTest {
  //@Param({"10", "25", "50"})
  @Param({ "25" })
  int writePercentage;

  //@Param({"10", "100", "1000"})
  @Param({ "100" })
  int keyRange;

  private static final int randomSize = 32768;
  private static final int[] randomInts;

  static {
    ThreadLocalRandom tlRandom;

    tlRandom = ThreadLocalRandom.current();
    randomInts = new int[randomSize];
    for (int i = 0; i < randomInts.length; i++) {
      randomInts[i] = tlRandom.nextInt();
    }
  }

  @Benchmark
  private void doMap(int reps, Map<Integer, Integer> map) {
    int total;
    int randomIndex;

    randomIndex = 0;
    total = 0;
    for (int i = 0; i < reps; i++) {
      boolean write;
      int key;
      int rand;

      rand = randomInts[randomIndex];
      randomIndex = (randomIndex + 1) % randomSize;
      write = (Math.abs(rand) % 100) < writePercentage;
      key = (Math.abs(rand >> 7) % keyRange);
      if (write) {
        map.put(key, key);
      } else {
        Integer x;

        x = map.get(key);
        if (x != null) {
          total += x.intValue();
        }
      }
    }
    System.out.println(total);
  }

  @Benchmark
  public void timeHashMap(int reps) {
    doMap(reps, new HashMap<Integer, Integer>());
  }
    
    /*
    public void timeConcurrentHashMap(int reps) {
        doMap(reps, new ConcurrentHashMap<Integer, Integer>());
    }
    
    public void timeConcurrentSkipListMap(int reps) {
        doMap(reps, new ConcurrentSkipListMap<Integer, Integer>());
    }
    */

  @Benchmark
  public void timeSimpleHashMap(int reps) {
    doMap(reps, new SimpleHashMap<Integer, Integer>());
  }

  @Benchmark
  public void timeIntHashMap(int reps) {
    doMap(reps, new IntHashMap<Integer>());
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    CaliperMain.main(SimpleMapTest.class, args);
  }
}
