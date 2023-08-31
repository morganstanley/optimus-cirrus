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
package com.ms.silverking.cloud.ring;

public class RingTest {
  /*
      private final int    numNodes;
      private final Ring<Long, Integer>    ring;

      private static final int    batchSize = 1000000;

      public RingTest(Ring<Long, Integer> ring, int numNodes) {
          this.ring = ring;
          this.numNodes = numNodes;
          populateRing(numNodes);
      }

      private void populateRing(int numNodes) {
          Random    random;

          random = new Random();
          for (int i = 0; i < numNodes; i++) {
              ring.put(random.nextLong(), i);
          }
      }

      public void runTest(double durationSeconds, int numMembers) {
          Stopwatch    sw;
          Random        random;
          double        secondsPerAccess;
          double        usPerAccess;
          long        totalAccesses;

          totalAccesses = 0;
          random = new Random();
          sw = new Stopwatch();
          while (sw.getSplitSeconds() < durationSeconds) {
              for (int i = 0; i < batchSize; i++) {
                  long    key;

                  key = random.nextLong();
                  if (numMembers == 1) {
                      int    entry;

                      entry = ring.getOwner(key);
                  } else {
                      List<Integer>    entries;

                      entries = ring.get(key, numMembers);
                  }
              }
              totalAccesses += batchSize;
          }
          sw.stop();
          secondsPerAccess = sw.getElapsedSeconds() / (double)totalAccesses;
          usPerAccess = secondsPerAccess * 1e6;
          System.out.println("usPerAccess:\t"+ usPerAccess);
      }

      public static Ring<Long, Integer> createRing(String ringType) {
          if (ringType.equals("TreeMapRing")) {
              return new TreeMapRing<Long,Integer>();
          } else if (ringType.equals("SampleRing")) {
                  return new SampleRing<Long,Integer>();
          } else if (ringType.equals("LongTreeMapRing.SUBSEQUENT")) {
              return new LongTreeMapRing<Integer>(LongTreeMapRing.Mode.SUBSEQUENT);
          } else if (ringType.equals("LongTreeMapRing.ROTATE")) {
              return new LongTreeMapRing<Integer>(LongTreeMapRing.Mode.ROTATE);
          } else if (ringType.equals("LongConcurrentMapRing.SUBSEQUENT")) {
              return new LongConcurrentMapRing<Integer>(LongConcurrentMapRing.Mode.SUBSEQUENT);
          } else if (ringType.equals("LongConcurrentMapRing.ROTATE")) {
              return new LongConcurrentMapRing<Integer>(LongConcurrentMapRing.Mode.ROTATE);
          } else if (ringType.equals("LongLockingTreeMapRing.SUBSEQUENT")) {
              return new LongLockingTreeMapRing<Integer>(LongLockingTreeMapRing.Mode.SUBSEQUENT);
          } else if (ringType.equals("LongLockingTreeMapRing.ROTATE")) {
              return new LongLockingTreeMapRing<Integer>(LongLockingTreeMapRing.Mode.ROTATE);
          } else {
              throw new RuntimeException();
          }
      }
  */
  /**
   * @param args
   */
  /*
  public static void main(String[] args) {
      try {
          if (args.length != 4) {
              System.out.println("<ringType> <numNodes> <duration> <numMembers>");
          } else {
              String        ringType;
              int            numNodes;
              double        durationSeconds;
              int            numMembers;
              RingTest    treeMapTest;
              Ring<Long,Integer>    ring;

              ringType = args[0];
              numNodes = Integer.parseInt(args[1]);
              durationSeconds = Double.parseDouble(args[2]);
              numMembers = Integer.parseInt(args[3]);
              ring = createRing(ringType);
              treeMapTest = new RingTest(ring, numNodes);
              treeMapTest.runTest(durationSeconds, numMembers);
          }
      } catch (Exception e) {
          e.printStackTrace();
      }
  }
  */
}
