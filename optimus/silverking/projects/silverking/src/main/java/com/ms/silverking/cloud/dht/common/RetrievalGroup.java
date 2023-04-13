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
package com.ms.silverking.cloud.dht.common;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.net.MessageGroupRetrievalResponseEntry;

/**
 * Retrieval group for a particular DHTKey. Namespace is known within
 * the owning context, hence we only need to specify version here.
 */
public class RetrievalGroup {
  private final ConcurrentMap<VersionConstraint, Object> retrievals;
  // FIXME - THINK ABOUT CONCURRENCY IN THIS CLASS
  //private final ConcurrentNavigableMap<VersionConstraint,Retrieval> retrievals;
    /*
    private final List<Retrieval>   retrievals;
    private final Lock  lock;
    */

  //private static final VCComparator  vcComparator;

  // FIXME - PROBABLY ONLY LOOK TO COMBINE OPERATIONS IF THE VERSION MATCH IS
  // EXACT

  //static {
  //    vcComparator = new VCComparator();
  //}

  public RetrievalGroup() {
    retrievals = new ConcurrentHashMap<>();
    //retrievals = new ConcurrentSkipListMap<>(vcComparator);
        /*
        retrievals = new LinkedList<>();
        lock = new ReentrantLock();
        */
  }

  public boolean addRetrieval(Retrieval retrieval) {
    Object previous;

    assert retrieval.getVersionConstraint() != null;
    previous = retrievals.putIfAbsent(retrieval.getVersionConstraint(), retrieval);
    if (previous != null) {
      if (previous instanceof Retrieval) {
        ArrayList<Retrieval> rList;
        boolean replaced;

        rList = new ArrayList<Retrieval>(2);
        rList.add((Retrieval) previous);
        rList.add(retrieval);
        replaced = retrievals.replace(retrieval.getVersionConstraint(), previous, rList);
        if (!replaced) {
          ArrayList<Retrieval> rList2;

          rList2 = (ArrayList<Retrieval>) retrievals.get(retrieval.getVersionConstraint());
          synchronized (rList2) {
            rList2.add(retrieval);
          }
        }
      } else {
        ArrayList<Retrieval> rList;

        rList = (ArrayList<Retrieval>) previous;
        synchronized (rList) {
          rList.add(retrieval);
        }
      }
    }
    return false;
        /*
        lock.lock();
        try {
            boolean overlaps;
            
            // FIXME - this is a placeholder implementation
            // (whole class implementation is)
            // speed up once correct
            overlaps = false;
            if (retrieval.getVersionConstraint() != null) {
                for (Retrieval existing : retrievals) {
                    if (existing.getVersionConstraint() != null) {
                        if (retrieval.getVersionConstraint().overlaps(existing.getVersionConstraint())) {
                            overlaps = true;
                            break;
                        }
                    }
                }
            }
            retrievals.add(retrieval);
            return overlaps;
        } finally {
            lock.unlock();
        }
        */
        /*
        Retrieval   existing;
        
        existing = retrievals.putIfAbsent(retrieval.getVersionConstraint(), retrieval);
        if (existing != null) {
            throw new RuntimeException("duplicates not yet supported");
        } else {
            // FIXME
        }
        */
  }

  public void removeRetrieval(Retrieval retrieval) {

  }

  public void receivedResponse(MessageGroupRetrievalResponseEntry entry) {
    for (Object obj : retrievals.values()) {
      if (obj instanceof Retrieval) {
        Retrieval retrieval;

        retrieval = (Retrieval) obj;
        retrieval.addResponse(entry);
      } else {
        ArrayList<Retrieval> rList;

        rList = (ArrayList<Retrieval>) obj;
        synchronized (rList) {
          for (Retrieval retrieval : rList) {
            retrieval.addResponse(entry);
          }
        }
      }
    }
        /*
        lock.lock();
        try {
            for (Retrieval retrieval : retrievals) {
                // FIXME - need to look at metadata to see if this matches
                if (true) {
                    retrieval.addResponse(entry);
                }
            }
        } finally {
            lock.unlock();
        }
        */
  }
    
    /*
    static class VCComparator implements Comparator<VersionConstraint> {
        @Override
        public int compare(VersionConstraint vc1, VersionConstraint vc2) {
            if (vc1.getMax() < vc2.getMax()) {
                return -1;
            } else if (vc1.getMax() > vc2.getMax()) {
                return 1;
            } else {
                return 0;
            }
        }
    }
    */
}
