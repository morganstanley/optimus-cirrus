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

public class LongLockingTreeMapRing<T> {// implements Ring<Long, T> {
    /*
    private final Mode    mode;
    private final TreeMap<Long, T>    ringMap;
    private final ReentrantReadWriteLock    rwLock;
    private final ReadLock    readLock;
    private final WriteLock    writeLock;
    
    public enum Mode {SUBSEQUENT, ROTATE};

    public LongLockingTreeMapRing(Mode mode) {
        this.mode = mode;
        ringMap = new TreeMap<Long, T>();
        rwLock = new ReentrantReadWriteLock();
        readLock = rwLock.readLock();
        writeLock = rwLock.writeLock();
    }
    
    @Override
    public void put(Long key, T member) {
        writeLock.lock();
        try {
            ringMap.put(key, member);
        } finally {
            writeLock.unlock();
        }
    }
    
    private Map.Entry<Long, T> getEntry(Long key) {
        readLock.lock();
        try {
            Map.Entry<Long, T> entry;
            
            entry = ringMap.ceilingEntry(key);
            if (entry == null) {
                entry = ringMap.firstEntry();
            }
            return entry;
        } finally {
            readLock.unlock();
        }
    }
    
    @Override
    public T getOwner(Long key) {
        readLock.lock();
        try {
            Map.Entry<Long, T> entry;
    
            entry = getEntry(key);
            return entry.getValue();
        } finally {
            readLock.unlock();
        }
    }
    
    @Override
    public List<T> get(Long key, int numMembers) {
        readLock.lock();
        try {
            Map.Entry<Long, T> entry;
            List<T>    members;
            
            if (numMembers < 1) {
                throw new RuntimeException("numMembers < 1");
            }
            members = new ArrayList<T>(numMembers);
            entry = getEntry(key);
            members.add(entry.getValue());
            switch (mode) {
            case SUBSEQUENT: addSubsequent(members, entry, numMembers - 1); break;
            case ROTATE: addRotated(members, entry, numMembers - 1); break;
            default: throw new RuntimeException("panic");
            }
            return members;
        } finally {
            readLock.unlock();
        }
    }
    
    private void addSubsequent(List<T> members, Map.Entry<Long,T> entry, int numSubsequent) {
        for (int i = 0; i < numSubsequent; i++) {
            entry = ringMap.higherEntry(entry.getKey());
            if (entry == null) {
                entry = ringMap.firstEntry();
            }
            members.add(entry.getValue());
        }
    }
    
    private void addRotated(List<T> members, Map.Entry<Long,T> entry, int numSubsequent) {
        for (int i = 0; i < numSubsequent; i++) {
            Long    rotated;
            
            rotated = Long.rotateRight(entry.getKey(), 1);
            entry = getEntry(rotated);
            members.add(entry.getValue());
        }
    }
    
    @Override
    public Collection<T> getMembers() {
        return ringMap.values();
    }
    
    @Override
    public int numMembers() {
        return ringMap.size();
    }
    */
}
