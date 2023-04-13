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

public class TreeMapRing<K, T> {//implements Ring<K, T> {
    /*
    private final TreeMap<K, T>    ringMap;

    public TreeMapRing() {
        ringMap = new TreeMap<K, T>();
    }
    
    @Override
    public void put(K key, T member) {
        ringMap.put(key, member);
    }
    
    @Override
    public T getOwner(K key) {
        Map.Entry<K, T> entry;
        
        entry = ringMap.ceilingEntry(key);
        if (entry == null) {
            entry = ringMap.firstEntry();
        }
        return entry.getValue();
    }
    
    @Override
    public List<T> get(K key, int numMembers) {
        Map.Entry<K, T> entry;
        List<T>    members;
        
        if (numMembers < 1) {
            throw new RuntimeException("numMembers < 1");
        }
        members = new ArrayList<T>(numMembers);
        entry = ringMap.ceilingEntry(key);
        if (entry == null) {
            entry = ringMap.firstEntry();
        }
        members.add(entry.getValue());
        for (int i = 0; i < numMembers - 1; i++) {
            entry = ringMap.higherEntry(entry.getKey());
            if (entry == null) {
                entry = ringMap.firstEntry();
            }
            members.add(entry.getValue());
        }
        return members;
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
