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

public class SampleRing<K, T> {// implements Ring<K, T> {
    /*
    private T    last;
    
    public SampleRing() {
    }
    
    @Override
    public void put(K key, T member) {
        last = member;
    }
    
    @Override
    public T getOwner(K key) {
        return last;
    }
    
    @Override
    public List<T> get(K key, int numMembers) {
        List<T>    members;
        
        if (numMembers < 1) {
            throw new RuntimeException("numMembers < 1");
        }
        members = new ArrayList<T>(numMembers);
        for (int i = 0; i < numMembers; i++) {
            members.add(last);
        }
        return members;
    }
    
    @Override
    public int numMembers() {
        return 1;
    }
    
    @Override
    public Collection<T> getMembers() {
        return null;
    }
    */
}
