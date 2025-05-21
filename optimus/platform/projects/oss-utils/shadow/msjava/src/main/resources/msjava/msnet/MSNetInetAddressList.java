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

package msjava.msnet;
import java.util.ArrayList;
import java.util.List;
class MSNetInetAddressList {
    
    public static final int ADDR_OVERRIDE = -1;
    
    public static final int ADDR_PRIMARY = 0;
    
    public static final int ADDR_BACKUP = 1;
    final List<MSNetInetAddress> addrs =  new ArrayList<>(2);
    MSNetInetAddress override;
    
    protected int addrIdx;
    
    public MSNetInetAddressList() {
    }
    public synchronized void setOverrideAddress(MSNetInetAddress addr) {
        override = addr;
        if (addr == null){
            addrIdx = ADDR_PRIMARY;
        } else {
            addrIdx = ADDR_OVERRIDE;
        }
    }
    public synchronized MSNetInetAddress getOverrideAddress() {
        return override;
    }
    public synchronized MSNetInetAddress getAddress(int index) {
        if ((index < 0) || (index >= addrs.size())) {
            return null;
        }
        return addrs.get(index);
    }
    
    public synchronized MSNetInetAddress getAddress(){
        if (addrIdx == ADDR_OVERRIDE){
            return override;
        }
        return getAddress(addrIdx);
    }
    public synchronized void setAddress(int index, MSNetInetAddress addr) {
        if (index == addrs.size()) {
            addrs.add(addr);
        } else {
            addrs.set(index, addr);
        }
    }
    
    public synchronized void setAddressList(List<MSNetInetAddress> l) {
        clear();
        addrs.addAll(l);
        setOverrideAddress(null);
        addrIdx = ADDR_PRIMARY;
    }
    public synchronized List<MSNetInetAddress> getAddressList() {
        return new ArrayList<>(addrs);
    }
    public synchronized void clear() {
        addrs.clear();
    }
    
    public synchronized void failOver(){
        int sz = addrs.size();
        if (sz == 0 && getOverrideAddress() == null){
            return;
        }
        do {
            ++addrIdx;
            if (addrIdx >= sz) {
                if (null == getOverrideAddress()) {
                    addrIdx = ADDR_PRIMARY;
                } else {
                    addrIdx = ADDR_OVERRIDE;
                }
            }
        } while (getAddress() == null);
    }
    
    public synchronized void failback(boolean useOverride) {
        if (useOverride) {
            addrIdx = ADDR_OVERRIDE;
        } else {
            addrIdx = ADDR_PRIMARY;
        }
    }
    
    public MSNetAddress getBackupAddress() {
        return getAddress(ADDR_BACKUP);
    }
    public void setBackupAddress(MSNetInetAddress addr) {
        setAddress(ADDR_BACKUP, addr);
    }
    public MSNetInetAddress getPrimaryAddress(){
        return getAddress(ADDR_PRIMARY);
    }
    
    public void setPrimaryAddress(MSNetInetAddress addr) {
        setAddress(ADDR_PRIMARY, addr);
    }
}
