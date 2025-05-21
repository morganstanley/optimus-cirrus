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

package msjava.zkapi.leader.internal;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.zookeeper.data.Stat;
import msjava.zkapi.internal.ZkaData;
import msjava.zkapi.support.ZkaUtils;
final class LeaderData {
    private final Stat stat;
    private final ZkaData zkaData;
    private final long leaderId;
    
    LeaderData(ChildData cd) {
        this.stat = cd.getStat();
        this.zkaData = ZkaData.tryFromBytes(cd.getData())
                .orElseThrow(() -> new IllegalArgumentException("invalid data in " + cd.getPath()));
        this.leaderId = ZkaUtils.toLong(zkaData.getReservedMap().get(ZkaLeaderElector.LEADER_ID_KEY), () -> {
            throw new IllegalArgumentException("no leader id in " + cd.getPath());
        });
    }
    
    
    Stat getStat() { return stat; }
    
    ZkaData getData() { return zkaData; }
    long getLeaderId() { return leaderId; }
    
}
