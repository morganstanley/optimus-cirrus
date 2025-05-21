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

package msjava.zkapi.leader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class LoggingListener implements LeaderElectorListener {
    private static final Logger log = LoggerFactory.getLogger(LoggingListener.class);
    
    @Override
    public void startServices(LeaderElector leaderElector) throws InterruptedException {
        log.info("{} acquired leadership", leaderElector.getLeaderId());
    }
    
    @Override
    public void stopServices(LeaderElector leaderElector) {
        log.info("{} lost leadership", leaderElector.getLeaderId());
    }
}
