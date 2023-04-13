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
package com.ms.silverking.cloud.dht.daemon;

import com.ms.silverking.net.async.ConnectionController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DisconnectAllExclusionResponder implements SelfExclusionResponder {

  ConnectionController connectionController;
  private static Logger log = LoggerFactory.getLogger(DisconnectAllExclusionResponder.class);

  public DisconnectAllExclusionResponder(ConnectionController controller) {
    this.connectionController = controller;
  }

  @Override
  public void onExclusion() {
    log.warn("SelfExclusionResponder detected exclusion");
    connectionController.disconnectAll("onExclusion");
  }
}
