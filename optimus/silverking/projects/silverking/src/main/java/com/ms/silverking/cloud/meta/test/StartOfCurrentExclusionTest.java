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
package com.ms.silverking.cloud.meta.test;

import java.util.Map;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.cloud.dht.meta.InstanceExclusionZK;
import com.ms.silverking.cloud.dht.meta.MetaClient;

public class StartOfCurrentExclusionTest {
  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      if (args.length < 2) {
        System.out.println("<gcName> <server>");
      } else {
        InstanceExclusionZK ieZK;
        MetaClient mc;
        String gcName;
        String server;
        Map<String, Long> exclusionSetStartMap;
        long v;

        gcName = args[0];
        server = args[1];
        mc = new MetaClient(SKGridConfiguration.parseFile(gcName));
        ieZK = new InstanceExclusionZK(mc);
        exclusionSetStartMap = ieZK.getStartOfCurrentWorrisome(ImmutableSet.of(server));
        v = exclusionSetStartMap.get(server);
        System.out.printf("StartOfCurrentExclusionTest: %s\n", v);
        System.out.printf("mzxid: %d\n", ieZK.getVersionMzxid(v));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
