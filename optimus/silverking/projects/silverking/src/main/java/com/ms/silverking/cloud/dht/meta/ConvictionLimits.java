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
package com.ms.silverking.cloud.dht.meta;

import com.ms.silverking.text.FieldsRequirement;
import com.ms.silverking.text.ObjectDefParser2;

public class ConvictionLimits {
  private final int totalGuiltyServers;
  private final int guiltyServersPerHour;

  private static final ConvictionLimits template = new ConvictionLimits(0, 0);

  static {
    ObjectDefParser2.addParser(template, FieldsRequirement.REQUIRE_ALL_FIELDS);
  }

  public ConvictionLimits(int totalGuiltyServers, int guiltyServersPerHour) {
    this.totalGuiltyServers = totalGuiltyServers;
    this.guiltyServersPerHour = guiltyServersPerHour;
  }

  public int getTotalGuiltyServers() {
    return totalGuiltyServers;
  }

  public int getGuiltyServersPerHour() {
    return guiltyServersPerHour;
  }

  @Override
  public boolean equals(Object obj) {
    ConvictionLimits o;

    o = (ConvictionLimits) obj;
    return this.totalGuiltyServers == o.totalGuiltyServers && this.guiltyServersPerHour == o.guiltyServersPerHour;
  }

  @Override
  public int hashCode() {
    return Integer.hashCode(totalGuiltyServers) ^ Integer.hashCode(guiltyServersPerHour);
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }

  public static ConvictionLimits parse(String def) {
    return ObjectDefParser2.parse(ConvictionLimits.class, def);
  }
}
