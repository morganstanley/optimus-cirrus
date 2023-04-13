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

import com.ms.silverking.net.IPAndPort;

public class SimpleIPDistancePrioritizer implements ReplicaPrioritizer {
  private final IPAndPort referenceIP;
  private final int mask;

  public SimpleIPDistancePrioritizer(IPAndPort referenceIP, int mask) {
    this.referenceIP = referenceIP;
    this.mask = mask;
  }

  @Override
  public int compare(IPAndPort r0, IPAndPort r1) {
    long d0;
    long d1;

    d0 = distance(referenceIP, r0);
    d1 = distance(referenceIP, r1);
    if (d0 < d1) {
      return -1;
    } else if (d0 > d1) {
      return 1;
    } else {
      return Integer.compare(Long.hashCode(r0.toLong()), Long.hashCode(r1.toLong()));
    }
  }

  private long ipAsMaskedLong(IPAndPort ip) {
    return (ip.getIPAsInt() & mask) & 0x00000000ffffffff;
  }

  private long distance(IPAndPort r0, IPAndPort r1) {
    long i0;
    long i1;

    i0 = ipAsMaskedLong(r0);
    i1 = ipAsMaskedLong(r1);
    return Math.abs(i0 - i1);
  }

  public static void main(String[] args) {
    try {
      SimpleIPDistancePrioritizer p;
      IPAndPort[] ips;
      IPAndPort referenceIP;
      int mask;

      referenceIP = new IPAndPort(args[0]);
      mask = Integer.parseUnsignedInt(args[1], 16);
      ips = new IPAndPort[args.length - 2];
      for (int i = 2; i < args.length; i++) {
        ips[i - 2] = new IPAndPort(args[i]);
      }
      p = new SimpleIPDistancePrioritizer(referenceIP, mask);
      for (int i = 0; i < ips.length; i++) {
        for (int j = 0; j < ips.length; j++) {
          System.out.printf("%20s\t%20s\t%s\n", ips[i], ips[j], p.compare(ips[i], ips[j]));
        }
        System.out.println();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
