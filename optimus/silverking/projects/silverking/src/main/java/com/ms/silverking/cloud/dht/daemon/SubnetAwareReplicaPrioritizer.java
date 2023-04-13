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

import java.io.IOException;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.util.Arrays;

import com.ms.silverking.cloud.dht.client.crypto.AESEncrypterDecrypter;
import com.ms.silverking.cloud.dht.client.crypto.Encrypter;
import com.ms.silverking.net.IPAddrUtil;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.util.ArrayUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Prioritizes replicas as follows:
 * 1) equal to local ip
 * 2) in same subnet as local ip
 * 3) others
 * <p>
 * Within classes 2 and 3, a psuedorandom (but consistent per each ip) prioritization is used
 */
public class SubnetAwareReplicaPrioritizer implements ReplicaPrioritizer {
  private final IPAndPort myIPAndPort;
  private final int subnetPrefixLength;
  private final int localSubnet;
  private final Encrypter encrypter;

  private static final int defaultPrefixLength = 24;

  private static Logger log = LoggerFactory.getLogger(SubnetAwareReplicaPrioritizer.class);

  public SubnetAwareReplicaPrioritizer(IPAndPort myIPAndPort) {
    NetworkInterface localNIC;
    int _subnetPrefixLength;

    this.myIPAndPort = myIPAndPort;
    this.encrypter = new AESEncrypterDecrypter();
    _subnetPrefixLength = defaultPrefixLength;
    try {
      localNIC = NetworkInterface.getByInetAddress(myIPAndPort.toInetSocketAddress().getAddress());
      if (localNIC == null) {
        log.warn("SubnetAwareReplicaPrioritizer can't find local subnet");
      } else {
        for (InterfaceAddress a : localNIC.getInterfaceAddresses()) {
          if (a.getNetworkPrefixLength() <= IPAddrUtil.IPV4_BYTES * 8) {
            _subnetPrefixLength = a.getNetworkPrefixLength();
            break;
          }
        }
      }
    } catch (IOException ioe) {
      log.warn("Error finding local subnet", ioe);
    }
    subnetPrefixLength = _subnetPrefixLength;
    localSubnet = myIPAndPort.getSubnetAsInt(subnetPrefixLength);
  }

  @Override
  public int compare(IPAndPort o1, IPAndPort o2) {
    if (o1.equals(myIPAndPort)) {
      if (o2.equals(myIPAndPort)) {
        return 0;
      } else {
        return -1;
      }
    } else {
      if (o2.equals(myIPAndPort)) {
        return 1;
      } else {
        return compareOthers(o1, o2);
      }
    }
  }

  private int compareOthers(IPAndPort o1, IPAndPort o2) {
    if (inLocalSubnet(o1)) {
      if (inLocalSubnet(o2)) {
        return compareRandomly(o1, o2);
      } else {
        return -1;
      }
    } else {
      if (inLocalSubnet(o2)) {
        return 1;
      } else {
        return compareRandomly(o1, o2);
      }
    }
  }

  private boolean inLocalSubnet(IPAndPort o1) {
    return o1.getSubnetAsInt(subnetPrefixLength) == localSubnet;
  }

  private int compareRandomly(IPAndPort o1, IPAndPort o2) {
    if (o1.equals(o2)) {
      //System.out.println("Equal");
      return 0;
    } else {
      //System.out.println("Comparing randomly");
      return ArrayUtil.compareSigned(encrypter.encrypt(o1.getIP()), encrypter.encrypt(o2.getIP()));
    }
  }

  public static void main(String[] args) {
    try {
      SubnetAwareReplicaPrioritizer p;
      IPAndPort localIP;
      IPAndPort[] ips;

      localIP = new IPAndPort(IPAddrUtil.localIP(), 0);
      ips = new IPAndPort[args.length];
      for (int i = 0; i < args.length; i++) {
        ips[i] = new IPAndPort(args[i]);
      }
      p = new SubnetAwareReplicaPrioritizer(localIP);
      for (int i = 0; i < ips.length; i++) {
        for (int j = 0; j < ips.length; j++) {
          System.out.printf("%20s\t%20s\t%s\n", ips[i], ips[j], p.compare(ips[i], ips[j]));
        }
        System.out.println();
      }
      Arrays.sort(ips, p);
      for (int i = 0; i < ips.length; i++) {
        System.out.printf("%d:\t%s\n", i, ips[i]);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
