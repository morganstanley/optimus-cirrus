// HostAndPort.java

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
package com.ms.silverking.net;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import com.ms.silverking.numeric.NumConversion;

/**
 * Bundles a hostname and a port.
 */
public final class HostAndPort implements Comparable<HostAndPort>, Serializable, AddrAndPort {
  private final String hostName;
  private final int port;
  private InetSocketAddress inetSocketAddress;

  private static final long serialVersionUID = 6519167306053592030L;

  public HostAndPort(String hostName, int port) {
    this.hostName = hostName;
    this.port = port;
  }

  public HostAndPort(byte[] ip, int port) throws UnknownHostException {
    InetAddress inetAddress;

    inetAddress = InetAddress.getByAddress(ip);
    this.hostName = inetAddress.getHostName();
    this.port = port;
  }

  public HostAndPort(byte[] def) throws UnknownHostException {
    InetAddress inetAddress;

    port = NumConversion.bytesToInt(def, 4);
    inetAddress = InetAddress.getByAddress(Arrays.copyOfRange(def, 5, 9));
    this.hostName = inetAddress.getHostName();
  }

  public static HostAndPort localHostAndPort(int port) {
    return new HostAndPort(IPAddrUtil.localIPString(), port);
  }

  /**
   * Optimization for cases where toInetAddress will be called
   * many times. Precompute so that we can pass it back without
   * the overhead of creation.
   */
  public void computeInetAddress() {
    inetSocketAddress = new InetSocketAddress(hostName, port);
  }

  public InetSocketAddress toInetSocketAddress() throws UnknownHostException {
    if (inetSocketAddress == null) {
      computeInetAddress();
    }
    return inetSocketAddress;
        /*
        if (inetSocketAddress == null) {
            return new InetSocketAddress(hostName, port);
        } else {
            return inetSocketAddress;
        }
        */
  }

  public byte[] getBytes() throws UnknownHostException {
    byte[] ip;
    byte[] ipAndPort;

    ip = InetAddress.getByName(hostName).getAddress();
    ipAndPort = new byte[8];
    System.arraycopy(ip, 0, ipAndPort, 0, ip.length);
    NumConversion.intToBytes(port, ipAndPort, ip.length);
    return ipAndPort;
  }

  public HostAndPort(String hostName) {
    String[] elements;

    elements = hostName.split(":");
    if (elements.length != 2) {
      throw new RuntimeException("Bad HostAndPort: " + hostName);
    }
    this.hostName = elements[0];
    port = Integer.parseInt(elements[1]);
  }

  public String getHostName() {
    return hostName;
  }

  public int getPort() {
    return port;
  }

  @Override
  public String toString() {
    return hostName + ":" + port;
  }

  @Override
  public boolean equals(Object other) {
    HostAndPort otherHostAndPort;

    otherHostAndPort = (HostAndPort) other;
    if (port != otherHostAndPort.port) {
      return false;
    } else {
      return hostName.equals(otherHostAndPort.hostName);
    }
  }

  @Override
  public int hashCode() {
    return hostName.hashCode() ^ port;
  }

  public static HostAndPort[] parseMultiple(String def) {
    String[] defs;
    HostAndPort[] hps;

    defs = def.split(AddrAndPort.multipleDefDelimiter);
    hps = new HostAndPort[defs.length];
    for (int i = 0; i < defs.length; i++) {
      hps[i] = new HostAndPort(defs[i]);
    }
    return hps;
  }

  public static HostAndPort[] parseMultiple(List<String> defs) {
    HostAndPort[] hps;

    hps = new HostAndPort[defs.size()];
    for (int i = 0; i < defs.size(); i++) {
      hps[i] = new HostAndPort(defs.get(i));
    }
    return hps;
  }

  public static String toString(HostAndPort[] hps) {
    StringBuilder sb;
    boolean first;

    sb = new StringBuilder();
    first = true;
    for (HostAndPort hp : hps) {
      if (!first) {
        sb.append(AddrAndPort.multipleDefDelimiter);
      } else {
        first = false;
      }
      sb.append(hp.toString());
    }
    return sb.toString();
  }

  /////////////////////////////////////////////////////////////////

  public static HostAndPort readFromExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    return new HostAndPort(in.readUTF(), in.readInt());
  }

  public void writeToExternal(ObjectOutput out) throws IOException {
    out.writeUTF(hostName);
    out.writeInt(port);
  }

  /////////////////////////////////////////////////////////////////

  @Override
  public int compareTo(HostAndPort o) {
    int result;

    result = hostName.compareTo(o.hostName);
    if (result != 0) {
      return result;
    } else {
      if (port < o.port) {
        return -1;
      } else if (port > o.port) {
        return 1;
      } else {
        return 0;
      }
    }
  }

  /////////////////////////////////////////////////////////////////

  public IPAndPort toIPAndPort() throws UnknownHostException {
    String ip;

    ip = InetAddress.getByName(hostName).getHostAddress();
    return new IPAndPort(ip, port);
  }

  //public static void main(String[] args) {
  //    System.out.println(new HostAndPort(args[0]));
  //}
}