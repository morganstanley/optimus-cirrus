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
package com.ms.silverking.cloud.zookeeper;

import com.google.common.base.Preconditions;
import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.net.AddrAndPort;
import com.ms.silverking.net.AddrAndPortUtil;
import com.ms.silverking.net.HostAndPort;

public class ZooKeeperConfig {
  private final AddrAndPort[] ensemble;
  private final String proid;
  private final String chroot;

  @OmitGeneration
  public ZooKeeperConfig(AddrAndPort[] ensemble, String proid, String chroot) {
    Preconditions.checkNotNull(ensemble, "ensemble null");
    this.ensemble = ensemble;
    this.proid = (proid == null ? "" : proid);
    this.chroot = (chroot == null ? "" : chroot);
  }

  @OmitGeneration
  public ZooKeeperConfig(Triple<AddrAndPort[], String, String> ensemble_proid_chroot) {
    this(
        ensemble_proid_chroot.getV1(),
        ensemble_proid_chroot.getV2(),
        ensemble_proid_chroot.getV3());
  }

  @OmitGeneration
  public ZooKeeperConfig(AddrAndPort[] ensemble) {
    this(ensemble, "", "");
  }

  public ZooKeeperConfig(String def) {
    this(parseDef(def));
  }

  private static Triple<AddrAndPort[], String, String> parseDef(String def) {
    String ensembleDef;
    String proid = "";
    String chroot = "";

    int proidIndex = def.indexOf('@');
    int chrootIndex = def.indexOf('/');
    if (proidIndex < 0 && chrootIndex < 0) {
      ensembleDef = def;
    } else if (chrootIndex < 0) {
      ensembleDef = def.substring(0, proidIndex);
      proid = def.substring(proidIndex + 1);
    } else if (proidIndex < 0) {
      ensembleDef = def.substring(0, chrootIndex);
      chroot = def.substring(chrootIndex);
    } else {
      ensembleDef = def.substring(0, proidIndex);
      proid = def.substring(proidIndex + 1, chrootIndex);
      chroot = def.substring(chrootIndex);
    }

    AddrAndPort[] ensemble = HostAndPort.parseMultiple(ensembleDef);
    return new Triple<>(ensemble, proid, chroot);
  }

  public AddrAndPort[] getEnsemble() {
    return ensemble;
  }

  public String getProid() {
    return proid;
  }

  public String getChroot() {
    return chroot;
  }

  @Override
  public String toString() {
    return getEnsembleString() + (proid.isEmpty() ? "" : ("@" + proid)) + chroot;
  }

  public String getEnsembleString() {
    return AddrAndPortUtil.toString(ensemble);
  }

  public String getConnectString() {
    return getEnsembleString() + chroot;
  }

  @Override
  public int hashCode() {
    return AddrAndPortUtil.hashCode(ensemble) ^ proid.hashCode() ^ chroot.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof ZooKeeperConfig)) {
      return false;
    }

    ZooKeeperConfig otherZKC = (ZooKeeperConfig) other;
    if (ensemble.length != otherZKC.ensemble.length) {
      return false;
    }
    // Note that this implementation is currently order-sensitive
    for (int i = 0; i < ensemble.length; i++) {
      if (!ensemble[i].equals(otherZKC.ensemble[i])) {
        return false;
      }
    }
    return proid.equals(otherZKC.proid) && chroot.equals(otherZKC.chroot);
  }
}
