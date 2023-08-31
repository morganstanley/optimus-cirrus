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
package com.ms.silverking.cloud.dht.daemon.storage.convergence.management;

import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import com.ms.silverking.cloud.dht.daemon.storage.convergence.management.CentralConvergenceController.SyncTargets;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.id.UUIDBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RingManagerControlImpl extends UnicastRemoteObject implements RingManagerControl {
  private final DHTRingManager rm;
  private final Registry registry;

  private static final long serialVersionUID = -1659250097671179852L;

  public static final int defaultRegistryPort = 2099;

  private static Logger log = LoggerFactory.getLogger(RingManagerControlImpl.class);

  protected RingManagerControlImpl(DHTRingManager rm, int port)
      throws RemoteException, AlreadyBoundException {
    super();
    this.rm = rm;
    registry = LocateRegistry.createRegistry(port);
    registry.rebind(RingManagerControl.getRegistryName(rm.getDHTName()), this);
  }

  protected RingManagerControlImpl(DHTRingManager rm)
      throws RemoteException, AlreadyBoundException {
    this(rm, defaultRegistryPort);
  }

  @Override
  public void setMode(Mode mode) {
    rm.setMode(mode);
  }

  @Override
  public Mode getMode() {
    return rm.getMode();
  }

  @Override
  public UUIDBase setTarget(Triple<String, Long, Long> target) {
    log.info("RingManagerControlImpl.setTarget {}", target);
    return rm.setTarget(target);
  }

  @Override
  public UUIDBase syncData(
      Triple<String, Long, Long> source,
      Triple<String, Long, Long> target,
      SyncTargets syncTargets) {
    log.info("RingManagerControlImpl.syncData {}", source);
    return rm.syncData(source, target, syncTargets);
  }

  @Override
  public UUIDBase recoverData() {
    return rm.recoverData();
  }

  @Override
  public void requestChecksumTree(
      Triple<Long, Long, Long> nsAndRegion,
      Triple<String, Long, Long> source,
      Triple<String, Long, Long> target,
      String owner) {
    log.info(
        "RingManagerControlImpl.requestChecksumTree {} {} {} {}",
        nsAndRegion,
        source,
        target,
        owner);
    rm.requestChecksumTree(nsAndRegion, source, target, owner);
  }

  @Override
  public String getDHTConfiguration() throws RemoteException {
    return rm.getDHTConfiguration().toString();
  }

  /////////////////////////////////////////////////

  @Override
  public void stop(UUIDBase uuid) {
    rm.stop(uuid);
  }

  @Override
  public void waitForCompletion(UUIDBase uuid) {
    rm.waitForCompletion(uuid);
  }

  @Override
  public RequestStatus getStatus(UUIDBase uuid) {
    return rm.getStatus(uuid);
  }

  @Override
  public void reap() {
    rm.reap();
  }

  @Override
  public UUIDBase getCurrentConvergenceID() {
    return rm.getCurrentConvergenceID();
  }
}
