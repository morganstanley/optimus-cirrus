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
package com.ms.silverking.cloud.dht.net;

import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;

import com.ms.silverking.cloud.dht.SessionPolicyOnDisconnect;
import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.common.SimpleValueCreator;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.dht.daemon.PeerHealthMonitor;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.net.AddrAndPort;
import com.ms.silverking.net.IPAddrUtil;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.net.async.AddressStatusProvider;
import com.ms.silverking.net.async.ConnectionController;
import com.ms.silverking.net.async.MultipleConnectionQueueLengthListener;
import com.ms.silverking.net.async.NewConnectionTimeoutController;
import com.ms.silverking.net.async.PersistentAsyncServer;
import com.ms.silverking.net.async.QueueingConnectionLimitListener;
import com.ms.silverking.net.async.SelectorController;
import com.ms.silverking.net.security.AuthFailedException;
import com.ms.silverking.time.AbsMillisTimeSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageGroupBase {
  private final PersistentAsyncServer<MessageGroupConnection> paServer;
  private final IPAndPort myIPAndPort;
  private final AbsMillisTimeSource deadlineTimeSource;
  private final ValueCreator myID;
  private final MessageGroupReceiver messageGroupReceiver;
  private final IPAliasMap aliasMap;
  private final boolean isClient;

  private static final boolean debug = false;

  private static Logger log = LoggerFactory.getLogger(MessageGroupBase.class);

  private MessageGroupBase(
      int interfacePort,
      IPAndPort myIPAndPort,
      MessageGroupReceiver messageGroupReceiver,
      AbsMillisTimeSource deadlineTimeSource,
      NewConnectionTimeoutController newConnectionTimeoutController,
      QueueingConnectionLimitListener limitListener,
      int queueLimit,
      int numSelectorControllers,
      String controllerClass,
      MultipleConnectionQueueLengthListener mqListener,
      UUIDBase mqUUID,
      IPAliasMap aliasMap,
      boolean isClient,
      SessionPolicyOnDisconnect onDisconnect)
      throws IOException {
    this.myIPAndPort = myIPAndPort;
    this.deadlineTimeSource =
        deadlineTimeSource != null ? deadlineTimeSource : SystemTimeUtil.timerDrivenTimeSource;
    MessageGroupConnectionCreator connectionCreator =
        new MessageGroupConnectionCreator(messageGroupReceiver, limitListener, queueLimit);
    paServer =
        new PersistentAsyncServer(
            interfacePort,
            connectionCreator,
            newConnectionTimeoutController,
            numSelectorControllers,
            controllerClass,
            mqListener,
            mqUUID,
            SelectorController.defaultSelectionThreadWorkLimit,
            isClient,
            onDisconnect);
    myID = SimpleValueCreator.forLocalProcess();
    this.messageGroupReceiver = messageGroupReceiver;
    if (aliasMap != null) {
      this.aliasMap = aliasMap;
    } else {
      this.aliasMap = IPAliasMap.identityMap();
    }
    this.isClient = isClient;
  }

  public static MessageGroupBase newClientMessageGroupBase(
      int interfacePort,
      MessageGroupReceiver messageGroupReceiver,
      AbsMillisTimeSource deadlineTimeSource,
      NewConnectionTimeoutController newConnectionTimeoutController,
      QueueingConnectionLimitListener limitListener,
      int queueLimit,
      int numSelectorControllers,
      String controllerClass,
      IPAliasMap aliasMap)
      throws IOException {
    return newClientMessageGroupBase(
        interfacePort,
        messageGroupReceiver,
        deadlineTimeSource,
        newConnectionTimeoutController,
        limitListener,
        queueLimit,
        numSelectorControllers,
        controllerClass,
        aliasMap,
        SessionPolicyOnDisconnect.DoNothing);
  }

  public static MessageGroupBase newClientMessageGroupBase(
      int interfacePort,
      MessageGroupReceiver messageGroupReceiver,
      AbsMillisTimeSource deadlineTimeSource,
      NewConnectionTimeoutController newConnectionTimeoutController,
      QueueingConnectionLimitListener limitListener,
      int queueLimit,
      int numSelectorControllers,
      String controllerClass,
      IPAliasMap aliasMap,
      SessionPolicyOnDisconnect onDisconnect)
      throws IOException {
    return new MessageGroupBase(
        interfacePort,
        new IPAndPort(IPAddrUtil.localIP(), interfacePort),
        messageGroupReceiver,
        deadlineTimeSource,
        newConnectionTimeoutController,
        limitListener,
        queueLimit,
        numSelectorControllers,
        controllerClass,
        null,
        null,
        aliasMap,
        true,
        onDisconnect);
  }

  public static MessageGroupBase newServerMessageGroupBase(
      int interfacePort,
      IPAndPort myIPAndPort,
      MessageGroupReceiver messageGroupReceiver,
      AbsMillisTimeSource deadlineTimeSource,
      NewConnectionTimeoutController newConnectionTimeoutController,
      QueueingConnectionLimitListener limitListener,
      int queueLimit,
      int numSelectorControllers,
      String controllerClass,
      MultipleConnectionQueueLengthListener mqListener,
      UUIDBase mqUUID,
      IPAliasMap aliasMap)
      throws IOException {
    return new MessageGroupBase(
        interfacePort,
        myIPAndPort,
        messageGroupReceiver,
        deadlineTimeSource,
        newConnectionTimeoutController,
        limitListener,
        queueLimit,
        numSelectorControllers,
        controllerClass,
        mqListener,
        mqUUID,
        aliasMap,
        false,
        SessionPolicyOnDisconnect.DoNothing);
  }

  public void enable() {
    paServer.enable();
  }

  public boolean isClient() {
    return this.isClient;
  }

  public boolean isRunning() {
    return paServer.isRunning();
  }

  public AbsMillisTimeSource getAbsMillisTimeSource() {
    return deadlineTimeSource;
  }

  public byte[] getMyID() {
    return myID.getBytes();
  }

  public int getInterfacePort() {
    return paServer.getPort();
  }

  public IPAndPort getIPAndPort() {
    return myIPAndPort;
  }

  public void setAddressStatusProvider(AddressStatusProvider addressStatusProvider) {
    paServer.setAddressStatusProvider(addressStatusProvider);
  }

  private boolean isLocalDest(AddrAndPort dest) {
    return this.getIPAndPort().equals(dest);
  }

  public void send(MessageGroup mg, AddrAndPort dest) {
    if (debug) {
      log.debug("Sending: {} to {}", mg, dest);
    }
    if (isLocalDest(dest)) {
      messageGroupReceiver.receive(MessageGroup.clone(mg), null);
    } else {
      try {
        paServer.sendAsynchronous(
            aliasMap.daemonToInterface(dest),
            mg,
            null,
            null,
            mg.getDeadlineAbsMillis(deadlineTimeSource));
      } catch (UnknownHostException exception) {
        throw new RuntimeException(exception);
      }
    }
  }

  public void ensureConnected(AddrAndPort dest) throws ConnectException, AuthFailedException {
    paServer.ensureConnected(aliasMap.daemonToInterface(dest));
  }

  public MessageGroupConnection getConnection(AddrAndPort dest, long deadline)
      throws ConnectException, AuthFailedException {
    return (MessageGroupConnection)
        paServer.getConnection(aliasMap.daemonToInterface(dest), deadline);
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(paServer.getPort());
    return stringBuilder.toString();
  }

  public void shutdown() {
    if (paServer != null) {
      paServer.shutdown();
    }
  }

  public void setPeerHealthMonitor(PeerHealthMonitor peerHealthMonitor) {
    paServer.setSuspectAddressListener(peerHealthMonitor);
  }

  public ConnectionController getConnectionController() {
    return paServer.getConnectionController();
  }
}
