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
package com.ms.silverking.cloud.dht.client.impl;

import java.util.List;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespacePerspectiveOptions;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.OperationOptions;
import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.client.AbsMillisVersionProvider;
import com.ms.silverking.cloud.dht.client.AbsNanosVersionProvider;
import com.ms.silverking.cloud.dht.client.AsynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.ConstantVersionProvider;
import com.ms.silverking.cloud.dht.client.KeyDigestType;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.client.NamespaceCreationException;
import com.ms.silverking.cloud.dht.client.NamespaceLinkException;
import com.ms.silverking.cloud.dht.client.NamespaceModificationException;
import com.ms.silverking.cloud.dht.client.SessionClosedException;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.VersionProvider;
import com.ms.silverking.cloud.dht.client.serialization.SerializationRegistry;
import com.ms.silverking.cloud.dht.common.Context;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.NamespaceProperties;
import com.ms.silverking.cloud.dht.common.OptionsValidator;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.dht.daemon.storage.NamespaceNotCreatedException;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.cloud.dht.net.MessageGroupConnection;
import com.ms.silverking.net.AddrAndPort;
import com.ms.silverking.net.async.QueueingConnectionLimitListener;
import com.ms.silverking.time.AbsMillisTimeSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientNamespace implements QueueingConnectionLimitListener, Namespace {
  private final DHTSessionImpl session;
  private final String name;
  private final NamespaceOptions nsOptions;
  private final Context context;
  private final ActiveClientOperationTable activeOpTable;
  private final AbsMillisTimeSource absMillisTimeSource;
  private final OpSender opSender;
  private final OpSender putSender;
  private final OpSender retrievalSender;
  private final byte[] originator;
  private final SerializationRegistry serializationRegistry;
  private final Namespace parent;
  private final NamespaceLinkMeta nsLinkMeta;

  private static Logger log = LoggerFactory.getLogger(ClientNamespace.class);

  protected enum OpLWTMode {
    AllowUserThreadUsage, DisallowUserThreadUsage;

    public int getDirectCallDepth() {
      switch (this) {
      case AllowUserThreadUsage:
        return Integer.MAX_VALUE;
      case DisallowUserThreadUsage:
        return 0;
      default:
        throw new RuntimeException("panic");
      }
    }
  }

  ;

  /*
   * The current implementation allows puts which are doomed to fail to continue.
   * They will eventually be rejected by the storage node. We don't
   * avoid this currently since this is expected to be a rare case.
   *
   * For receives, however, we track versions more carefully in order to
   * group concurrent receives together. ActiveOperationTable
   * takes care of this.
   *
   * FUTURE - think about whether we want to keep ActiveOperationTable.
   * Seems like put/retrieve should use similar approach more.
   */

  ClientNamespace(DHTSessionImpl session, String name, NamespaceOptions nsOptions,
      SerializationRegistry serializationRegistry, AbsMillisTimeSource absMillisTimeSource, AddrAndPort server,
      Namespace parent, NamespaceLinkMeta nsLinkMeta) {
    MessageGroupBase mgBase;

    mgBase = session.getMessageGroupBase();
    this.session = session;
    this.name = name;
    this.nsOptions = nsOptions;
    this.serializationRegistry = serializationRegistry;
    this.absMillisTimeSource = absMillisTimeSource;
    context = new SimpleNamespaceCreator().createNamespace(name);
    activeOpTable = new ActiveClientOperationTable();
    opSender = new OpSender(server, mgBase);
    putSender = new OpSender(server, mgBase);
    retrievalSender = new OpSender(server, mgBase);
    originator = mgBase.getMyID();
    this.parent = parent;
    this.nsLinkMeta = nsLinkMeta;
    if (nsOptions.getVersionMode() != NamespaceVersionMode.SINGLE_VERSION || !nsOptions.getAllowLinks()) {
      assert nsLinkMeta == null;
    }
  }

  @Override
  public String getName() {
    return name;
  }

  Context getContext() {
    return context;
  }

  @Override
  public NamespaceOptions getOptions() {
    return nsOptions;
  }

  @Override
  public boolean isServerTraceEnabled() {
    return session.isServerTraceEnabled();
  }

  @Override
  public <K, V> NamespacePerspectiveOptions<K, V> getDefaultNSPOptions(Class<K> keyClass, Class<V> valueClass) {
    VersionProvider versionProvider;

    // FUTURE - Currently we intercept request for NS-supplied versions and provide them here.
    // We may want to provide them on the server. Code for this is already in ActiveProxyPut.
    switch (nsOptions.getVersionMode()) {
    case SINGLE_VERSION:
      // SINGLE_VERSION internally expects the version number to be abs millis
      versionProvider = new AbsMillisVersionProvider(SystemTimeUtil.skSystemTimeSource);
      break;
    case CLIENT_SPECIFIED:
      versionProvider = new ConstantVersionProvider(SystemTimeUtil.skSystemTimeSource.absTimeMillis());
      break;
    case SYSTEM_TIME_MILLIS:
      versionProvider = new AbsMillisVersionProvider(SystemTimeUtil.skSystemTimeSource);
      break;
    case SEQUENTIAL: // TODO (OPTIMUS-0000): for now treat sequential as nanos
    case SYSTEM_TIME_NANOS:
      versionProvider = new AbsNanosVersionProvider(SystemTimeUtil.skSystemTimeSource);
      break;
    default:
      throw new RuntimeException("panic");
    }

    return new NamespacePerspectiveOptions<K, V>(keyClass, valueClass, KeyDigestType.MD5, // FUTURE - centralize
        nsOptions.getDefaultPutOptions(), nsOptions.getDefaultInvalidationOptions(), nsOptions.getDefaultGetOptions(),
        nsOptions.getDefaultWaitOptions(), versionProvider, null);
  }

  DHTSessionImpl getSession() {
    return session;
  }

  public AbsMillisTimeSource getAbsMillisTimeSource() {
    return absMillisTimeSource;
  }

  public byte[] getOriginator() {
    return originator;
  }

  public ActivePutListeners getActivePutListeners() {
    return activeOpTable.getActivePutListeners();
  }

  public ActiveRetrievalListeners getActiveRetrievalListeners() {
    return activeOpTable.getActiveRetrievalListeners();
  }

  public ActiveVersionedBasicOperations getActiveVersionedBasicOperations() {
    return activeOpTable.getActiveVersionedBasicOperations();
  }

  OpSender getRetrievalSender() {
    return retrievalSender;
  }

  @Override
  public void queueAboveLimit() {
    putSender.pause();
    retrievalSender.pause();
  }

  @Override
  public void queueBelowLimit() {
    putSender.unpause();
    retrievalSender.unpause();
  }

  // operation

  public <K, V> void startOperation(AsyncOperationImpl opImpl, OpLWTMode opLWTMode) throws SessionClosedException {
    session.assertOpen();
    validateOpOptions(opImpl.operation.options);
    switch (opImpl.getType()) {
    case RETRIEVE:
      retrievalSender.addWorkForGrouping(opImpl, opLWTMode.getDirectCallDepth());
      break;
    case PUT:
      putSender.addWorkForGrouping(opImpl, opLWTMode.getDirectCallDepth());
      break;
    default:
      opSender.addWorkForGrouping(opImpl, 0);
      break; // TODO (OPTIMUS-0000): don't group
    }
  }

  // receive

  public void receive(MessageGroup message, MessageGroupConnection connection) {
    if (log.isDebugEnabled()) {
      log.debug("  *** Received: {}", message);
      message.displayForDebug(false);
      //message.displayForDebug(true);
    }
    switch (message.getMessageType()) {
    case PUT_RESPONSE:
    case PUT_RESPONSE_TRACE:
      activeOpTable.getActivePutListeners().receivedPutResponse(message);
      break;
    case RETRIEVE_RESPONSE:
    case RETRIEVE_RESPONSE_TRACE:
      //activeOpTable.receivedRetrievalResponse(message);
      activeOpTable.getActiveRetrievalListeners().receivedRetrievalResponse(message);
      break;
    case OP_RESPONSE:
      activeOpTable.getActiveVersionedBasicOperations().receivedOpResponse(message);
      break;
    case ERROR_RESPONSE:
      //Check if there is any active listener for the ID
      if (activeOpTable.getActiveRetrievalListeners().isResponsibleFor(message.getUUID())) {
        activeOpTable.getActiveRetrievalListeners().receivedRetrievalResponse(message);
      } else if (activeOpTable.getActivePutListeners().isResponsibleFor(message.getUUID())) {
        activeOpTable.getActivePutListeners().receivedPutResponse(message);
      }
      break;
    case CHECKSUM_TREE: // FUTURE - for testing, consider removing
      activeOpTable.receivedChecksumTree(message); // FUTURE - for testing, consider removing
      break;
    default:
      throw new RuntimeException("Unexpected message type: " + message.getMessageType());
    }
  }

  void checkForTimeouts(long curTimeMillis, boolean exclusionSetHasChanged) {
    log.debug("checkForTimeouts: {}", name);
    activeOpTable.checkForTimeouts(curTimeMillis, putSender, retrievalSender, exclusionSetHasChanged);
  }

  List<AsyncOperationImpl> getActiveAsyncOperations() {
    return activeOpTable.getActiveAsyncOperations();
  }

  @Override
  public <K, V> AsynchronousNamespacePerspective<K, V> openAsyncPerspective(
      NamespacePerspectiveOptions<K, V> nspOptions) {
    return new AsynchronousNamespacePerspectiveImpl<K, V>(this, name,
        new NamespacePerspectiveOptionsImpl<>(nspOptions, serializationRegistry));
  }

  @Override
  public <K, V> AsynchronousNamespacePerspective<K, V> openAsyncPerspective(Class<K> keyClass, Class<V> valueClass) {
    return new AsynchronousNamespacePerspectiveImpl<K, V>(this, name,
        new NamespacePerspectiveOptionsImpl<>(getDefaultNSPOptions(keyClass, valueClass), serializationRegistry));
  }

  @Override
  public AsynchronousNamespacePerspective<String, byte[]> openAsyncPerspective() {
    return openAsyncPerspective(DHTConstants.defaultKeyClass, DHTConstants.defaultValueClass);
  }

  @Override
  public <K, V> SynchronousNamespacePerspective<K, V> openSyncPerspective(
      NamespacePerspectiveOptions<K, V> nspOptions) {
    return new SynchronousNamespacePerspectiveImpl<K, V>(this, name,
        new NamespacePerspectiveOptionsImpl<>(nspOptions, serializationRegistry));
  }

  @Override
  public <K, V> SynchronousNamespacePerspective<K, V> openSyncPerspective(Class<K> keyClass, Class<V> valueClass) {
    return new SynchronousNamespacePerspectiveImpl<K, V>(this, name,
        new NamespacePerspectiveOptionsImpl<>(getDefaultNSPOptions(keyClass, valueClass), serializationRegistry));
  }

  @Override
  public SynchronousNamespacePerspective<String, byte[]> openSyncPerspective() {
    return openSyncPerspective(DHTConstants.defaultKeyClass, DHTConstants.defaultValueClass);
  }

  // misc.

  @Override
  public String toString() {
    return name;
  }

  @Override
  public Namespace clone(String childName) throws NamespaceCreationException {
    long creationTime;
    long minVersion;

    creationTime = SystemTimeUtil.skSystemTimeSource.absTimeNanos();
    switch (nsOptions.getVersionMode()) {
    case SINGLE_VERSION:
      minVersion = 0;
      break;
    case CLIENT_SPECIFIED:
      throw new RuntimeException(
          "Namespace.clone() must be provided a version " + "for version mode of CLIENT_SPECIFIED");
    case SEQUENTIAL:
      throw new RuntimeException("Namespace.clone() not supported for version mode of SEQUENTIAL");
    case SYSTEM_TIME_MILLIS:
      minVersion = SystemTimeUtil.skSystemTimeSource.absTimeMillis();
      break;
    case SYSTEM_TIME_NANOS:
      minVersion = SystemTimeUtil.skSystemTimeSource.absTimeNanos();
      break;
    default:
      throw new RuntimeException("Panic");
    }
    return session.createNamespace(childName,
        new NamespaceProperties(nsOptions, childName, this.name, minVersion, creationTime));
  }

  @Override
  public Namespace modifyNamespace(NamespaceOptions nsOptions) throws NamespaceModificationException {
    if (parent != null) {
      // TODO (OPTIMUS-0000): for now we just disallow the modification from cloned namespace
      throw new NamespaceModificationException("modification from cloned namespace is not currently supported");
    } else {
      return session.modifyNamespace(name, nsOptions);
    }
  }

  @Override
  public Namespace clone(String childName, long minVersion) throws NamespaceCreationException {
    long creationTime;

    creationTime = SystemTimeUtil.skSystemTimeSource.absTimeNanos();
    switch (nsOptions.getVersionMode()) {
    case CLIENT_SPECIFIED:
      break;
    case SEQUENTIAL:
      throw new RuntimeException("Namespace.clone() not supported for version mode of SEQUENTIAL");
    case SINGLE_VERSION:
    case SYSTEM_TIME_MILLIS:
    case SYSTEM_TIME_NANOS:
      throw new RuntimeException(
          "Namespace.clone() can only be supplied a version " + "for a version mode of CLIENT_SPECIFIED");
    default:
      throw new RuntimeException("Panic");
    }
    return session.createNamespace(childName,
        new NamespaceProperties(nsOptions, childName, this.name, minVersion, creationTime));
  }

  public void linkTo(String parentName) throws NamespaceLinkException {
    Namespace parent;

    if (!nsOptions.getAllowLinks()) {
      throw new NamespaceLinkException("Links not allowed for: " + name);
    }

    try {
      parent = session.getNamespace(parentName);
    } catch (NamespaceNotCreatedException nnce) {
      throw new NamespaceLinkException("Parent not created: " + parentName);
    }

    if (!parent.getOptions().equals(nsOptions)) {
      throw new NamespaceLinkException("Parent and child options differ:\n" + parent.getOptions() + "\n" + nsOptions);
    }

    if (nsOptions.getVersionMode() != NamespaceVersionMode.SINGLE_VERSION) {
      throw new NamespaceLinkException("Namespace.linkTo() only supported for write-once namespaces");
    } else {
      nsLinkMeta.createLink(name, parentName);
    }
  }

  public void validateOpOptions(OperationOptions opOptions) {
        /*  Case(1) session.isServerTraceEnabled() == true:
                Server supports both old protocol and new trace protocol
                => So let it pass the check
            Case(2) session.isServerTraceEnabled() == false:
                Server could:
                   (2.1) supports both old protocol and new trace protocol but has trace feature disabled
                   (2.2) only supports old protocol (run in old SK version)
               => So we let this clientside check fail if request will use new protocol (i.e. has TraceID)
                  as server will ignore the traceID (case-2.10) or even crash and go down (case-2.2)
         */
    if (opOptions.hasTraceID() && !session.isServerTraceEnabled()) {
      throw new IllegalArgumentException(String.format(
          "Server[%s] has trace feature disabled or running in a old version, which doesn't expect to have traceID in" +
              " the opOptions (traceIDProvider=[%s])",
          session.getDhtConfig().toString(), opOptions.getTraceIDProvider().toString()));
    }
  }

  public void validatePutOptions(PutOptions putOptions) {
    OptionsValidator.validatePutOptions(putOptions);
    if (nsOptions.getMaxValueSize() > nsOptions.getSegmentSize()) {
      // Values > a segment are allowed. Ensure that fragmentation is set to a sane value
      if (putOptions.getFragmentationThreshold() > nsOptions.getSegmentSize() - DHTConstants.segmentSafetyMargin) {
        throw new IllegalArgumentException(
            "Values larger than a segment are allowed, " + "but putOptions.getFragmentationThreshold() > nsOptions" + ".getSegmentSize() - DHTConstants.segmentSafetyMargin");
      }
    } else {
      // Values can all fit in one segment
    }
  }
}
