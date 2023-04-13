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

import static com.ms.silverking.cloud.dht.common.OpResult.SESSION_CLOSED;

import java.io.IOException;
import java.net.ConnectException;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.ms.silverking.cloud.dht.GetOptions;
import com.ms.silverking.cloud.dht.NamespaceCreationOptions;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespacePerspectiveOptions;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.SessionPolicyOnDisconnect;
import com.ms.silverking.cloud.dht.WaitOptions;
import com.ms.silverking.cloud.dht.client.AsyncSingleValueRetrieval;
import com.ms.silverking.cloud.dht.client.AsynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.ClientDHTConfiguration;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.client.NamespaceCreationException;
import com.ms.silverking.cloud.dht.client.NamespaceDeletionException;
import com.ms.silverking.cloud.dht.client.NamespaceModificationException;
import com.ms.silverking.cloud.dht.client.NamespaceRecoverException;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.SessionClosedException;
import com.ms.silverking.cloud.dht.client.SessionEstablishmentTimeoutController;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.serialization.SerializationRegistry;
import com.ms.silverking.cloud.dht.common.Context;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.DHTUtil;
import com.ms.silverking.cloud.dht.common.NamespaceOptionsClientNSPImpl;
import com.ms.silverking.cloud.dht.common.NamespaceOptionsClientZKImpl;
import com.ms.silverking.cloud.dht.common.NamespaceOptionsMode;
import com.ms.silverking.cloud.dht.common.NamespaceProperties;
import com.ms.silverking.cloud.dht.common.NamespacePropertiesDeleteException;
import com.ms.silverking.cloud.dht.common.NamespacePropertiesRetrievalException;
import com.ms.silverking.cloud.dht.common.NamespaceUtil;
import com.ms.silverking.cloud.dht.common.TimeoutException;
import com.ms.silverking.cloud.dht.daemon.storage.NamespaceNotCreatedException;
import com.ms.silverking.cloud.dht.meta.MetaClient;
import com.ms.silverking.cloud.dht.meta.NamespaceLinksZK;
import com.ms.silverking.cloud.dht.net.IPAliasMap;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.cloud.dht.net.MessageGroupConnection;
import com.ms.silverking.cloud.dht.net.MessageGroupReceiver;
import com.ms.silverking.cloud.meta.ExclusionSet;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.net.AddrAndPort;
import com.ms.silverking.net.IPAddrUtil;
import com.ms.silverking.net.async.QueueingConnectionLimitListener;
import com.ms.silverking.net.security.AuthFailedException;
import com.ms.silverking.thread.lwt.BaseWorker;
import com.ms.silverking.time.AbsMillisTimeSource;
import com.ms.silverking.util.SafeTimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Concrete implementation of DHTSession.
 */
public class DHTSessionImpl implements DHTSession, MessageGroupReceiver, QueueingConnectionLimitListener {
  protected final MessageGroupBase mgBase;
  private final ClientDHTConfiguration dhtConfig;
  private final ConcurrentMap<Long, ClientNamespace> clientNamespaces;
  private final List<ClientNamespace> clientNamespaceList;
  private final byte[] myIPAndPort;
  private final AbsMillisTimeSource absMillisTimeSource;
  private final SerializationRegistry serializationRegistry;
  private final Worker worker;
  private final NamespaceCreator namespaceCreator;
  private final NamespaceOptionsMode nsOptionsMode;
  private final NamespaceOptionsClientCS nsOptionsClient;
  private final boolean enableMsgGroupTrace;

  private static Logger log = LoggerFactory.getLogger(DHTSessionImpl.class);

  private NamespaceLinkMeta nsLinkMeta;
  private SafeTimerTask timeoutCheckTask;
  private AsynchronousNamespacePerspective<String, String> systemNSP;
  private ExclusionSet exclusionSet;
  volatile private boolean closed = false;

  /*
   * FUTURE - This class can be improved significantly. It contains remnants of the ActiveOperation* implementation.
   * Also to be decided is whether or not the server will share common functionality with the client
   * side. It currently does not, but it did when the ActiveOperation* implementation was in place.
   */

  // FUTURE - server selection currently pinned to preferredServer only; allow for others
  private AddrAndPort server;

  //private final Map<OperationUUID,ActiveOperation>    activeOps;

  // new implementation
  //private final ActiveOperationTable   activeOpTable;
  // FUTURE - THINK IF WE WANT OPERATION TABLE ANY MORE

  // retrieve
  // map of retrievals ns, key to retrieval list
  // this list then maps back to active operation
  // retrieval list maps version to active operation

  // put
  // map of puts ns, key to put list

  // end new implementation

  private static final int timeoutCheckIntervalMillis = 4 * 1000;
  private static final int serverCheckIntervalMillis = 2 * 60 * 1000;
  private static final int serverOrderIntervalMillis = 5 * 60 * 1000;
  private static final int timeoutExclusionSetRetrievalMillis = 2000;
  private static final int exclusionSetRetrievalTimeoutSeconds = 10;

  private static final int connectionQueueLimit = 0;

  private static final int numSelectorControllers = 1;
  private static final String selectorControllerClass = "DHTSessionImpl";

  public DHTSessionImpl(ClientDHTConfiguration dhtConfig, AddrAndPort server, AbsMillisTimeSource absMillisTimeSource,
      SerializationRegistry serializationRegistry, SessionEstablishmentTimeoutController timeoutController,
      NamespaceOptionsMode nsOptionsMode, boolean enableMsgGroupTrace, IPAliasMap aliasMap,
      SessionPolicyOnDisconnect onDisconnect) throws IOException, AuthFailedException {

    this.dhtConfig = dhtConfig;
    this.absMillisTimeSource = absMillisTimeSource;
    this.serializationRegistry = serializationRegistry;
    this.nsOptionsMode = nsOptionsMode;
    this.enableMsgGroupTrace = enableMsgGroupTrace;

    clientNamespaces = new ConcurrentHashMap<>();
    clientNamespaceList = new CopyOnWriteArrayList<>();
    namespaceCreator = new SimpleNamespaceCreator();
    this.server = server;

    mgBase = buildMessageGroupBase(timeoutController, aliasMap, onDisconnect);
    mgBase.enable();

    myIPAndPort = IPAddrUtil.createIPAndPort(IPAddrUtil.localIP(), mgBase.getInterfacePort());
    log.debug("Session IP:Port {}", IPAddrUtil.addrAndPortToString(myIPAndPort));

    if (!isDaemon()) {
      // Eagerly create the connection so that failures occur here, rather than after the session object is returned
      eagerConnect();
    }

    switch (nsOptionsMode) {
    case ZooKeeper:
      try {
        nsOptionsClient = new NamespaceOptionsClientZKImpl(dhtConfig);
      } catch (KeeperException ke) {
        throw new IOException("Cannot create NamespaceOptionsClientZKImpl", ke);
      }
      break;
    case MetaNamespace:
      nsOptionsClient = new NamespaceOptionsClientNSPImpl(this, dhtConfig, timeoutController);
      break;
    default:
      throw new RuntimeException("Unhandled nsOptionsMode: " + nsOptionsMode);
    }

    // Post-construction task: make sure this scheduled task is lastly called
    worker = new Worker();
    timeoutCheckTask = new SafeTimerTask(new TimeoutCheckTask());
    DHTUtil.timer().scheduleAtFixedRate(timeoutCheckTask, timeoutCheckIntervalMillis, timeoutCheckIntervalMillis);
  }

  public boolean isDaemon() {
    return DHTConstants.isDaemon;
  }

  protected void eagerConnect() throws AuthFailedException, ConnectException {
    try {
      mgBase.ensureConnected(server);
    } catch (AuthFailedException e) {
      mgBase.shutdown();
      throw e;
    } catch (Exception e) {
      mgBase.shutdown();
      String msg = "Failed to connect to " + server.toString();
      log.error("{}", msg, e);
      throw new ConnectException(msg);
    }
  }

  protected MessageGroupBase buildMessageGroupBase(SessionEstablishmentTimeoutController timeoutController,
      IPAliasMap aliasMap, SessionPolicyOnDisconnect onDisconnect) throws IOException {
    return MessageGroupBase.newClientMessageGroupBase(0, this, absMillisTimeSource,
        new NewConnectionTimeoutControllerWrapper(timeoutController), this, connectionQueueLimit,
        numSelectorControllers, selectorControllerClass, aliasMap, onDisconnect);
  }

  MessageGroupBase getMessageGroupBase() {
    return mgBase;
  }

  @Override
  public AddrAndPort getServer() {
    return server;
  }

  @Override
  public boolean isServerTraceEnabled() {
    return enableMsgGroupTrace;
  }

  public ClientDHTConfiguration getDhtConfig() {
    return dhtConfig;
  }

  @Override
  public NamespaceCreationOptions getNamespaceCreationOptions() {
    return nsOptionsClient.getNamespaceCreationOptions();
  }

  @Override
  public NamespaceOptions getDefaultNamespaceOptions() {
    return getNamespaceCreationOptions().getDefaultNamespaceOptions();
  }

  @Override
  public PutOptions getDefaultPutOptions() {
    return getDefaultNamespaceOptions().getDefaultPutOptions();
  }

  @Override
  public GetOptions getDefaultGetOptions() {
    return getDefaultNamespaceOptions().getDefaultGetOptions();
  }

  @Override
  public WaitOptions getDefaultWaitOptions() {
    return getDefaultNamespaceOptions().getDefaultWaitOptions();
  }

  private NamespaceLinkMeta getNSLinkMeta() {
    synchronized (this) {
      if (nsLinkMeta == null) {
        try {
          MetaClient mc;

          mc = new MetaClient(dhtConfig.getName(), dhtConfig.getZKConfig());
          nsLinkMeta = new NamespaceLinkMeta(new NamespaceLinksZK(mc));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      return nsLinkMeta;
    }
  }

  @Override
  public void queueAboveLimit() {
    for (ClientNamespace clientNamespace : clientNamespaceList) {
      clientNamespace.queueAboveLimit();
    }
  }

  @Override
  public void queueBelowLimit() {
    for (ClientNamespace clientNamespace : clientNamespaceList) {
      clientNamespace.queueBelowLimit();
    }
  }

  private NamespaceProperties getNamespaceProperties(String namespace) {
    if (namespace.equals(NamespaceUtil.metaInfoNamespaceName)) {
      return NamespaceUtil.metaInfoNamespaceProperties;
    } else if (namespace.startsWith(Namespace.namespaceMetricsBaseName)) {
      return DHTConstants.metricsNamespaceProperties;
    } else {
      try {
        return nsOptionsClient.getNamespacePropertiesAndTryAutoCreate(namespace);
      } catch (TimeoutException te) {
        throw new RuntimeException("Timeout retrieving namespace meta information " + Long.toHexString(
            NamespaceUtil.nameToContext(namespace)) + " " + namespace + ", session closed: " + closed, te);
      } catch (NamespacePropertiesRetrievalException re) {
        SynchronousNamespacePerspective<Long, String> syncNSP;
        String locations;
        long ns;

        ns = NamespaceUtil.nameToContext(namespace);
        syncNSP = getNamespace(Namespace.replicasName).openSyncPerspective(Long.class, String.class);
        try {
          locations = syncNSP.get(ns);
          log.info("Failed to retrieve namespace {}" , String.format("%x", ns));
          log.info("{}",locations);
        } catch (RetrievalException re2) {
          log.info("{}",re2.getDetailedFailureMessage());
          log.info("Unexpected failure attempting to find key locations  during failed ns retrieval processing");
        }
        log.info("",re);
        throw new RuntimeException(
            "Unable to retrieve namespace meta information " + Long.toHexString(ns) + ", session closed: " + closed,
            re);
      }
    }
  }

  private ClientNamespace getClientNamespace(String namespace) {
    ClientNamespace clientNamespace;
    Context context;

    log.debug("getClientNamespace {}", namespace);
    context = namespaceCreator.createNamespace(namespace);
    clientNamespace = clientNamespaces.get(context.contextAsLong());
    if (clientNamespace == null) {
      ClientNamespace previous;
      NamespaceProperties nsProperties;
      NamespaceOptions nsOptions;
      ClientNamespace parent;
      NamespaceLinkMeta nsLinkMeta;

      nsProperties = getNamespaceProperties(namespace);
      log.debug("nsProperties {}", nsProperties);
      if (nsProperties == null) {
        throw new NamespaceNotCreatedException(namespace);
      }
      nsOptions = nsProperties.getOptions();
      if (nsProperties.getParent() != null) {
        parent = getClientNamespace(nsProperties.getParent());
      } else {
        parent = null;
      }
      if (nsOptions.getAllowLinks() && nsOptions.getVersionMode() == NamespaceVersionMode.SINGLE_VERSION) {
        nsLinkMeta = getNSLinkMeta();
      } else {
        nsLinkMeta = null;
      }
      clientNamespace = new ClientNamespace(this, namespace, nsOptions, serializationRegistry, absMillisTimeSource,
          server, parent, nsLinkMeta);
      previous = clientNamespaces.putIfAbsent(context.contextAsLong(), clientNamespace);
      if (previous != null) {
        clientNamespace = previous;
      } else {
        log.debug("Created client namespace: {}   {}" , namespace ,context);
        clientNamespaceList.add(clientNamespace);
      }
    }
    return clientNamespace;
  }

  @Override
  public Namespace createNamespace(String namespace) throws NamespaceCreationException {
    return createNamespace(namespace, getNamespaceCreationOptions().getDefaultNamespaceOptions());
  }

  @Override
  public Namespace createNamespace(String namespace, NamespaceOptions nsOptions) throws NamespaceCreationException {
    if (nsOptions == null) {
      nsOptions = getNamespaceCreationOptions().getDefaultNamespaceOptions();
    }
    return createNamespace(namespace, new NamespaceProperties(nsOptions).name(namespace));
  }

  Namespace modifyNamespace(String namespace, NamespaceOptions nsOptions) throws NamespaceModificationException {
    if (nsOptions == null) {
      nsOptions = getNamespaceCreationOptions().getDefaultNamespaceOptions();
    }
    return modifyNamespace(namespace, new NamespaceProperties(nsOptions));
  }

  Namespace createNamespace(String namespace, NamespaceProperties nsProperties) throws NamespaceCreationException {
    if (Namespace.isReservedNamespace(namespace)) {
      throw new NamespaceCreationException("Reserved name: " + namespace);
    } else {
      // New version of code will always enrich nsProperties with name
      Preconditions.checkArgument(nsProperties.hasName(),
          "nsProperties is not enriched to create namespace (wrong call path or wrong Silverking version is used); " + "ns: " + nsProperties);
      nsOptionsClient.createNamespace(namespace, nsProperties);
      return getClientNamespace(namespace);
    }
  }

  Namespace modifyNamespace(String namespace, NamespaceProperties nsProperties) throws NamespaceModificationException {
    if (Namespace.isReservedNamespace(namespace)) {
      throw new NamespaceModificationException("Reserved name: " + namespace);
    } else {
      /* We let this early failure here, since for now only ZK impl supports mutability
       * (Without this check, the modification request will still fail at server side and return back here which will
       * take longer)
       */
      if (nsOptionsMode != NamespaceOptionsMode.ZooKeeper) {
        throw new NamespaceModificationException(
            "For now only NamespaceOptions ZooKeeper mode supports mutable nsOptions");
      }

      nsOptionsClient.modifyNamespace(namespace, nsProperties);
      return getClientNamespace(namespace);
    }
  }

  /**
   * Support metrics namespaces by translating the name to the hash for the ns
   */
  private String translateNamespace(String namespace) {
    if (namespace.startsWith(Namespace.namespaceMetricsBaseName)) {
      String targetNS;

      targetNS = namespace.substring(Namespace.namespaceMetricsBaseName.length());
      if (log.isDebugEnabled()) {
        log.debug("{}   {}", Namespace.namespaceMetricsBaseName ,
            namespaceCreator.createNamespace(targetNS).contextAsLong());
        log.debug("{}", namespaceCreator.createNamespace(Namespace.namespaceMetricsBaseName + String.format("%x",
            namespaceCreator.createNamespace(targetNS).contextAsLong())).contextAsLong());
      }
      return Namespace.namespaceMetricsBaseName + String.format("%x",
          namespaceCreator.createNamespace(targetNS).contextAsLong());
    } else {
      return namespace;
    }
  }

  @Override
  public Namespace getNamespace(String namespace) {
    return getClientNamespace(translateNamespace(namespace));
  }

  @Override
  public void deleteNamespace(String namespace) throws NamespaceDeletionException {
    // Placeholder for future implementation
        /*
        try {
            //GlobalCommandZK zk;
            //MetaClient      mc;
            
            //mc = new MetaClient(dhtConfig.getName(), new ZooKeeperConfig(dhtConfig.getZkLocs()));        
            //zk = new GlobalCommandZK(mc);
            // FUTURE - we don't want this here, we need another class to execute the command
            // and track its completion
        } catch (IOException | KeeperException e) {
            throw new NamespaceDeletionException(e);
        }
        */

    /* We let this early failure here, since for now only ZK impl supports deletion
     * (Without this check, the deletion request will still fail at low-level SNPImpl client)
     */
    if (nsOptionsMode != NamespaceOptionsMode.ZooKeeper) {
      throw new NamespaceDeletionException("For now only NamespaceOptions ZooKeeper mode supports namespace deletion");
    }

    /*
     * These codes might be updated in the future; For now:
     * - SNP impl will simply throw Exception since server side cannot handle such request
     * - ZK impl will work and deleteAllNamespaceProperties is sufficient as clientside actions (server can handle
     * the deletion in ZK server, since ZK impl has no dependency on properties file for bootstrap)
     */
    try {
      // sufficient for ZKImpl as clientside actions for now
      nsOptionsClient.deleteNamespace(namespace);
    } catch (NamespacePropertiesDeleteException npe) {
      throw new NamespaceDeletionException(npe);
    }
  }

  @Override
  public void recoverNamespace(String namespace) throws NamespaceRecoverException {
    throw new NamespaceRecoverException("recoverNamespace functionality is currently not available");
  }

  @Override
  public <K, V> AsynchronousNamespacePerspective<K, V> openAsyncNamespacePerspective(String namespace,
      NamespacePerspectiveOptions<K, V> nspOptions) {
    return new AsynchronousNamespacePerspectiveImpl<K, V>(getClientNamespace(namespace), namespace,
        new NamespacePerspectiveOptionsImpl<>(nspOptions, serializationRegistry));
  }

  @Override
  public <K, V> AsynchronousNamespacePerspective<K, V> openAsyncNamespacePerspective(String namespace,
      Class<K> keyClass, Class<V> valueClass) {
    ClientNamespace ns;

    ns = getClientNamespace(namespace);
    return new AsynchronousNamespacePerspectiveImpl<K, V>(ns, namespace,
        new NamespacePerspectiveOptionsImpl<>(ns.getDefaultNSPOptions(keyClass, valueClass), serializationRegistry));
  }

  @Override
  public AsynchronousNamespacePerspective<String, byte[]> openAsyncNamespacePerspective(String namespace) {
    return openAsyncNamespacePerspective(namespace, DHTConstants.defaultKeyClass, DHTConstants.defaultValueClass);
  }

  @Override
  public <K, V> SynchronousNamespacePerspective<K, V> openSyncNamespacePerspective(String namespace,
      NamespacePerspectiveOptions<K, V> nspOptions) {
    return new SynchronousNamespacePerspectiveImpl<K, V>(getClientNamespace(namespace), namespace,
        new NamespacePerspectiveOptionsImpl<>(nspOptions, serializationRegistry));
  }

  @Override
  public <K, V> SynchronousNamespacePerspective<K, V> openSyncNamespacePerspective(String namespace, Class<K> keyClass,
      Class<V> valueClass) {
    ClientNamespace ns;

    ns = getClientNamespace(namespace);
    return new SynchronousNamespacePerspectiveImpl<K, V>(ns, namespace,
        new NamespacePerspectiveOptionsImpl<>(ns.getDefaultNSPOptions(keyClass, valueClass), serializationRegistry));
  }

  @Override
  public SynchronousNamespacePerspective<String, byte[]> openSyncNamespacePerspective(String namespace) {
    return openSyncNamespacePerspective(namespace, DHTConstants.defaultKeyClass, DHTConstants.defaultValueClass);
  }

  @Override
  public void close() {
    mgBase.shutdown();
    timeoutCheckTask.cancel();
    cancelAllActiveOps();
    log.info("Cancelled all active asyncOps before shutting down.");
    closed = true;
    // FUTURE - consider additional actions
  }

  static class MessageAndConnection {
    final MessageGroup message;
    final MessageGroupConnection connection;

    MessageAndConnection(MessageGroup message, MessageGroupConnection connection) {
      this.message = message;
      this.connection = connection;
    }
  }

  class Worker extends BaseWorker<MessageAndConnection> {
    Worker() {
    }

    @Override
    public void doWork(MessageAndConnection mac) {
      _receive(mac.message, mac.connection);
    }
  }

  @Override
  public void receive(MessageGroup message, MessageGroupConnection connection) {
    _receive(message, connection);
    //worker.addWork(new MessageAndConnection(message, connection));
    // FUTURE - add intelligence about when to do work directly and when to use a worker
  }

  private void _receive(MessageGroup message, MessageGroupConnection connection) {
    ClientNamespace clientNamespace;

    log.debug("received from {}", connection);
    clientNamespace = clientNamespaces.get(message.getContext());
    if (clientNamespace != null) {
      clientNamespace.receive(message, connection);
    } else {
      log.info("No context found for: {}", message);
    }
  }

  /////////////////////

  void initializeExclusionSet() {
    try {
      ExclusionSet newExclusionSet;

      if (systemNSP == null) {
        systemNSP = getClientNamespace(Namespace.systemName).openAsyncPerspective(String.class, String.class);
      }
      newExclusionSet = getCurrentExclusionSet();
      if (newExclusionSet != null) {
        setExclusionSet(newExclusionSet);
      } else {
        log.info("initializeExclusionSet() failed to read exclusion set. Presuming empty");
        setExclusionSet(ExclusionSet.emptyExclusionSet(ExclusionSet.NO_VERSION));
      }
    } catch (Exception e) {
      log.error("initializeExclusionSet() failed",e);
    }
  }

  ExclusionSet getCurrentExclusionSet() {
    try {
      AsyncSingleValueRetrieval<String, String> retrieval;
      String exclusionSetDef;
      boolean complete;

      retrieval = systemNSP.get("exclusionSet");
      complete = retrieval.waitForCompletion(exclusionSetRetrievalTimeoutSeconds, TimeUnit.SECONDS);
      if (complete) {
        exclusionSetDef = retrieval.getValue();
      } else {
        exclusionSetDef = null;
      }
      if (exclusionSetDef != null) {
        return ExclusionSet.parse(exclusionSetDef);
      } else {
        return null;
      }

    } catch (Exception e) {
      log.error("getCurrentExclusionSet() failed",e);
      return null;
    }
  }

  boolean exclusionSetHasChanged() {
    if (getExclusionSet() == null) {
      initializeExclusionSet();
      return false;
    } else {
      ExclusionSet newExclusionSet;
      boolean exclusionSetHasChanged;

      newExclusionSet = getCurrentExclusionSet();
      if (newExclusionSet == null) {
        log.info("exclusionSetHasChanged() failed to read exclusion set. Presuming empty");
        newExclusionSet = ExclusionSet.emptyExclusionSet(ExclusionSet.NO_VERSION);
      }
      exclusionSetHasChanged = !getExclusionSet().equals(newExclusionSet);
      setExclusionSet(newExclusionSet);

      return exclusionSetHasChanged;
    }
  }

  void checkForTimeouts() {
    long curTimeMillis;
    boolean exclusionSetHasChanged;

    curTimeMillis = getAbsMillisTimeSource().absTimeMillis();
    exclusionSetHasChanged = exclusionSetHasChanged();
    List<ClientNamespace> clientNamespaceList = getClientNamespaceList();
    for (ClientNamespace clientNamespace : clientNamespaceList) {
      clientNamespace.checkForTimeouts(curTimeMillis, exclusionSetHasChanged);
    }
  }

  void cancelAllActiveOps() {
    for (ClientNamespace namespace : clientNamespaceList) {
      List<AsyncOperationImpl> asyncOps = namespace.getActiveAsyncOperations();
      log.info("trying to complete {} async operations on namespace {} with SESSION_CLOSED", asyncOps.size(),
          namespace.getName());
      for (AsyncOperationImpl op : asyncOps) {
        op.setResult(SESSION_CLOSED);
      }
    }
  }

  public class TimeoutCheckTask extends TimerTask {
    public void run() {
      try {
        checkForTimeouts();
      } catch (Exception e) {
        log.error("",e);
      }
    }
  }

  ExclusionSet getExclusionSet() {
    return exclusionSet;
  }

  void setExclusionSet(ExclusionSet exclusionSet) {
    this.exclusionSet = exclusionSet;
  }

  List<ClientNamespace> getClientNamespaceList() {
    return clientNamespaceList;
  }

  AbsMillisTimeSource getAbsMillisTimeSource() {
    return absMillisTimeSource;
  }

  void assertOpen() throws SessionClosedException {
    if (!mgBase.isRunning()) {
      cancelAllActiveOps();
      close();
      throw new SessionClosedException("session is closed");
    }
  }

  public boolean isClosed() {
    return closed;
  }
}
