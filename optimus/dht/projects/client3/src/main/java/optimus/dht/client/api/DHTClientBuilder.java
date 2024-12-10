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
 
package optimus.dht.client.api;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.Supplier;

import javax.annotation.Nullable;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import optimus.dht.client.api.servers.ServerRingPlacement;
import optimus.dht.client.api.transport.SslSettings;
import optimus.dht.client.api.transport.IdleConnectionStrategy;
import optimus.dht.client.internal.builder.ModulesAwareModule;
import optimus.dht.client.internal.coreprotocol.CoreProtocolModuleSPI;
import optimus.dht.common.internal.transport.ChannelHandlerFactory;
import optimus.dht.common.internal.transport.SslTransportConfig;
import optimus.dht.client.api.callback.CallbackRegistry;
import optimus.dht.client.api.callback.TimeoutsConfig;
import optimus.dht.client.api.hash.HashCalculator;
import optimus.dht.client.api.module.ClientModuleSPI;
import optimus.dht.client.api.registry.RegistryNodeChangesListener;
import optimus.dht.client.api.registry.RegistryObserver;
import optimus.dht.client.api.registry.ZKRegistryConfig;
import optimus.dht.client.api.servers.ServerConnectionsManager;
import optimus.dht.client.api.transport.ConnectionsManager;
import optimus.dht.client.api.transport.RawServerAccess;
import optimus.dht.client.api.transport.RequestIdGenerator;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Stage;
import com.google.inject.assistedinject.FactoryModuleBuilder;

import optimus.dht.client.api.qualifiers.Scheduled;
import optimus.dht.client.api.replication.ReplicationStrategy;
import optimus.dht.client.api.replication.SimpleReplicationStrategy;
import optimus.dht.client.internal.DefaultClientIdentity;
import optimus.dht.client.internal.DefaultDHTClient;
import optimus.dht.client.internal.callback.InternalCallbackRegistry;
import optimus.dht.client.internal.hash.DefaultHashCalculator;
import optimus.dht.client.internal.kv.KVModuleSPI;
import optimus.dht.client.internal.module.ClientModulesRegistry;
import optimus.dht.client.internal.module.ClientModulesSupport;
import optimus.dht.client.internal.registry.ZookeeperRegistryObserver;
import optimus.dht.client.internal.servers.DefaultServerConnectionsManager;
import optimus.dht.client.internal.servers.ServerConnectionsManagerConfig;
import optimus.dht.client.internal.transport.ClientTransportConfig;
import optimus.dht.client.internal.transport.DefaultConnectionsManager;
import optimus.dht.client.internal.transport.DefaultRawServerAccess;
import optimus.dht.client.internal.transport.MonotonicRequestIdGenerator;
import optimus.dht.common.api.lifecycle.ExternallyProvidedScheduledExecutorService;
import optimus.dht.common.internal.lifecycle.ShutdownAwareListenerModule;
import optimus.dht.common.util.registry.ZKUtils;
import optimus.dht.common.util.registry.ZkConfig;

public class DHTClientBuilder {

  private static final Logger logger = LoggerFactory.getLogger(DHTClientBuilder.class);

  /** Holder for settings. 
  protected class Setting<T> {
    /** Property name 
    final String name;
    /** Property value (if set) 
    T value;
    /** Optional property default value supplier 
    final Supplier<T> defaultValueSupplier;
    /** Cached default value 
    T defaultValue;
    /** List of properties that conflict with this setting 
    final List<Setting<?>> conflictsWith;

    public Setting(String name, Supplier<T> defaultValueSupplier, Setting<?>... conflictsWith) {
      this.name = name;
      this.defaultValueSupplier = defaultValueSupplier;
      this.conflictsWith = Arrays.asList(conflictsWith);
    }

    public Setting(String name, T defaultValue, Setting<?>... conflictsWith) {
      this.name = name;
      this.defaultValueSupplier = null;
      this.defaultValue = defaultValue;
      this.conflictsWith = Arrays.asList(conflictsWith);
    }

    public Setting(String name, Setting<?>... conflictsWith) {
      this(name, (Supplier<T>) null, conflictsWith);
    }

    public void set(T value) {
      this.value = value;
    }

    protected void verifyConflicting(String propertyName) {
      Preconditions.checkArgument(
          !isSet(), "Setting [" + name + "] cannot be set together with [" + propertyName + "]");
      for (Setting<?> conflictingSetting : conflictsWith) {
        conflictingSetting.verifyConflicting(propertyName);
      }
    }

    public T get() {
      if (value != null) {
        for (Setting<?> conflictingSetting : conflictsWith) {
          conflictingSetting.verifyConflicting(name);
        }
      } else {
        Preconditions.checkArgument(
            defaultValue != null || defaultValueSupplier != null,
            "Property [" + name + "] must be set");
        if (defaultValue == null) {
          defaultValue = defaultValueSupplier.get();
          Preconditions.checkNotNull(
              defaultValue, "Default value supplier for property [" + name + "] returned null");
        }
        logger.info(
            "Value for property {} not explicitly set - using default value: {}",
            name,
            defaultValue);
        value = defaultValue;
      }
      return value;
    }

    public boolean isSet() {
      return value != null;
    }
  }

  /**
   * Creates a new instance of a builder.
   *
   * @return a new instance of a builder
   
  public static DHTClientBuilder create() {
    return new DHTClientBuilder();
  }

  protected final Setting<RegistryObserver> registryObserverSetting =
      new Setting<>("registryObserver");

  protected final Setting<CuratorFramework> curatorFrameworkSetting =
      new Setting<>("curatorFramework");

  protected final Setting<String> zkPathSetting = new Setting<>("zkPath");
  protected final Setting<Duration> zkConnectTimeoutSetting =
      new Setting<>("zkConnectTimeout", Duration.ofSeconds(60));
  protected final Setting<Duration> zkSessionEstablishmentTimeoutSetting =
      new Setting<>("zkSessionEstablishmentTimeout", Duration.ofSeconds(60));
  protected final Setting<Duration> zkRetryDelaySetting =
      new Setting<>("zkRetryDelay", Duration.ofSeconds(60));
  protected final Setting<Boolean> zkKerberosSetting = new Setting<>("zkKerberos", true);

  protected final Setting<RegistryNodeChangesListener> registryNodeChangesListenerSetting =
      new Setting<>("registryNodeChangesListener");

  protected final Setting<Boolean> kerberosSetting = new Setting<>("kerberos", true);
  protected final Setting<Integer> kerberosThreadsSetting = new Setting<>("kerberosThreads", 8);
  protected final Setting<SslSettings> sslSettingsSetting = new Setting<>("sslSettings");
  protected final Setting<Duration> connectTimeoutSetting =
      new Setting<>("connectTimeout", Duration.ofSeconds(30));
  protected final Setting<Duration> readTimeoutSetting =
      new Setting<>("readTimeout", Duration.ofSeconds(30));
  protected final Setting<Integer> ioThreadsSetting = new Setting<>("ioThreads", 4);

  protected final Setting<Integer> sendBufferSetting = new Setting<>("sendBuffer", 32768);
  protected final Setting<Integer> writeLowWaterMarkSetting =
      new Setting<>("writeLowWaterMark", () -> 3 * sendBufferSetting.get());
  protected final Setting<Integer> writeHighWaterMarkSetting =
      new Setting<>("writeHighWaterMark", () -> 5 * sendBufferSetting.get());
  protected final Setting<Integer> readMinBufferSizeSetting =
      new Setting<>("readMinBufferSize", 1024);
  protected final Setting<Integer> readMaxBufferSizeSetting =
      new Setting<>("readMaxBufferSize", 32768);
  protected final Setting<Boolean> asyncDnsSetting = new Setting<>("asyncDns", false);
  protected final Setting<IdleConnectionStrategy> idleConnectionStrategySetting =
      new Setting<>("idleConnectionStrategy", IdleConnectionStrategy.NOOP);

  protected final Setting<Duration> defaultOperationTimeoutSetting =
      new Setting<>("defaultOperationTimeout", Duration.ofSeconds(60));
  protected final Setting<ReplicationStrategy> replicationStrategySetting =
      new Setting<>("replicationStrategy", () -> new SimpleReplicationStrategy(1));

  protected final Setting<Boolean> eagerDnsResolutionSetting =
      new Setting<>("eagerDnsResolution", true);

  protected final Setting<Integer> ringSlotsPerServerSetting =
      new Setting<>("ringSlotsPerServer", 200);

  protected final Setting<ServerRingPlacement> serverRingPlacementSetting =
      new Setting<>("serverRingPlacement", ServerRingPlacement.STABLE);

  protected final Setting<Duration> orphanedNodeTimeoutSetting =
      new Setting<>("orphanedNodeTimeout", Duration.ofSeconds(60));
  protected final Setting<Duration> initialConnectionRetryIntervalSetting =
      new Setting<>("initialConnectionRetryInterval", Duration.ofSeconds(1));
  protected final Setting<Duration> maxConnectionRetryIntervalSetting =
      new Setting<>("maxConnectionRetryInterval", Duration.ofSeconds(30));
  protected final Setting<Float> connectionRetryIntervalExpRatioSetting =
      new Setting<>("connectionRetryIntervalExpRatio", 2.0f);

  protected final Setting<Boolean> omitKVModuleSetting =
      new Setting<>("omitKVModuleSetting", false);

  protected final Setting<ScheduledExecutorService> scheduledExecutorServiceSetting =
      new Setting<>("scheduledExecutorService");

  protected final List<ClientModuleSPI<?>> modulesList = new ArrayList<>();

  protected DHTClientBuilder() {}

  public DHTClientBuilder registryObserver(RegistryObserver registryObserver) {
    registryObserverSetting.set(registryObserver);
    return this;
  }

  public DHTClientBuilder curatorFramework(CuratorFramework curatorFramework) {
    curatorFrameworkSetting.set(curatorFramework);
    return this;
  }

  public DHTClientBuilder zkPath(String zkPath) {
    zkPathSetting.set(zkPath);
    return this;
  }

  public DHTClientBuilder zkConnectTimeout(Duration zkConnectTimeout) {
    zkConnectTimeoutSetting.set(zkConnectTimeout);
    return this;
  }

  public DHTClientBuilder zkSessionEstablishmentTimeout(Duration zkSessionEstablishmentTimeout) {
    zkSessionEstablishmentTimeoutSetting.set(zkSessionEstablishmentTimeout);
    return this;
  }

  public DHTClientBuilder zkRetryDelay(Duration zkRetryDelay) {
    zkRetryDelaySetting.set(zkRetryDelay);
    return this;
  }

  public DHTClientBuilder registryNodeChangesListener(
      RegistryNodeChangesListener registryNodeChangesListener) {
    registryNodeChangesListenerSetting.set(registryNodeChangesListener);
    return this;
  }

  public DHTClientBuilder zkKerberos(boolean zkKerberos) {
    zkKerberosSetting.set(zkKerberos);
    return this;
  }

  public DHTClientBuilder kerberos(boolean kerberos) {
    kerberosSetting.set(kerberos);
    return this;
  }

  public DHTClientBuilder kerberosThreads(int kerberosThreads) {
    kerberosThreadsSetting.set(kerberosThreads);
    return this;
  }

  public DHTClientBuilder sslSettings(SslSettings sslSettings) {
    sslSettingsSetting.set(sslSettings);
    return this;
  }

  public DHTClientBuilder connectTimeout(Duration connectTimeout) {
    connectTimeoutSetting.set(connectTimeout);
    return this;
  }

  public DHTClientBuilder readTimeout(Duration readTimeout) {
    readTimeoutSetting.set(readTimeout);
    return this;
  }

  public DHTClientBuilder ioThreads(int ioThreads) {
    ioThreadsSetting.set(ioThreads);
    return this;
  }

  public DHTClientBuilder sendBuffer(int sendBuffer) {
    sendBufferSetting.set(sendBuffer);
    return this;
  }

  public DHTClientBuilder writeLowWaterMark(int writeLowWaterMark) {
    writeLowWaterMarkSetting.set(writeLowWaterMark);
    return this;
  }

  public DHTClientBuilder writeHighWaterMark(int writeHighWaterMark) {
    writeHighWaterMarkSetting.set(writeHighWaterMark);
    return this;
  }

  public DHTClientBuilder readMinBufferSize(int readMinBufferSize) {
    readMinBufferSizeSetting.set(readMinBufferSize);
    return this;
  }

  public DHTClientBuilder readMaxBufferSize(int readMaxBufferSize) {
    readMaxBufferSizeSetting.set(readMaxBufferSize);
    return this;
  }

  public DHTClientBuilder asyncDns(boolean asyncDns) {
    asyncDnsSetting.set(asyncDns);
    return this;
  }

  public DHTClientBuilder idleConnectionStrategy(IdleConnectionStrategy idleConnectionStrategy) {
    idleConnectionStrategySetting.set(idleConnectionStrategy);
    return this;
  }

  public DHTClientBuilder defaultOperationTimeout(Duration defaultOperationTimeout) {
    defaultOperationTimeoutSetting.set(defaultOperationTimeout);
    return this;
  }

  public DHTClientBuilder replicationStrategy(ReplicationStrategy replicationStrategy) {
    replicationStrategySetting.set(replicationStrategy);
    return this;
  }

  public DHTClientBuilder eagerDnsResolution(boolean eagerDnsResolution) {
    eagerDnsResolutionSetting.set(eagerDnsResolution);
    return this;
  }

  public DHTClientBuilder ringSlotsPerNode(int ringSlotsPerNode) {
    ringSlotsPerServerSetting.set(ringSlotsPerNode);
    return this;
  }

  public DHTClientBuilder serverRingPlacement(ServerRingPlacement serverRingPlacement) {
    serverRingPlacementSetting.set(serverRingPlacement);
    return this;
  }

  public DHTClientBuilder orphanedNodeTimeout(Duration orphanedNodeTimeout) {
    orphanedNodeTimeoutSetting.set(orphanedNodeTimeout);
    return this;
  }

  public DHTClientBuilder initialConnectionRetryInterval(Duration initialConnectionRetryInterval) {
    initialConnectionRetryIntervalSetting.set(initialConnectionRetryInterval);
    return this;
  }

  public DHTClientBuilder maxConnectionRetryInterval(Duration maxConnectionRetryInterval) {
    maxConnectionRetryIntervalSetting.set(maxConnectionRetryInterval);
    return this;
  }

  public DHTClientBuilder connectionRetryIntervalExpRatio(float connectionRetryIntervalExpRatio) {
    connectionRetryIntervalExpRatioSetting.set(connectionRetryIntervalExpRatio);
    return this;
  }

  public DHTClientBuilder omitKVModule(boolean omitKVModule) {
    omitKVModuleSetting.set(omitKVModule);
    return this;
  }

  public DHTClientBuilder scheduledExecutorService(
      ScheduledExecutorService scheduledExecutorService) {
    scheduledExecutorServiceSetting.set(scheduledExecutorService);
    return this;
  }

  public DHTClientBuilder registerModule(ClientModuleSPI<?> moduleSpi) {
    modulesList.add(moduleSpi);
    return this;
  }

  public DHTClient build() {
    CoreModule coreModule = new CoreModule();
    ModulesAwareModule modulesAwareModule = new ModulesAwareModule();
    Injector coreInjector =
        Guice.createInjector(
            Stage.PRODUCTION,
            coreModule,
            modulesAwareModule,
            new RegistryObserverModule(),
            new ShutdownAwareListenerModule());
    DHTClient dhtClient = coreInjector.getInstance(DHTClient.class);

    // Not all servers support core protocol yet - only enable it if we really need it
    if (idleConnectionStrategySetting.get().actionType()
        != IdleConnectionStrategy.ActionType.NOOP) {
      modulesList.add(new CoreProtocolModuleSPI());
    }

    if (!omitKVModuleSetting.get()) {
      modulesList.add(new KVModuleSPI());
    }

    ClientModulesRegistry clientModulesRegistry =
        ClientModulesSupport.createRegistry(coreInjector, modulesList);
    modulesAwareModule.init(clientModulesRegistry);

    return dhtClient;
  }

  protected class CoreModule extends AbstractModule {

    @Override
    protected void configure() {
      // we want to know about cycles
      binder().disableCircularProxies();

      bind(ServerConnectionsManager.class).to(DefaultServerConnectionsManager.class);
      bind(RegistryNodeChangesListener.class).to(DefaultServerConnectionsManager.class);
      bind(HashCalculator.class).to(DefaultHashCalculator.class);
      bind(RequestIdGenerator.class).to(MonotonicRequestIdGenerator.class);
      bind(CallbackRegistry.class).to(InternalCallbackRegistry.class);
      bind(ClientIdentity.class).to(DefaultClientIdentity.class);
      bind(RawServerAccess.class).to(DefaultRawServerAccess.class);
      bind(DHTClient.class).to(DefaultDHTClient.class);
      bind(ChannelHandlerFactory.class)
          .toInstance(ServiceLoader.load(ChannelHandlerFactory.class).findFirst().get());

      install(
          new FactoryModuleBuilder()
              .implement(ConnectionsManager.class, DefaultConnectionsManager.class)
              .build(ConnectionsManager.Factory.class));
    }

    @Provides
    @Singleton
    protected ReplicationStrategy replicationStrategy() {
      return replicationStrategySetting.get();
    }

    @Provides
    @Singleton
    protected ServerRingPlacement serverRingPlacement() {
      return serverRingPlacementSetting.get();
    }

    @Provides
    @Singleton
    @Scheduled
    protected ScheduledExecutorService scheduledExecutorService(ClientIdentity clientIdentity) {
      if (scheduledExecutorServiceSetting.isSet()) {
        return new ExternallyProvidedScheduledExecutorService(
            scheduledExecutorServiceSetting.get());
      } else {
        ScheduledThreadPoolExecutor scheduledThreadPoolExecutor =
            new ScheduledThreadPoolExecutor(
                2,
                new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("dht-client-scheduled-" + clientIdentity.uniqueId() + "-%d")
                    .build());
        scheduledThreadPoolExecutor.setRemoveOnCancelPolicy(true);
        return scheduledThreadPoolExecutor;
      }
    }

    @Provides
    @Singleton
    protected ServerConnectionsManagerConfig serverNodesManagerConfig() {
      return new ServerConnectionsManagerConfig(
          ringSlotsPerServerSetting.get(),
          orphanedNodeTimeoutSetting.get(),
          initialConnectionRetryIntervalSetting.get(),
          maxConnectionRetryIntervalSetting.get(),
          connectionRetryIntervalExpRatioSetting.get(),
          eagerDnsResolutionSetting.get());
    }

    @Provides
    @Singleton
    @Named("kerberos")
    protected Executor kerberosExecutor(ClientIdentity clientIdentity) {
      if (kerberosSetting.get()) {
        return Executors.newFixedThreadPool(
            kerberosThreadsSetting.get(),
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("dht-client-kerberos-" + clientIdentity.uniqueId() + "-%d")
                .build());
      } else {
        return null;
      }
    }

    @Provides
    @Singleton
    protected SslTransportConfig sslTransportConfig() {
      if (!sslSettingsSetting.isSet()) {
        return null;
      }
      try {
        SslSettings sslSettings = sslSettingsSetting.get();
        KeyManagerFactory keyManagerFactory;
        if (sslSettings.keyManagerFactory() != null) {
          keyManagerFactory = sslSettings.keyManagerFactory();
        } else {
          String keystoreFile = sslSettings.keystoreFile();
          String keystoreFormat = sslSettings.keystoreFormat().name();
          KeyStore keyStore = KeyStore.getInstance(keystoreFormat);
          try (FileInputStream fis = new FileInputStream(keystoreFile)) {
            keyStore.load(fis, sslSettings.keystorePass().toCharArray());
          }
          keyManagerFactory =
              KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
          keyManagerFactory.init(keyStore, sslSettings.keyPass().toCharArray());
          logger.info(
              "Loaded SSL/TLS keystore from {} using format {}", keystoreFile, keystoreFormat);
        }

        TrustManagerFactory trustManagerFactory;
        if (sslSettings.trustManagerFactory() != null) {
          trustManagerFactory = sslSettings.trustManagerFactory();
        } else if (sslSettings.truststoreFile() != null) {
          String truststoreFile = sslSettings.truststoreFile();
          String truststoreFormat = sslSettings.truststoreFormat().name();
          KeyStore trustStore = KeyStore.getInstance(truststoreFormat);
          try (FileInputStream fis = new FileInputStream(truststoreFile)) {
            trustStore.load(fis, sslSettings.truststorePass().toCharArray());
          }
          trustManagerFactory =
              TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
          trustManagerFactory.init(trustStore);
          logger.info(
              "Loaded SSL/TLS truststore from {} using format {}",
              truststoreFile,
              truststoreFormat);
        } else {
          trustManagerFactory = null;
        }

        return new SslTransportConfig(
            keyManagerFactory,
            trustManagerFactory,
            sslSettings.runtimeImplementation(),
            sslSettings.handshakeTimeout());

      } catch (Exception e) {
        throw new RuntimeException("Unable to load SSL/TLS configuration", e);
      }
    }

    @Provides
    @Singleton
    protected ClientTransportConfig clientTransportConfig(
        @Nullable @Named("kerberos") Executor kerberosExecutor,
        @Nullable SslTransportConfig sslTransportConfig) {

      if (kerberosExecutor != null && sslTransportConfig != null) {
        throw new IllegalArgumentException(
            "Kerberos and SSL/TLS cannot be enabled at the same time");
      }

      return new ClientTransportConfig(
          kerberosExecutor,
          sslTransportConfig,
          ioThreadsSetting.get(),
          connectTimeoutSetting.get(),
          sendBufferSetting.get(),
          writeLowWaterMarkSetting.get(),
          writeHighWaterMarkSetting.get(),
          readMinBufferSizeSetting.get(),
          readMaxBufferSizeSetting.get(),
          asyncDnsSetting.get(),
          idleConnectionStrategySetting.get());
    }

    @Provides
    @Singleton
    protected ConnectionsManager connectionsManager(DefaultServerConnectionsManager nodesManager) {
      return nodesManager.connectionsManager();
    }

    @Provides
    @Singleton
    protected TimeoutsConfig timeoutsConfig() {
      Duration effectiveOperationTimeout = defaultOperationTimeoutSetting.get();
      Duration readTimeout = readTimeoutSetting.get();
      if (effectiveOperationTimeout.compareTo(readTimeout) < 0) {
        logger.warn(
            "Default operation timeout[{}] is lower than read timeout[{}] - increasing it to match",
            effectiveOperationTimeout,
            readTimeout);
        effectiveOperationTimeout = readTimeout;
      }
      return new TimeoutsConfig(readTimeout, effectiveOperationTimeout);
    }
  }

  protected class RegistryObserverModule extends AbstractModule {
    @Override
    protected void configure() {
      if (registryObserverSetting.isSet()) {
        install(new ProvidedRegistryObserverModule(registryObserverSetting.get()));
      } else {
        install(new ZKRegistryObserverModule());
      }
    }
  }

  protected class ZKRegistryObserverModule extends AbstractModule {

    @Provides
    @Singleton
    protected RegistryObserver registryObserver(CuratorFramework curator, ZKRegistryConfig config) {
      ZookeeperRegistryObserver nodesObserver = new ZookeeperRegistryObserver(curator, config);
      if (registryNodeChangesListenerSetting.isSet()) {
        nodesObserver.registerListener(registryNodeChangesListenerSetting.get());
      }
      nodesObserver.start();
      return nodesObserver;
    }

    @Provides
    @Singleton
    protected ZkConfig zkConfig() {
      return ZkConfig.parse(zkPathSetting.get(), zkKerberosSetting.get());
    }

    @Provides
    @Singleton
    protected CuratorFramework curatorFramework(ZkConfig zkConfig) {
      CuratorFramework curatorFramework;
      if (!curatorFrameworkSetting.isSet()) {
        curatorFramework =
            ZKUtils.getFrameworkBuilder(zkConfig)
                .connectionTimeoutMs((int) zkConnectTimeoutSetting.get().toMillis())
                .sessionTimeoutMs((int) zkSessionEstablishmentTimeoutSetting.get().toMillis())
                .build();
      } else {
        curatorFramework = curatorFrameworkSetting.get();
      }
      return curatorFramework;
    }

    @Provides
    @Singleton()
    protected ZKRegistryConfig zkRegistryConfig(
        @Scheduled ScheduledExecutorService scheduledExecutorService, ZkConfig zkConfig) {
      return new ZKRegistryConfig(
          zkConfig.getBasePath(), zkRetryDelaySetting.get(), scheduledExecutorService);
    }
  }

  protected class ProvidedRegistryObserverModule extends AbstractModule {

    private final RegistryObserver registryObserver;

    public ProvidedRegistryObserverModule(RegistryObserver registryObserver) {
      this.registryObserver = registryObserver;
    }

    @Provides
    @Singleton
    protected RegistryObserver registryObserver() {
      return registryObserver;
    }
  }
}
*/
