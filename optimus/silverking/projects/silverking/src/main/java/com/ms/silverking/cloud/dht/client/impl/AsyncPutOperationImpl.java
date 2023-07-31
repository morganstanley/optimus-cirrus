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

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import com.ms.silverking.cloud.dht.NonExistenceResponse;
import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.client.AsyncInvalidation;
import com.ms.silverking.cloud.dht.client.AsyncPut;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.FailureCause;
import com.ms.silverking.cloud.dht.client.OperationException;
import com.ms.silverking.cloud.dht.client.OperationState;
import com.ms.silverking.cloud.dht.client.PutException;
import com.ms.silverking.cloud.dht.client.VersionProvider;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.MetaDataConstants;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.common.OpResultListener;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoMessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoPutMessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoPutMessageGroup.ValueAdditionResult;
import com.ms.silverking.cloud.dht.trace.SkTraceId;
import com.ms.silverking.compression.CodecProvider;
import com.ms.silverking.compression.Compressor;
import com.ms.silverking.util.ArrayUtil;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * An active PutOperation
 *
 * @param <K> key type
 * @param <V> value type
 */
class AsyncPutOperationImpl<K, V> extends AsyncKVOperationImpl<K, V>
    implements AsyncPut<K>, OpResultListener, ActiveKeyedOperationResultListener<OpResult>, AsyncInvalidation<K> {
  private final PutOperation<K, V> putOperation;
  private final long version;
  private final AtomicLong resolvedVersion;
  private final VersionProvider versionProvider;
  private final ConcurrentMap<DHTKey, OpResult> opResults;
  private final ActivePutListeners activePutListeners;

  private int creationCalls;

  private static Logger log = LoggerFactory.getLogger(AsyncPutOperationImpl.class);

  private final List<OperationUUID> opUUIDs; // holds references to ops to prevent GC
  private List<FragmentedPutValue> fragmentedPutValues; // hold references to prevent GC

  private static final boolean verboseToString = true;

  AsyncPutOperationImpl(PutOperation<K, V> putOperation,
                        ClientNamespace namespace,
                        NamespacePerspectiveOptionsImpl<K, V> nspoImpl,
                        long curTimeMillis,
                        byte[] originator,
                        VersionProvider versionProvider) {
    super(putOperation, namespace, nspoImpl, curTimeMillis, originator);
    this.putOperation = putOperation;
    if (putOperation.size() == 0) {
      setResult(OpResult.SUCCEEDED);
    }
    this.version = putOperation.putOptions().getVersion();
    resolvedVersion = new AtomicLong(DHTConstants.noSuchVersion);
    this.versionProvider = versionProvider;
    this.opResults = new ConcurrentHashMap();
    this.activePutListeners = namespace.getActivePutListeners();
    opUUIDs = new LinkedList();
  }

  @Override
  protected NonExistenceResponse getNonExistenceResponse() {
    return null;
  }

  @Override
  public MessageEstimate createMessageEstimate() {
    return new PutMessageEstimate();
  }

  PutOptions putOptions() {
    return putOperation.putOptions();
  }

  long getPotentiallyUnresolvedVersion() {
    return version;
  }

  long getResolvedVersion() {
    resolveVersion();
    return resolvedVersion.get();
  }

  /**
   * We delay resolving the version to improve the accuracy for fine-grained version providers.
   */
  private void resolveVersion() {
    if (resolvedVersion.get() == DHTConstants.noSuchVersion) {
      long version = putOperation.putOptions().getVersion();
      if (version == PutOptions.defaultVersion) {
        version = versionProvider.getVersion();
      }
      resolvedVersion.compareAndSet(DHTConstants.noSuchVersion, version);
    }
  }

  @Override
  public long getStoredVersion() {
    return getResolvedVersion();
  }

  @Override
  ProtoMessageGroup createProtoMG(MessageEstimate estimate) {
    return createProtoPutMG((PutMessageEstimate) estimate, DHTClient.getValueCreator().getBytes());
  }

  private ProtoPutMessageGroup<V> createProtoPutMG(PutMessageEstimate estimate) {
    return createProtoPutMG(estimate, DHTClient.getValueCreator().getBytes());
  }

  private ProtoPutMessageGroup<V> createProtoPutMG(PutMessageEstimate estimate, byte[] creator) {
    long resolvedVersion = getResolvedVersion();
    OperationUUID opUUID = activePutListeners.newOpUUIDAndMap();
    SkTraceId maybeTraceID = putOptions().getTraceIDProvider().traceID();
    return new ProtoPutMessageGroup(opUUID,
                                    context.contextAsLong(),
                                    estimate.getNumKeys(),
                                    resolvedVersion,
                                    nspoImpl.getValueSerializer(),
                                    putOperation.putOptions().version(resolvedVersion),
                                    putOperation.putOptions().getChecksumType(),
                                    originator,
                                    creator,
                                    operation.getTimeoutController().getMaxRelativeTimeoutMillis(this),
                                    nspoImpl.getNSPOptions().getEncrypterDecrypter(),
                                    maybeTraceID);
    // FUTURE - trim the above timeout according to the amount of time that has elapsed since the start
  }

  @Override
  public void addToEstimate(MessageEstimate estimate) {
    PutMessageEstimate putMessageEstimate = (PutMessageEstimate) estimate;
    putMessageEstimate.addKeys(putOperation.size());
    for (K key : getKeys()) {
      if (!getSent() || OpResult.isIncompleteOrNull(opResults.get(keyToDHTKey.get(key)))) {
        V value = putOperation.getValue(key);
        int estimatedValueSize = nspoImpl.getValueSerializer().estimateSerializedSize(value);
        putMessageEstimate.addBytes(estimatedValueSize);
      }
    }
  }

  @Override
  ProtoMessageGroup createMessagesForIncomplete(ProtoMessageGroup protoMG,
                                                List<MessageGroup> messageGroups,
                                                MessageEstimate estimate) {
    return createMessagesForIncomplete((ProtoPutMessageGroup<V>) protoMG, messageGroups, (PutMessageEstimate) estimate);
  }

  private ProtoPutMessageGroup<V> createMessagesForIncomplete(ProtoPutMessageGroup<V> protoPutMG,
                                                              List<MessageGroup> messageGroups,
                                                              PutMessageEstimate estimate) {
    // FUTURE - we would like to iterate on DHTKeys to avoid the keyToDHTKey lookup
    // except that we need to retrieve values by the user key.

    assert estimate.getNumKeys() != 0;
    creationCalls++;
    resolveVersion();

    for (K key : getKeys()) {
      if (!getSent() || OpResult.isIncompleteOrNull(opResults.get(keyToDHTKey.get(key)))) {
        DHTKey dhtKey = keyToDHTKey.get(key);
        V value = putOperation.getValue(key);
        ValueAdditionResult additionResult;
        boolean listenerInserted = activePutListeners.addListener(protoPutMG.getUUID(), dhtKey, this);

        if (listenerInserted) {
          additionResult = protoPutMG.addValue(dhtKey, value);
          if (additionResult == ValueAdditionResult.MessageGroupFull) {
            // If we couldn't add this key/value to the current ProtoPutMessageGroup, then we must
            // create a new message group. Save the current group to the list of groups before that.
            // First update the message estimate to remove keys/bytes already added.
            estimate.addKeys(-protoPutMG.currentBufferKeys());
            estimate.addBytes(-protoPutMG.currentValueBytes());
            assert estimate.getNumKeys() != 0;
            protoPutMG.addToMessageGroupList(messageGroups);
            protoPutMG = createProtoPutMG(estimate);
            listenerInserted = activePutListeners.addListener(protoPutMG.getUUID(), dhtKey, this);
            if (!listenerInserted) {
              throw new RuntimeException("Can't insert listener to new protoPutMG");
            }
            opUUIDs.add((OperationUUID) protoPutMG.getUUID()); // hold a reference to the uuid to prevent GC
            additionResult = protoPutMG.addValue(dhtKey, value);
            if (additionResult != ValueAdditionResult.Added) {
              throw new RuntimeException("Can't add to new protoPutMG");
            }
          } else if (additionResult == ValueAdditionResult.ValueNeedsFragmentation) {
            fragment(key, messageGroups);
            continue;
            // TODO (OPTIMUS-0000): call continue? how to handle the opUUIDs.add() below?
          }
        } else {
          // The existing protoPutMG already had an entry for the given key.
          // Create a new protoPutMG so that we can add this key.
          estimate.addKeys(-protoPutMG.currentBufferKeys());
          estimate.addBytes(-protoPutMG.currentValueBytes());
          assert estimate.getNumKeys() != 0;
          protoPutMG.addToMessageGroupList(messageGroups);
          protoPutMG = createProtoPutMG(estimate, DHTClient.getValueCreator().getBytes());
          opUUIDs.add((OperationUUID) protoPutMG.getUUID()); // hold a reference to the uuid to prevent GC
          listenerInserted = activePutListeners.addListener(protoPutMG.getUUID(), dhtKey, this);
          if (!listenerInserted) {
            throw new RuntimeException("Can't insert listener to new protoPutMG");
          }
          additionResult = protoPutMG.addValue(dhtKey, value);
          if (additionResult != ValueAdditionResult.Added) {
            throw new RuntimeException("Can't add to new protoPutMG");
          }
        }
        opUUIDs.add((OperationUUID) protoPutMG.getUUID()); // hold a reference to the uuid to prevent GC
      }
    }
    setSent();
    // Return the current protoPutMG so that subsequent operations can use it. Some
    // subsequent operation or the sender will add it to the list of message groups.

    return protoPutMG;
  }

  private void fragment(K key, List<MessageGroup> messageGroups) {
    log.debug("fragmenting: {}", key);

    // Serialize the value and compress if needed
    ByteBuffer buf = nspoImpl.getValueSerializer().serializeToBuffer(putOperation.getValue(key));
    int uncompressedLength = buf.limit();
    Compression compression = putOperation.putOptions().getCompression();
    if (compression != Compression.NONE) {
      Compressor compressor = CodecProvider.getCompressor(compression);
      try {
        byte[] compressedValue = compressor.compress(buf.array(), buf.position(), buf.remaining());
        buf = ByteBuffer.wrap(compressedValue);
      } catch (IOException ioe) {
        throw new RuntimeException("Compression error in segmentation", ioe);
      }
    }
    int storedLength = buf.limit();

    // Checksum the value
    // For segmented values we do not compute a complete checksum, but
    // instead we use the piecewise checksums. The primary reason for this is to allow for the
    // standard corrupt value detection/correction code to work for segmented values.
    byte[] checksum = new byte[putOperation.putOptions().getChecksumType().length()];

    int fragmentationThreshold = putOperation.putOptions().getFragmentationThreshold();

    // Now segment the value
    int valueSize = buf.limit();
    int numFragments = SegmentationUtil.getNumSegments(valueSize, fragmentationThreshold);
    fragmentsCreated += numFragments;
    ByteBuffer[] subBufs = new ByteBuffer[numFragments];
    for (int i = 0; i < numFragments; i++) {
      int fragmentStart = i * fragmentationThreshold;
      int fragmentSize = Math.min(fragmentationThreshold, valueSize - fragmentStart);
      buf.position(fragmentStart);
      ByteBuffer subBuf = buf.slice();
      subBuf.limit(fragmentSize);
      subBufs[i] = subBuf;
      if (debugFragmentation) {
        log.info("{} {} {}", fragmentStart, fragmentSize, subBufs[i]);
      }
    }

    DHTKey dhtKey = keyCreator.createKey(key);
    DHTKey[] subKeys = keyCreator.createSubKeys(dhtKey, numFragments);

    if (fragmentedPutValues == null) {
      fragmentedPutValues = new LinkedList();
    }
    FragmentedPutValue fragmentedPutValue = new FragmentedPutValue(subKeys, dhtKey, this);
    fragmentedPutValues.add(fragmentedPutValue);

    ProtoPutMessageGroup<V> protoPutMG;
    boolean listenerInserted;
    // NEED TO ALLOW FOR MULTIPLE PROTOPUTMG SINCE SEGMENT WILL LIKELY
    // BE A SINGLE MESSAGE OR LARGE PORTION

    // Create the message groups and add them to the list
    // For now, assume only one segment per message
    for (int i = 0; i < numFragments; i++) {
      protoPutMG = createProtoPutMG(new PutMessageEstimate(1, subBufs[i].limit()));
      opUUIDs.add((OperationUUID) protoPutMG.getUUID()); // hold a reference to the uuid to prevent GC
      listenerInserted = activePutListeners.addListener(protoPutMG.getUUID(), subKeys[i], fragmentedPutValue);
      if (!listenerInserted) {
        throw new RuntimeException("Panic: Unable to insert listener into dedicated segment protoPutMG");
      }
      if (debugFragmentation) {
        log.info("segmentation listener: {} {} {}", protoPutMG.getUUID(), subKeys[i], subBufs[i]);
      }
      protoPutMG.addValueDedicated(subKeys[i], subBufs[i]);
      protoPutMG.addToMessageGroupList(messageGroups);
      byte[] segmentChecksum = new byte[putOperation.putOptions().getChecksumType().length()];
      protoPutMG.getMostRecentChecksum(segmentChecksum);
      ArrayUtil.xor(checksum, segmentChecksum);
    }

    // Now add the index key/value
    // indicate segmentation by storing segmentationBytes in the creator field
    protoPutMG = createProtoPutMG(new PutMessageEstimate(1, SegmentationUtil.segmentedValueBufferLength),
                                  MetaDataConstants.segmentationBytes);
    opUUIDs.add((OperationUUID) protoPutMG.getUUID()); // hold a reference to the uuid to prevent GC
    listenerInserted = activePutListeners.addListener(protoPutMG.getUUID(), dhtKey, fragmentedPutValue);
    if (!listenerInserted) {
      throw new RuntimeException("Panic: Unable to add index key/value into dedicated protoPutMG");
    }
    ByteBuffer segmentMetaDataBuffer = SegmentationUtil.createSegmentMetaDataBuffer(DHTClient.getValueCreator()
                                                                                             .getBytes(),
                                                                                    storedLength,
                                                                                    uncompressedLength,
                                                                                    putOperation.putOptions()
                                                                                                .getFragmentationThreshold(),
                                                                                    putOperation.putOptions()
                                                                                                .getChecksumType(),
                                                                                    checksum);
    protoPutMG.addValueDedicated(dhtKey, segmentMetaDataBuffer);
    protoPutMG.addToMessageGroupList(messageGroups);
  }

  @Override
  public OpResult getOpResult(K key) {
    OpResult opResult = opResults.get(keyToDHTKey.get(key));
    if (opResult == null) {
      return OpResult.INCOMPLETE;
    } else {
      return opResult;
    }
  }

  @Override
  public void resultUpdated(DHTKey key, OpResult opResult) {
    log.debug("resultUpdated {}", key);
    OpResult previous = opResults.putIfAbsent(key, opResult);
    if (previous != null && previous != opResult) {
      switch (previous.toOperationState()) {
        case INCOMPLETE:
          break; // no action necessary
        case FAILED:
          opResults.put(key, previous);
          if (opResult.toOperationState() == OperationState.FAILED) {
            log.debug("Multiple failures: {}", key);
          } else if (opResult.toOperationState() == OperationState.SUCCEEDED) {
            log.info("ActivePutOperationImpl received failure then success for: {}", key);
            log.info("{}", previous);
          }
          break;
        case SUCCEEDED:
          if (opResult.toOperationState() == OperationState.FAILED) {
            log.info("ActivePutOperationImpl received success then failure for: {}", key);
            log.info("{}", opResult);
          }
          break;
        default:
          throw new RuntimeException("panic");
      }
    } else {
      if (opResult.hasFailed()) {
        setFailureCause(key, opResult.toFailureCause(getNonExistenceResponse()));
      }
    }
    if (resultsReceived.incrementAndGet() >= putOperation.size()) {
      checkForCompletion();
    }
  }

  @Override
  public OperationState getOperationState(K key) {
    OpResult result = opResults.get(keyToDHTKey.get(key));
    return result == null
           ? OperationState.INCOMPLETE
           : result.toOperationState();
  }

  @Override
  protected void throwFailedException() throws OperationException {
    throw new PutExceptionImpl((Map<Object, OperationState>) getOperationStateMap(),
                               (Map<Object, FailureCause>) getFailureCauses());
  }

  @Override
  public void waitForCompletion() throws PutException {
    try {
      _waitForCompletion();
    } catch (OperationException operationException) {
      throw (PutException) operationException;
    }
  }

  @Override
  public void resultReceived(DHTKey key, OpResult result) {
    resultUpdated(key, result);
  }

  protected void debugTimeout() {
    for (OperationUUID opUUID : opUUIDs) {
      log.info("opUUID: {}", opUUID);
      ConcurrentMap<DHTKey, WeakReference<ActiveKeyedOperationResultListener<OpResult>>> keyMap;
      keyMap = activePutListeners.getKeyMap(opUUID);
      for (DHTKey key : keyMap.keySet()) {
        if (keyMap.get(key).get() == this) {
          OpResult result = opResults.get(key);
          if (result == null || !result.isComplete()) {
            log.info("IncompleteA: {} {}", opUUID, key);
          }
        } else {
          log.info("{} -> {}", key, keyMap.get(key));
        }
      }
    }

    for (DHTKey key : dhtKeys) {
      OpResult result = opResults.get(key);
      if (result == null || !result.isComplete()) {
        log.info("IncompleteB: {} {}", key, creationCalls);
      }
    }
  }

  @Override
  public boolean canBeGroupedWith(AsyncOperationImpl asyncOperationImpl) {
    if (asyncOperationImpl instanceof AsyncPutOperationImpl) {
      AsyncPutOperationImpl other = (AsyncPutOperationImpl) asyncOperationImpl;
      return getResolvedVersion() == other.getResolvedVersion() &&
             putOperation.putOptions().equals(other.putOperation.putOptions());
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    if (verboseToString) {
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append(super.toString());
      stringBuilder.append('\n');
      for (K key : putOperation.getKeys()) {
        stringBuilder.append(String.format("%s\t%s\t%s\n", key, keyToDHTKey.get(key), getOperationState(key)));
      }
      return stringBuilder.toString();
    } else {
      return super.toString();
    }
  }
}
