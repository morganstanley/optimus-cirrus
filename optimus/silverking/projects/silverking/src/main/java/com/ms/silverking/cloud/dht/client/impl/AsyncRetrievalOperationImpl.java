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

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

import com.ms.silverking.cloud.dht.AllReplicasExcludedResponse;
import com.ms.silverking.cloud.dht.NonExistenceResponse;
import com.ms.silverking.cloud.dht.RetrievalOptions;
import com.ms.silverking.cloud.dht.TimeoutResponse;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.WaitMode;
import com.ms.silverking.cloud.dht.WaitOptions;
import com.ms.silverking.cloud.dht.client.AsyncSingleValueRetrieval;
import com.ms.silverking.cloud.dht.client.FailureCause;
import com.ms.silverking.cloud.dht.client.MetaData;
import com.ms.silverking.cloud.dht.client.OperationException;
import com.ms.silverking.cloud.dht.client.OperationState;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.StoredValue;
import com.ms.silverking.cloud.dht.common.CorruptValueException;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.InternalRetrievalOptions;
import com.ms.silverking.cloud.dht.common.MetaDataUtil;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.common.RawRetrievalResult;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.MessageGroupRetrievalResponseEntry;
import com.ms.silverking.cloud.dht.net.ProtoMessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoRetrievalMessageGroup;
import com.ms.silverking.cloud.dht.trace.SkTraceId;
import com.ms.silverking.collection.ConcurrentSingleMap;
import com.ms.silverking.text.StringUtil;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class AsyncRetrievalOperationImpl<K, V> extends AsyncKVOperationImpl<K, V>
    implements AsyncSingleValueRetrieval<K, V>, ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry> {
  private final RetrievalOperation<K> retrievalOperation;
  private final VersionConstraint resolvedVC;
  private final OpSender retrievalSender; // for requesting segments or retries

  private static Logger log = LoggerFactory.getLogger(AsyncRetrievalOperationImpl.class);

  private final List<OperationUUID> opUUIDs;
  private List<SegmentedRetrievalValue<K, V>> segmentedRetrievalValues;
  //private final AtomicReference<Set<DHTKey>> latestStoredReturnedRef;
  private final ConcurrentSkipListSet<DHTKey> latestStoredReturned;

  private final ConcurrentMap<K, RetrievalResultBase<V>> results;
  private final ActiveRetrievalListeners activeRetrievalListeners;

  private static final int opConcurrencyLevel = 4;
  private static final int capacityFactor = 2;

  private static final int waitForConstantTime_ms = 60 * 1000;
  private static final boolean testReceiveCorruption = false;
  private static final double receiveCorruptionProbability = 0.3;

  private static final boolean debugShortTimeout = false;
  private static final int shortTimeoutLimit = 10 * 1000;

  public AsyncRetrievalOperationImpl(RetrievalOperation<K> retrievalOperation, ClientNamespace namespace,
      NamespacePerspectiveOptionsImpl<K, V> nspoImpl, long curTime, byte[] originator) {
    super(retrievalOperation, namespace, nspoImpl, curTime, originator);
    this.retrievalOperation = retrievalOperation;
    this.retrievalSender = namespace.getRetrievalSender();

    activeRetrievalListeners = namespace.getActiveRetrievalListeners();

    int retrievalOperationSize;

    retrievalOperationSize = retrievalOperation.size();
    if (retrievalOperationSize > 1) {
      results = new ConcurrentHashMap<>(retrievalOperationSize * capacityFactor, opConcurrencyLevel);
    } else {
      results = new ConcurrentSingleMap<>();
    }
    if (retrievalOperation.retrievalOptions().getVersionConstraint() == VersionConstraint.defaultConstraint) {
      resolvedVC = VersionConstraint.defaultConstraint;
    } else {
      resolvedVC = retrievalOperation.retrievalOptions().getVersionConstraint();
    }
    if (retrievalOperation.size() == 0) {
      checkForCompletion();
    }
    opUUIDs = new LinkedList<>();
    //latestStoredReturnedRef = new AtomicReference<>();
    // FUTURE - avoid eager construction?
    latestStoredReturned = new ConcurrentSkipListSet<>();
  }

  RetrievalOptions retrievalOptions() {
    return retrievalOperation.retrievalOptions();
  }

  @Override
  protected NonExistenceResponse getNonExistenceResponse() {
    return retrievalOperation.retrievalOptions().getNonExistenceResponse();
  }

  @Override
  public OpResult getOpResult(K key) {
    RetrievalResultBase<V> result;

    result = results.get(key);
    if (result != null) {
      return result.getOpResult();
    } else {
      return OpResult.INCOMPLETE;
    }
  }

  @Override
  protected boolean isFailure(OpResult result) {
    switch (result) {
    case MULTIPLE:
      // multiple will only be called when checking completion of the entire operation
      // others will be called key-by-key also

      // filter no such value errors to allow for users to override
      // all other failures result in a failure
      NonExistenceResponse nonExistenceResponse;

      nonExistenceResponse = getNonExistenceResponse();
      for (OpResult _result : allResults) {
        if (_result.hasFailed(nonExistenceResponse)) {
          return true;
        }
      }
      return false;
    case TIMEOUT:
      RetrievalOptions retrievalOptions;

      retrievalOptions = retrievalOperation.retrievalOptions();
      if (retrievalOptions.getWaitMode() == WaitMode.WAIT_FOR && ((WaitOptions) retrievalOptions).getTimeoutResponse() == TimeoutResponse.IGNORE) {
        return false;
      } else {
        return result.hasFailed();
      }
    default:
      return result.hasFailed(getNonExistenceResponse());
    }
  }

  //@Override
  //protected void setResult(EnumSet<OpResult> results) {
  //    super.setResult(results);
        /*
        for (OpResult result : results) {
            if (isFailure(result)) {
            }
        }
        */
  //}

  @Override
  public OperationState getOperationState(K key) {
    return getOpResult(key).toOperationState(getNonExistenceResponse());
  }

  @Override
  public void waitForCompletion() throws RetrievalException {
    try {
      super._waitForCompletion();
    } catch (OperationException oe) {
      throw (RetrievalException) oe;
    }
  }

  @Override
  protected void throwFailedException() throws OperationException {
    throw newRetrievalException();
  }

  private RetrievalException newRetrievalException() throws RetrievalException {
    return new RetrievalExceptionImpl((Map<Object, OperationState>) getOperationStateMap(),
        (Map<Object, FailureCause>) getFailureCauses(), (Map<Object, StoredValue>) getPartialResults());
  }

  @Override
  protected void cleanup() {
  }

  @Override
  void addToEstimate(MessageEstimate estimate) {
    ((KeyedMessageEstimate) estimate).addKeys(size);
  }

  @Override
  MessageEstimate createMessageEstimate() {
    return new KeyedMessageEstimate();
  }

  @Override
  ProtoMessageGroup createProtoMG(MessageEstimate estimate) {
    return createProtoRetrievalMG(estimate, false);
  }

  private ProtoRetrievalMessageGroup createProtoRetrievalMG(MessageEstimate estimate, boolean verifyIntegrity) {
    OperationUUID opUUID;
    SkTraceId maybeTraceID;
    ConcurrentMap<DHTKey, List<ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry>>> newMap;
    KeyedMessageEstimate keyedMessageEstimate;
    int relDeadline;

    keyedMessageEstimate = (KeyedMessageEstimate) estimate;
    opUUID = activeRetrievalListeners.newOpUUID();
    maybeTraceID = retrievalOptions().getTraceIDProvider().traceID();
    relDeadline = operation.getTimeoutController().getMaxRelativeTimeoutMillis(this);
    if (debugShortTimeout) {
      if (relDeadline < shortTimeoutLimit) {
        log.info("short relDeadline: {} ", relDeadline);
        //Log.info(timeoutParameters.computeTimeout(keyedMessageEstimate.getNumKeys()));
        log.info("{}", timeoutState.getCurRelTimeoutMillis());
        //throw new RuntimeException();
      }
    }
    return new ProtoRetrievalMessageGroup(opUUID, context.contextAsLong(),
        new InternalRetrievalOptions(retrievalOperation.retrievalOptions(), verifyIntegrity), originator,
        keyedMessageEstimate.getNumKeys(), relDeadline, retrievalOperation.retrievalOptions().getForwardingMode(),
        maybeTraceID);
  }

  @Override
  ProtoMessageGroup createMessagesForIncomplete(ProtoMessageGroup protoMG, List<MessageGroup> messageGroups,
      MessageEstimate estimate) {
    return createMessagesForIncomplete((ProtoRetrievalMessageGroup) protoMG, messageGroups,
        (KeyedMessageEstimate) estimate);
  }

  private ProtoMessageGroup createMessagesForIncomplete(ProtoRetrievalMessageGroup protoMG,
      List<MessageGroup> messageGroups, KeyedMessageEstimate estimate) {
    int pmgRetrievals;
    int keysRemaining; // will be needed when we check for compatible retrievals

    pmgRetrievals = 0;
    keysRemaining = retrievalOperation.size();

    // now fill in keys and values
    for (K key : getKeys()) {
      if (!getSent() || OpResult.isIncompleteOrNull(getOpResult(key))) {
        DHTKey dhtKey;

        dhtKey = keyToDHTKey.get(key);
        //System.out.printf("%d\t%d %d\n", keysRemaining, pmgRetrievals, maxRetrievalsPerMessageGroup);
        if (activeRetrievalListeners.addListener(protoMG.getUUID(), dhtKey, this)) {
          protoMG.addKey(dhtKey);
        }
        opUUIDs.add((OperationUUID) protoMG.getUUID()); // hold a reference to the uuid to prevent GC

        ++pmgRetrievals;
        --keysRemaining;
      } else {
        throw new RuntimeException("resends not yet implemented");
      }
    }
    return protoMG;
  }

  @Override
  public Map<K, ? extends StoredValue<V>> getStoredValues() throws RetrievalException {
    // The contract of this method is to return all successfully stored values.
    // If the entire op has completed, then we can simply return the internal map.
    if (getState() == OperationState.SUCCEEDED && getResult() != OpResult.NO_SUCH_VALUE) {
      //System.out.println("skipping build");
      return results;
    } else {
      Map<K, StoredValue<V>> storedValueMap;

      // If the entire op has *not* completed, then we must create a new map
      // to contain only successful values.
      storedValueMap = new HashMap<>(retrievalOperation.size());
      for (Map.Entry<K, RetrievalResultBase<V>> resultEntry : results.entrySet()) {
        RetrievalResultBase<V> value;

        value = resultEntry.getValue();
        if (value.getOpResult() == OpResult.SUCCEEDED) {
          storedValueMap.put(resultEntry.getKey(), value);
        }
      }
      return storedValueMap;
    }
  }

  @Override
  public StoredValue<V> getStoredValue(K key) throws RetrievalException {
    RetrievalResultBase<V> retrievalResult;

    retrievalResult = results.get(key);
    if (retrievalResult != null) {
      if (retrievalResult.getOpResult() == OpResult.SUCCEEDED) {
        return retrievalResult;
      } else {
        // For this case, we do not throw exceptions for missing values irrespective of the
        // NonExistenceResponse specified. We allow users to probe for missing values without
        // worrying about exceptions getting thrown back at them.
        if (retrievalResult.getOpResult().hasFailed() && retrievalResult.getOpResult() != OpResult.NO_SUCH_VALUE) {
          //System.err.println(key +"\t"+ retrievalResult.getOpResult());
          throw newRetrievalException();
        } else {
          return null;
        }
      }
    } else {
      return null;
    }
  }

  @Override
  public Map<K, ? extends StoredValue<V>> getLatestStoredValues() throws RetrievalException {
    Map<K, StoredValue<V>> storedValueMap;

    storedValueMap = new HashMap<>(retrievalOperation.size() - latestStoredReturned.size());
    for (Map.Entry<K, RetrievalResultBase<V>> resultEntry : results.entrySet()) {
      K key;
      DHTKey dhtKey;

      key = resultEntry.getKey();
      dhtKey = keyToDHTKey.get(key);
      if (!latestStoredReturned.contains(dhtKey)) {
        RetrievalResultBase<V> value;

        latestStoredReturned.add(dhtKey);
        value = resultEntry.getValue();
        if (value.getOpResult() == OpResult.SUCCEEDED) {
          storedValueMap.put(resultEntry.getKey(), value);
        }
      }
    }
    return storedValueMap;
  }

  public void errorReceived(DHTKey dhtKey) {

  }

  @Override
  public void resultReceived(DHTKey dhtKey, MessageGroupRetrievalResponseEntry entry) {
    // FUTURE - avoid the multi-step construction
    resultReceived(new RawRetrievalResult(retrievalOperation.retrievalOptions().getRetrievalType()), dhtKey, entry);
  }

  public final void resultReceived(RawRetrievalResult rawResult, DHTKey dhtKey,
      MessageGroupRetrievalResponseEntry entry) {
    OpResult opResult;
    boolean setComplete;
    int oldSegmentsCreated;
    boolean segmented;

    // NEED TO MODIFY THIS METHOD TO ACCEPT SEGMENTED COMPLETIONS
    // THINK ABOUT STRUCTURE OF CODE

    oldSegmentsCreated = fragmentsCreated;
    //System.out.printf("resultReceived key: %s\nentry: %s\n%s\nvalue: %s\n%s\n",
    //        dhtKey, entry, entry.getOpResult(), StringUtil.byteBufferToHexString(entry.getValue()),
    //        StringUtil.byteBufferToString(entry.getValue()));
    //System.out.printf("resultReceived key: %s\nentry: %s\n%s\nvalue buf: %s\n",
    //        dhtKey, entry, entry.getOpResult(), entry.getValue());

        /*
        if (mvLock != null) {
            mvLock.lock();
        }
        try {
        */
    segmented = false;
    opResult = entry == null ? OpResult.ERROR : entry.getOpResult();
    log.debug("{} opResult: {}", this, opResult);
    if (opResult == OpResult.SUCCEEDED) {
      try {
        if (testReceiveCorruption) {
          MetaDataUtil.testCorruption(entry.getValue(), receiveCorruptionProbability,
              MetaDataUtil.getDataOffset(entry.getValue(), 0));
        }
        //segmented = MetaDataUtil.isSegmented(entry.getValue().array(), entry.getValue().position());
        segmented = MetaDataUtil.isSegmented(entry.getValue());
        rawResult.setStoredValue(entry.getValue(),
            !segmented && retrievalOperation.retrievalOptions().getVerifyChecksums(),
            !retrievalOperation.retrievalOptions().getReturnInvalidations(),
            nspoImpl.getNSPOptions().getEncrypterDecrypter());
      } catch (CorruptValueException cve) {
        // Client will get sent back a failing OpResult.CORRUPT by code path for
        // complete ops below
        log.info(String.format("Result for key did not pass checksum validation\t%s", dhtKey));
      }
    } else {
      if (opResult == OpResult.REPLICA_EXCLUDED
          && retrievalOperation.retrievalOptions().getAllReplicasExcludedResponse() == AllReplicasExcludedResponse.IGNORE) {
        // All replicas excluded, but options say to ignore it
        // (I.e. wait for a replica to recover)
        log.info("Ignoring replica exclusion");
      } else {
        rawResult.setOpResult(opResult);
      }
    }
    if (opResult == OpResult.SUCCEEDED && segmented) {
      ByteBuffer buf;

      if (debugFragmentation) {
        log.info("SEGMENTED RESULT\n");
      }
      buf = rawResult.getValue();
      if (retrievalOperation.retrievalOptions().getRetrievalType().hasValue()) {
        DHTKey[] segmentKeys;
        int numSegments;

        if (debugFragmentation) {
          log.debug("SEGMENTED  {}  {}  {}  {}",
              StringUtil.byteArrayToHexString(SegmentationUtil.getCreatorBytes(buf)), buf,
              SegmentationUtil.getStoredLength(buf), SegmentationUtil.getUncompressedLength(buf));
        }
        if (true) {
          int storedLength;
          int fragmentationThreshold;

          //not using below since the internal checksum should handle this
          //if (SegmentationUtil.checksumSegmentMetaDataBuffer(buf, nspoImpl.getNSPOptions().getChecksumType())) {
          setComplete = false;
          storedLength = SegmentationUtil.getStoredLength(buf);
          fragmentationThreshold = SegmentationUtil.getFragmentationThreshold(buf);
          if (storedLength < 0) {
            log.error(StringUtil.byteBufferToHexString(buf));
            log.error("{}",SegmentationUtil.getMetaData(rawResult, buf));
            System.exit(-1);
            numSegments = 1;
          } else {
            numSegments = SegmentationUtil.getNumSegments(storedLength, fragmentationThreshold);
          }
          fragmentsCreated += numSegments;
          if (debugFragmentation) {
            log.info("NUM SEGMENTS  {}", numSegments);
          }
          segmentKeys = keyCreator.createSubKeys(dhtKey, numSegments);
          retrieveSegments(dhtKey, segmentKeys, SegmentationUtil.getMetaData(rawResult, buf));
        } else {
          setComplete = true;
          rawResult.setOpResult(OpResult.CORRUPT, true);
          opResult = OpResult.CORRUPT;
        }
      } else {
        if (debugFragmentation) {
          log.info("SEGMENTED. MetaData retrieval");
        }
        setComplete = true;
      }
    } else {
      if (debugFragmentation) {
        log.info("opResult {}", opResult);
      }
      setComplete = true;
    }
    if (setComplete) {
      RetrievalResultBase<V> prev;
      RetrievalResult<V> newResult;

      if (log.isDebugEnabled()) {
        log.debug("setComplete: {}", setComplete);
        log.debug("dhtKey: {} ", dhtKey);
      }
      newResult = new RetrievalResult<>(rawResult, nspoImpl.getValueDeserializer());
      prev = results.putIfAbsent(dhtKeyToKey.get(dhtKey), newResult);
      if (prev == null) {
        if (resultsReceived.incrementAndGet() >= size) {
          checkForCompletion();
          // FUTURE - this doesn't work for multi valued since we don't know how many we are getting...
          // For now, we ignore this since we aren't supporting multi-value yet
        }
      } else {
                /*
                RetrievalResultBase<V>  p;
                boolean                    unique;

                // FUTURE - handle mutual exclusion
                unique = true;
                p = prev;
                while (p.getNext() != null) {
                    p = p.getNext();
                    if (p.getCreationTime().equals(newResult.getCreationTime())) {
                        unique = false;
                        break;
                    }
                }
                if (unique) {
                    p.setNext(newResult);
                } else {
                    // Ignoring duplicate result
                }
                */
      }
    }
        /*
        if (segmentsCreated != oldSegmentsCreated) {
            recomputeTimeoutState();
        }
        // FUTURE THINK ABOUT WHETHER WE NEED THIS
        */
    checkForUpdates();
  }

  public void reassembledResultReceived(DHTKey dhtKey, SegmentedRetrievalResult<V> segmentedRetrievalResult) {
    RawRetrievalResult rawResult;

    if (false) {
      log.info("reassembledResultReceived {} {}", dhtKey, segmentedRetrievalResult);
    }
    results.putIfAbsent(dhtKeyToKey.get(dhtKey), segmentedRetrievalResult);
    if (resultsReceived.incrementAndGet() >= size) {
      //System.out.println("checkForCompletion");
      checkForCompletion();
    } else {
      //System.out.printf("%d < %d\n", resultsReceived.incrementAndGet(), size);
    }
  }

  private void retrieveSegments(DHTKey relayKey, DHTKey[] segmentKeys, MetaData metaData) {
    List<MessageGroup> messageGroups;
    SegmentedRetrievalValue<K, V> segmentedRetrievalValue;

    if (segmentedRetrievalValues == null) {
      segmentedRetrievalValues = new ArrayList<>(segmentKeys.length);
    }
    segmentedRetrievalValue = new SegmentedRetrievalValue<>(segmentKeys, relayKey, this,
        nspoImpl.getValueDeserializer(), metaData);
    messageGroups = new ArrayList<>();
    for (DHTKey segmentKey : segmentKeys) {
      ProtoRetrievalMessageGroup protoRetrievalMG;
      boolean listenerInserted;
      List<WeakReference<ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry>>> listeners;

      protoRetrievalMG = createProtoRetrievalMG(new KeyedMessageEstimate(1), false);
      listenerInserted = activeRetrievalListeners.addListener(protoRetrievalMG.getUUID(), segmentKey,
          segmentedRetrievalValue);
      if (!listenerInserted) {
        throw new RuntimeException("Duplicate listener insertion");
      }
      protoRetrievalMG.addKey(segmentKey);
      protoRetrievalMG.addToMessageGroupList(messageGroups);
      opUUIDs.add((OperationUUID) protoRetrievalMG.getUUID()); // hold a reference to the uuid to prevent GC
      segmentedRetrievalValues.add(segmentedRetrievalValue);
    }
    for (MessageGroup messageGroup : messageGroups) {
      retrievalSender.send(messageGroup);
    }
  }

  @Override
  public Map<K, V> getValues() throws RetrievalException {
    Map<K, V> valueMap;

    valueMap = new HashMap<>(results.size());
    for (Map.Entry<K, RetrievalResultBase<V>> resultEntry : results.entrySet()) {
      valueMap.put(resultEntry.getKey(), resultEntry.getValue().getValue());
    }
    return valueMap;
    // Immutable map doesn't support null entries...
        /*
        ImmutableMap.Builder<K, V> valueMapBuilder;

        valueMapBuilder = ImmutableMap.builder();
        for (Map.Entry<K, RetrievalResultBase<V>> resultEntry : results.entrySet()) {
            valueMapBuilder.put(resultEntry.getKey(), resultEntry.getValue().getValue());
        }
        return valueMapBuilder.build();
        */
  }

  @Override
  public V getValue(K key) throws RetrievalException {
    StoredValue<V> storedValue;

    storedValue = results.get(key);
    if (storedValue == null) {
      return null;
    } else {
      return storedValue.getValue();
    }
  }

  @Override
  public Map<K, V> getLatestValues() throws RetrievalException {
    Map<K, V> valueMap;

    valueMap = new HashMap<>(retrievalOperation.size() - latestStoredReturned.size());
    for (Map.Entry<K, RetrievalResultBase<V>> resultEntry : results.entrySet()) {
      K key;
      DHTKey dhtKey;

      key = resultEntry.getKey();
      dhtKey = keyToDHTKey.get(key);
      if (!latestStoredReturned.contains(dhtKey)) {
        RetrievalResultBase<V> value;

        latestStoredReturned.add(dhtKey);
        value = resultEntry.getValue();
        if (value.getOpResult() == OpResult.SUCCEEDED) {
          valueMap.put(resultEntry.getKey(), value.getValue());
        }
      }
    }
    return valueMap;
  }

  @Override
  public StoredValue<V> getStoredValue() throws RetrievalException {
    Iterator<K> iterator;

    iterator = results.keySet().iterator();
    return iterator.hasNext() ? getStoredValue(iterator.next()) : null;
    // FUTURE - THINK ABOUT SPEEDING THIS UP
    // plan is for the single op case to use a custom map, could switch implementation then
  }

  @Override
  public V getValue() throws RetrievalException {
    StoredValue<V> storedValue;

    storedValue = getStoredValue();
    if (storedValue != null) {
      return storedValue.getValue();
    } else {
      return null;
    }
  }

  @Override
  public boolean canBeGroupedWith(AsyncOperationImpl asyncOperationImpl) {
    if (asyncOperationImpl instanceof AsyncRetrievalOperationImpl) {
      AsyncRetrievalOperationImpl other;

      other = (AsyncRetrievalOperationImpl) asyncOperationImpl;
      // FUTURE - consider a weaker notion of compatibility
      return retrievalOperation.retrievalOptions().equals(other.retrievalOperation.retrievalOptions());
    } else {
      return false;
    }
  }

  public Map<K, StoredValue> getPartialResults() {
    Map<K, StoredValue> partialResults;

    partialResults = new HashMap<>();
    for (Map.Entry<K, RetrievalResultBase<V>> result : results.entrySet()) {
      partialResults.put(result.getKey(), result.getValue());
    }
    return partialResults;
  }

  @Override
  public RetrievalOptions getRetrievalOptions() {
    return retrievalOperation.retrievalOptions();
  }
    
    /*
    protected void checkForCompletion() {
        // This method should only be called once in most cases,
        // so a lock here is OK. If it winds up being called multiple
        // times, then this approach is not appropriate.
        //synchronized (this) {
            if (retrievalOperation.size() == 0) {
                setResult(OpResult.SUCCEEDED);
                return;
            } else {
                EnumSet<OpResult> candidateResults;

                candidateResults = null;
                
                // FUTURE - ensure that we can't have missing values
                // think we're clean due to calling preconditions, but double check
                for (Map.Entry<K, RetrievalResultBase<V>> resultEntry : results.entrySet()) {
                    K           key;
                    OpResult    keyResult;
                    
                    key = resultEntry.getKey();
                    keyResult = resultEntry.getValue().getOpResult();
                    if (!keyResult.isComplete()) {
                        // early exit if any result is not complete
                        return;
                    }
                    if (candidateResults == null) {
                        candidateResults = EnumSet.of(keyResult);
                    } else {
                        candidateResults.add(keyResult);
                    }
                    if (isFailure(keyResult)) {
                        getFailureCauses().put(key, keyResult.toFailureCause());
                    }
                    // we only arrive here if all results are complete
                    if (candidateResults != null && candidateResults.size() > 0) {
                        setResult(candidateResults);
                    }
                }
            }
        //}
    }
    */

    /*
    public void debugReferences() {
        //System.out.println("#\t"+ objectToString());
        for (OperationUUID opUUID : opUUIDs) {
            ConcurrentMap<DHTKey,List<WeakReference<ActiveOperationResultListener<MessageGroupRetrievalResponseEntry
            >>>>   map;
            
            //System.out.println("#\t\t"+ opUUID);
            map = activeRetrievalListeners.get(opUUID);
            for (Map.Entry<DHTKey,List<WeakReference<ActiveOperationResultListener<MessageGroupRetrievalResponseEntry
            >>>> entry : map.entrySet()) {
                //System.out.println("#\t\t\t"+ entry.getKey());
                for (WeakReference<ActiveOperationResultListener<MessageGroupRetrievalResponseEntry>> listenerRef :
                entry.getValue()) {
                    ActiveOperationResultListener<MessageGroupRetrievalResponseEntry>   listener;
                    
                    listener = listenerRef.get();
                    if (listener instanceof AsyncRetrievalOperationImpl) {
                        AsyncRetrievalOperationImpl opImpl;
                        
                        opImpl = (AsyncRetrievalOperationImpl)listener;
                        ////System.out.println("#\t\t\t\t"+ opImpl.getUUID());
                        if (opImpl.opUUIDs.size() > 1) {
                            System.out.printf("LARGE %s\n", opUUID);
                        }
                        for (Object _opUUID : opImpl.opUUIDs) {
                            //System.out.println("#\t\t\t\t"+ _opUUID);
                            if (!_opUUID.equals(opUUID)) {
                                System.out.printf("NOT EQUAL %s %s\n", opUUID, _opUUID);
                            }
                        }
                    }
                }
            }
        }
    }
    */
}
