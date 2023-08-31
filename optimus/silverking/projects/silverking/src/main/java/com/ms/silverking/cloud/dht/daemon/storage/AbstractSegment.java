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
package com.ms.silverking.cloud.dht.daemon.storage;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Set;

import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.InternalRetrievalOptions;
import com.ms.silverking.cloud.dht.common.MetaDataUtil;
import com.ms.silverking.collection.cuckoo.IntCuckooConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractSegment implements ReadableSegment {
  protected ByteBuffer dataBuf;
  protected final OffsetListStore offsetListStore;
  protected final Set<Integer> invalidatedOffsets;

  protected static final int noSuchKey = IntCuckooConstants.noSuchValue;
  protected static final boolean debugRetrieve = false;
  protected static final boolean debugPut = false;
  protected static final boolean debugExternalStore = false;

  protected static Logger log = LoggerFactory.getLogger(AbstractSegment.class);

  AbstractSegment(
      ByteBuffer dataBuf, OffsetListStore offsetListStore, Set<Integer> invalidatedOffsets) {
    this.dataBuf = dataBuf;
    this.offsetListStore = offsetListStore;
    this.invalidatedOffsets = invalidatedOffsets;
  }

  abstract long getSegmentCreationMillis();

  abstract int getSegmentNumber();

  protected abstract int getRawOffset(DHTKey key);

  byte[] getChecksum(int offset) {
    assert offset >= 0;
    return MetaDataUtil.getChecksum(dataBuf, offset + DHTKey.BYTES_PER_KEY);
  }

  /** Returns whether the operation at the particular offset is an invalidation */
  boolean isInvalidation(int offset) {
    if (invalidatedOffsets != null) {
      return invalidatedOffsets.contains(offset);
    } else {
      return MetaDataUtil.isInvalidation(dataBuf, offset + DHTKey.BYTES_PER_KEY);
    }
  }

  long getVersion(int offset) {
    assert offset >= 0;
    return MetaDataUtil.getVersion(dataBuf, offset + DHTKey.BYTES_PER_KEY);
  }

  long getCreationTime(int offset) {
    assert offset >= 0;
    return MetaDataUtil.getCreationTime(dataBuf, offset + DHTKey.BYTES_PER_KEY);
  }

  ValueCreator getCreator(int offset) {
    return MetaDataUtil.getCreator(dataBuf, offset + DHTKey.BYTES_PER_KEY);
  }

  int getStoredLength(int offset) {
    return MetaDataUtil.getStoredLength(dataBuf, offset + DHTKey.BYTES_PER_KEY);
  }

  public ByteBuffer retrieve(DHTKey key, InternalRetrievalOptions options) {
    return retrieve(key, options, false);
  }

  public ByteBuffer retrieve(DHTKey key, InternalRetrievalOptions options, boolean verifySS) {
    try {
      int offset;

      offset = getRawOffset(key);
      if (debugRetrieve) {
        log.info("AbstractSegment.retrieve {} {} {}", key, options, offset);
      }
      return retrieve(key, options, offset, verifySS);
    } catch (RuntimeException re) { // TODO (OPTIMUS-0000): TEMP DEBUG
      log.info("segment {} {} {}", getSegmentNumber(), key, options);
      re.printStackTrace();
      throw re;
    }
  }

  /**
   * For utility use only
   *
   * @param key
   * @param offset
   * @return
   */
  public ByteBuffer retrieveForDebug(DHTKey key, int offset) {
    int storedLength;
    ByteBuffer buffer;
    ByteBuffer returnBuffer;

    offset += DHTKey.BYTES_PER_KEY;
    storedLength = MetaDataUtil.getStoredLength(dataBuf, offset);
    buffer = dataBuf.asReadOnlyBuffer();
    buffer.position(offset);
    buffer.limit(offset + storedLength);
    returnBuffer = buffer.slice();
    return returnBuffer;
  }

  private ByteBuffer retrieve(
      DHTKey key, InternalRetrievalOptions options, int offset, boolean verifySS) {
    // FUTURE - think about getResolvedOffset and doubleCheckVersion
    // Currently can't use getResolvedOffset due to the doubleCheckVersion
    // code. Need to determine if that code is required.
    if (debugRetrieve || log.isDebugEnabled()) {
      log.debug("segment number: {}", getSegmentNumber());
      log.debug("key offset: {} {}", key, offset);
    }
    if (offset == noSuchKey) {
      if (debugRetrieve) {
        log.info("noSuchKey");
      }
      return null;
    } else {
      int storedLength;
      ByteBuffer buffer;
      boolean doubleCheckVersion;
      ByteBuffer returnBuffer;

      if (offset < 0) {
        OffsetList offsetList;
        ValidityVerifier validityVerifier;

        doubleCheckVersion = false;
        if (debugRetrieve) {
          log.info("Looking in offset list: {}", -offset);
        }
        offsetList = offsetListStore.getOffsetList(-offset);
        if (debugRetrieve) {
          log.info("offsetList: {}", offsetList);
          offsetList.displayForDebug();
        }
        if (verifySS && options.getVerifyStorageState()) {
          validityVerifier = new ValidityVerifier(dataBuf, options.getCPSSToVerify());
        } else {
          validityVerifier = null;
        }
        offset = offsetList.getOffset(options.getVersionConstraint(), validityVerifier);
        if (offset < 0) {
          offset = noSuchKey; // FUTURE - think about this
          if (debugRetrieve) {
            log.info(
                "Couldn't find key in offset list. options: {}", options.getVersionConstraint());
          }
        }
        if (debugRetrieve || log.isDebugEnabled()) {
          log.info("offset list offset: {} {}", key, offset);
        }
      } else {
        doubleCheckVersion = true;
      }

      if (offset < 0) {
        if (debugRetrieve) {
          log.info("offset < 0");
        }
        return null;
      } else {
        offset += DHTKey.BYTES_PER_KEY;
        switch (options.getRetrievalType()) {
          case VALUE:
          case VALUE_AND_META_DATA:
            // FUTURE - consider creating a new buffer type to allow creation in one operation
            storedLength =
                MetaDataUtil.getStoredLength(dataBuf, offset); // FUTURE - think about this
            buffer = dataBuf.asReadOnlyBuffer();
            buffer.position(offset);
            buffer.limit(offset + storedLength);
            break;
          case EXISTENCE: // fall through
          case META_DATA:
            buffer = dataBuf.asReadOnlyBuffer();
            buffer.position(offset);
            buffer.limit(offset + MetaDataUtil.getMetaDataLength(dataBuf, offset));
            if (MetaDataUtil.isSegmented(buffer.slice())) {
              // FUTURE THIS CODE IS COPIED FROM VALUE CASES, ELIM THE DUPLICATE CODE
              storedLength =
                  MetaDataUtil.getStoredLength(
                      dataBuf, offset); // FUTURE & verify that segmented metadatautil works
              buffer = dataBuf.asReadOnlyBuffer();
              buffer.position(offset);
              buffer.limit(offset + storedLength);
            }
            break;
          default:
            throw new RuntimeException();
        }
        returnBuffer = buffer.slice();
        if (doubleCheckVersion) {
          VersionConstraint vc;

          vc = options.getVersionConstraint();

          if (debugRetrieve && returnBuffer != null) {
            boolean a;
            boolean b;
            boolean c;
            boolean d;

            a = !vc.equals(VersionConstraint.greatest);
            b = !vc.matches(MetaDataUtil.getVersion(returnBuffer, 0));
            c = vc.getMaxCreationTime() < Long.MAX_VALUE;
            d = vc.getMaxCreationTime() < MetaDataUtil.getCreationTime(returnBuffer, 0);
            log.info("doubleCheckVersion 1: {} {}", a && b, c && d);
            log.info("doubleCheckVersion 2: {} {} {} {}", a, b, c, d);
            log.info(
                "MetaDataUtil.getCreationTime(returnBuffer, 0): {}",
                MetaDataUtil.getCreationTime(returnBuffer, 0));
            if (c && d) {
              log.info(
                  "{} {}", vc.getMaxCreationTime(), MetaDataUtil.getCreationTime(returnBuffer, 0));
              log.info(
                  "{} {}",
                  new Date(vc.getMaxCreationTime()),
                  new Date(MetaDataUtil.getCreationTime(returnBuffer, 0)));
            }
          }

          // we include some extra checks below to avoid touching the returnBuffer unnecessarily
          if ((!vc.equals(VersionConstraint.greatest)
                  && !vc.matches(MetaDataUtil.getVersion(returnBuffer, 0)))
              || (vc.getMaxCreationTime() < Long.MAX_VALUE
                  && vc.getMaxCreationTime() < MetaDataUtil.getCreationTime(returnBuffer, 0))) {
            returnBuffer = null;
          }
        }
        if (debugRetrieve) {
          if (returnBuffer != null) {
            System.out.println(
                "MetaDataUtil.getCompressedLength: "
                    + MetaDataUtil.getCompressedLength(returnBuffer, 0));
          }
          log.info("returnBuffer: {}", returnBuffer);
        }
        return returnBuffer;
      }
    }
  }

  int getResolvedOffset(DHTKey key, VersionConstraint vc) {
    int offset;

    offset = getRawOffset(key);
    if (debugRetrieve || log.isDebugEnabled()) {
      log.debug("segment number: {}", getSegmentNumber());
      log.debug("key offset: {} {}", key, offset);
    }
    if (offset == noSuchKey) {
      if (debugRetrieve) {
        log.info("noSuchKey");
      }
      return noSuchKey;
    } else {
      if (offset < 0) {
        OffsetList offsetList;

        if (debugRetrieve) {
          log.info("Looking in offset list: {}", -offset);
        }
        offsetList = offsetListStore.getOffsetList(-offset);
        if (debugRetrieve) {
          log.info("offsetList: {}", offsetList);
          offsetList.displayForDebug();
        }
        offset = offsetList.getOffset(vc, null);
        if (offset < 0) {
          offset = noSuchKey; // FUTURE - think about this
          if (debugRetrieve) {
            log.info("Couldn't find key in offset list. options: {}", vc);
          }
        }
        if (debugRetrieve || log.isDebugEnabled()) {
          log.debug("offset list offset: {} {}", key, offset);
        }
      }
      if (offset < 0) {
        log.debug("getResolvedOffset < 0");
        return noSuchKey;
      } else {
        return offset;
      }
    }
  }
}
