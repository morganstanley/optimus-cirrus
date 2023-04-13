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
package com.ms.silverking.cloud.dht;

import com.google.common.base.Preconditions;
import com.ms.silverking.cloud.dht.client.ConstantVersionProvider;
import com.ms.silverking.cloud.dht.client.KeyDigestType;
import com.ms.silverking.cloud.dht.client.VersionProvider;
import com.ms.silverking.cloud.dht.client.crypto.EncrypterDecrypter;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.text.ObjectDefParser2;

/**
 * <p>Options specific to NamespacePerspectives as well as default options for individual
 * operations.</p>
 *
 * <p>Individual operations retain the ability to specify options such as
 * compression, but - by having defaults here - users may elide
 * these from individual operations if desired.</p>
 *
 * <p>Note that unlike the defaults in NamespaceOptions (which are fixed at Namespace creation time and persist),
 * these options only apply to the perspective of a single client.</p>
 */
public final class NamespacePerspectiveOptions<K, V> {
  private final Class<K> keyClass;
  private final Class<V> valueClass;
  private final KeyDigestType keyDigestType;
  private final PutOptions defaultPutOptions;
  private final InvalidationOptions defaultInvalidationOptions;
  private final GetOptions defaultGetOptions;
  private final WaitOptions defaultWaitOptions;
  private final VersionProvider defaultVersionProvider;
  private final EncrypterDecrypter encrypterDecrypter;

  // FUTURE - think about this
  static final KeyDigestType standardKeyDigestType = KeyDigestType.MD5;

  static final ConstantVersionProvider standardVersionProvider = new ConstantVersionProvider(
      SystemTimeUtil.skSystemTimeSource.absTimeMillis());
  static final NamespacePerspectiveOptions<byte[], byte[]> templateOptions = new NamespacePerspectiveOptions<byte[],
      byte[]>(
      byte[].class, byte[].class);

  static {
    ObjectDefParser2.addParser(templateOptions);
  }

  /**
   * Construct a NamespacePerspectiveOptions instance with complete specification of all parameters.
   * This constructor should generally be avoided. Instead, an instance of this class should be
   * obtained using Namespace.getDefaultNSPOptions().
   *
   * @param keyClass                   TODO
   * @param valueClass                 TODO
   * @param keyDigestType              TODO
   * @param defaultPutOptions          default PutOptions. If null, the namespace default will be used
   *                                   as the perspective default
   * @param defaultInvalidationOptions default InvalidationOptions. If null, the namespace default will be used
   *                                   as the perspective default
   * @param defaultGetOptions          default GetOptions. If null, the namespace default will be used
   *                                   as the perspective default
   * @param defaultWaitOptions         default WaitOptions. If null, the namespace default will be used
   *                                   as the perspective default
   * @param defaultVersionProvider     TODO
   * @param encrypterDecrypter         TODO
   */
  public NamespacePerspectiveOptions(Class<K> keyClass, Class<V> valueClass, KeyDigestType keyDigestType,
      PutOptions defaultPutOptions, InvalidationOptions defaultInvalidationOptions, GetOptions defaultGetOptions,
      WaitOptions defaultWaitOptions, VersionProvider defaultVersionProvider, EncrypterDecrypter encrypterDecrypter) {
    Preconditions.checkNotNull(keyClass);
    Preconditions.checkNotNull(valueClass);
    Preconditions.checkNotNull(keyDigestType);
    Preconditions.checkNotNull(defaultPutOptions);
    Preconditions.checkNotNull(defaultInvalidationOptions);
    Preconditions.checkNotNull(defaultGetOptions);
    Preconditions.checkNotNull(defaultWaitOptions);
    Preconditions.checkNotNull(defaultVersionProvider);

    this.keyClass = keyClass;
    this.valueClass = valueClass;
    this.keyDigestType = keyDigestType;
    this.defaultInvalidationOptions = defaultInvalidationOptions;
    this.defaultPutOptions = defaultPutOptions;
    this.defaultGetOptions = defaultGetOptions;
    this.defaultWaitOptions = defaultWaitOptions;
    this.defaultVersionProvider = defaultVersionProvider;
    if (encrypterDecrypter != null) {
      this.encrypterDecrypter = encrypterDecrypter;
    } else {
      this.encrypterDecrypter = DHTConstants.defaultEncrypterDecrypter;
    }
  }

  /**
   * For C++ implementation only. Do not use.
   *
   * @param keyClass                   same as above
   * @param valueClass                 same as above
   * @param keyDigestType              same as above
   * @param defaultPutOptions          same as above
   * @param defaultInvalidationOptions same as above
   * @param defaultGetOptions          same as above
   * @param defaultWaitOptions         same as above
   * @param defaultVersionProvider     same as above
   */
  public NamespacePerspectiveOptions(Class<K> keyClass, Class<V> valueClass, KeyDigestType keyDigestType,
      PutOptions defaultPutOptions, InvalidationOptions defaultInvalidationOptions, GetOptions defaultGetOptions,
      WaitOptions defaultWaitOptions, VersionProvider defaultVersionProvider) {
    this(keyClass, valueClass, keyDigestType, defaultPutOptions, defaultInvalidationOptions, defaultGetOptions,
        defaultWaitOptions, defaultVersionProvider, null);
  }

  /**
   * Construct a template NamespacePerspectiveOptions
   *
   * @param keyClass
   * @param valueClass
   */
  private NamespacePerspectiveOptions(Class<K> keyClass, Class<V> valueClass) {
    this(keyClass, valueClass, standardKeyDigestType, DHTConstants.standardPutOptions,
        DHTConstants.standardInvalidationOptions, DHTConstants.standardGetOptions, DHTConstants.standardWaitOptions,
        standardVersionProvider, null);
  }

  public Class<K> getKeyClass() {
    return keyClass;
  }

  public Class<V> getValueClass() {
    return valueClass;
  }

  public KeyDigestType getKeyDigestType() {
    return keyDigestType;
  }

  public PutOptions getDefaultPutOptions() {
    return defaultPutOptions;
  }

  public InvalidationOptions getDefaultInvalidationOptions() {
    return defaultInvalidationOptions;
  }

  public GetOptions getDefaultGetOptions() {
    return defaultGetOptions;
  }

  public WaitOptions getDefaultWaitOptions() {
    return defaultWaitOptions;
  }

  public VersionProvider getDefaultVersionProvider() {
    return defaultVersionProvider;
  }

  public EncrypterDecrypter getEncrypterDecrypter() {
    return encrypterDecrypter;
  }

  /**
   * Create a copy of this instance with a new keyClass
   *
   * @param keyClass new keyClass
   * @return the modified copy of the instance
   */
  public NamespacePerspectiveOptions<K, V> keyClass(Class keyClass) {
    return new NamespacePerspectiveOptions<>(keyClass, valueClass, keyDigestType, defaultPutOptions,
        defaultInvalidationOptions, defaultGetOptions, defaultWaitOptions, defaultVersionProvider, encrypterDecrypter);
  }

  /**
   * Create a copy of this instance with a new valueClass
   *
   * @param valueClass new valueClass
   * @return the modified copy of the instance
   */
  public NamespacePerspectiveOptions<K, V> valueClass(Class valueClass) {
    return new NamespacePerspectiveOptions<>(keyClass, valueClass, keyDigestType, defaultPutOptions,
        defaultInvalidationOptions, defaultGetOptions, defaultWaitOptions, defaultVersionProvider, encrypterDecrypter);
  }

  /**
   * Create a copy of this instance with a new KeyDigestType
   *
   * @param keyDigestType new KeyDigestType
   * @return the modified copy of the instance
   */
  public NamespacePerspectiveOptions<K, V> keyDigestType(KeyDigestType keyDigestType) {
    return new NamespacePerspectiveOptions<>(keyClass, valueClass, keyDigestType, defaultPutOptions,
        defaultInvalidationOptions, defaultGetOptions, defaultWaitOptions, defaultVersionProvider, encrypterDecrypter);
  }

  /**
   * Create a copy of this instance with a new default PutOptions
   *
   * @param defaultPutOptions new default PutOptions
   * @return the modified copy of the instance
   */
  public NamespacePerspectiveOptions<K, V> defaultPutOptions(PutOptions defaultPutOptions) {
    if (defaultPutOptions instanceof InvalidationOptions) {
      throw new IllegalArgumentException("InvalidationOptions not allowed for defaultPutOptions");
    }
    return new NamespacePerspectiveOptions<>(keyClass, valueClass, keyDigestType, defaultPutOptions,
        defaultInvalidationOptions, defaultGetOptions, defaultWaitOptions, defaultVersionProvider, encrypterDecrypter);
  }

  /**
   * Create a copy of this instance with a new default InvalidationOptions
   *
   * @param defaultInvalidationOptions new default InvalidationOptions
   * @return the modified copy of the instance
   */
  public NamespacePerspectiveOptions<K, V> defaultInvalidationOptions(InvalidationOptions defaultInvalidationOptions) {
    return new NamespacePerspectiveOptions<>(keyClass, valueClass, keyDigestType, defaultPutOptions,
        defaultInvalidationOptions, defaultGetOptions, defaultWaitOptions, defaultVersionProvider, encrypterDecrypter);
  }

  /**
   * Create a copy of this instance with a new default GetOptions
   *
   * @param defaultGetOptions new default GetOptions
   * @return the modified copy of the instance
   */
  public NamespacePerspectiveOptions<K, V> defaultGetOptions(GetOptions defaultGetOptions) {
    return new NamespacePerspectiveOptions<>(keyClass, valueClass, keyDigestType, defaultPutOptions,
        defaultInvalidationOptions, defaultGetOptions, defaultWaitOptions, defaultVersionProvider, encrypterDecrypter);
  }

  /**
   * Create a copy of this instance with a new default WaitOptions
   *
   * @param defaultWaitOptions new default WaitOptions
   * @return the modified copy of the instance
   */
  public NamespacePerspectiveOptions<K, V> defaultWaitOptions(WaitOptions defaultWaitOptions) {
    return new NamespacePerspectiveOptions<>(keyClass, valueClass, keyDigestType, defaultPutOptions,
        defaultInvalidationOptions, defaultGetOptions, defaultWaitOptions, defaultVersionProvider, encrypterDecrypter);
  }

  /**
   * Create a copy of this instance with a new default VersionProvider
   *
   * @param defaultVersionProvider new default VersionProvider
   * @return the modified copy of the instance
   */
  public NamespacePerspectiveOptions<K, V> defaultVersionProvider(VersionProvider defaultVersionProvider) {
    return new NamespacePerspectiveOptions<>(keyClass, valueClass, keyDigestType, defaultPutOptions,
        defaultInvalidationOptions, defaultGetOptions, defaultWaitOptions, defaultVersionProvider, encrypterDecrypter);
  }

  /**
   * Create a copy of this instance with a new encrypterDecrypter
   *
   * @param encrypterDecrypter the new encrypterDecrypter
   * @return the modified copy of the instance
   */
  public NamespacePerspectiveOptions<K, V> encrypterDecrypter(EncrypterDecrypter encrypterDecrypter) {
    return new NamespacePerspectiveOptions<>(keyClass, valueClass, keyDigestType, defaultPutOptions,
        defaultInvalidationOptions, defaultGetOptions, defaultWaitOptions, defaultVersionProvider, encrypterDecrypter);
  }

  /**
   * Parse a NamespacePerspectiveOptions definition
   *
   * @param def a NamespacePerspectiveOptions definition in SilverKing ObjectDefParser format
   * @return a parsed NamespacePerspectiveOptions instance
   */
  public NamespacePerspectiveOptions<K, V> parse(String def) {
    return ObjectDefParser2.parse(NamespacePerspectiveOptions.class, this, def);
  }

  @Override
  public int hashCode() {
    int hashCode;

    hashCode =
        keyClass.hashCode() ^ valueClass.hashCode() ^ keyDigestType.hashCode() ^ defaultPutOptions.hashCode() ^ defaultInvalidationOptions.hashCode() ^ defaultGetOptions.hashCode() ^ defaultWaitOptions.hashCode() ^ defaultVersionProvider.hashCode();

    if (encrypterDecrypter != null) {
      hashCode ^= encrypterDecrypter.hashCode();
    }

    return hashCode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (this.getClass() != o.getClass()) {
      return false;
    }

    NamespacePerspectiveOptions other;
    other = (NamespacePerspectiveOptions) o;

    boolean encrypterDecrypterEquals;
    if (encrypterDecrypter == null) {
      if (other.encrypterDecrypter == null) {
        encrypterDecrypterEquals = true;
      } else {
        return false;
      }
    } else {
      if (other.encrypterDecrypter == null) {
        return false;
      } else {
        encrypterDecrypterEquals = encrypterDecrypter.equals(other.encrypterDecrypter);
      }
    }

    return keyClass.equals(other.keyClass) && valueClass.equals(
        other.valueClass) && keyDigestType == other.keyDigestType && defaultPutOptions.equals(
        other.defaultPutOptions) && defaultInvalidationOptions.equals(
        other.defaultInvalidationOptions) && defaultGetOptions.equals(
        other.defaultGetOptions) && defaultWaitOptions.equals(
        other.defaultWaitOptions) && defaultVersionProvider.equals(
        other.defaultVersionProvider) && encrypterDecrypterEquals;
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }
}
