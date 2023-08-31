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

import java.util.UUID;

import com.ms.silverking.cloud.dht.GetOptions;
import com.ms.silverking.cloud.dht.InvalidationOptions;
import com.ms.silverking.cloud.dht.NamespacePerspectiveOptions;
import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.WaitOptions;
import com.ms.silverking.cloud.dht.client.KeyDigestType;
import com.ms.silverking.cloud.dht.client.serialization.BufferDestSerializer;
import com.ms.silverking.cloud.dht.client.serialization.BufferSourceDeserializer;
import com.ms.silverking.cloud.dht.client.serialization.SerializationRegistry;
import com.ms.silverking.cloud.dht.client.serialization.internal.ArrayMD5KeyCreator;
import com.ms.silverking.cloud.dht.client.serialization.internal.IntegerMD5KeyCreator;
import com.ms.silverking.cloud.dht.client.serialization.internal.LongMD5KeyCreator;
import com.ms.silverking.cloud.dht.client.serialization.internal.SerializedMD5KeyCreator;
import com.ms.silverking.cloud.dht.client.serialization.internal.StringMD5KeyCreator;
import com.ms.silverking.cloud.dht.client.serialization.internal.UUIDMD5KeyCreator;

/** NamespacePerspectiveOptions and classes necessary to implement the requested options. */
class NamespacePerspectiveOptionsImpl<K, V> {
  private final NamespacePerspectiveOptions<K, V> nspOptions;
  private final SerializationRegistry serializationRegistry;
  private final KeyCreator<K> keyCreator;
  private final BufferDestSerializer<V> valueSerializer;
  private final BufferSourceDeserializer<V> valueDeserializer;

  public NamespacePerspectiveOptionsImpl(
      NamespacePerspectiveOptions<K, V> nspOptions, SerializationRegistry serializationRegistry) {
    this.nspOptions = nspOptions;
    this.serializationRegistry = serializationRegistry;
    keyCreator =
        keyCreatorFor(
            serializationRegistry, nspOptions.getKeyClass(), nspOptions.getKeyDigestType());
    valueSerializer = serializationRegistry.getSerializer(nspOptions.getValueClass());
    if (valueSerializer == null) {
      throw new IllegalArgumentException(
          "Can't find serializer for: " + nspOptions.getValueClass());
    }
    valueDeserializer = serializationRegistry.getDeserializer(nspOptions.getValueClass());
    if (valueDeserializer == null) {
      throw new IllegalArgumentException(
          "Can't find deserializer for: " + nspOptions.getValueClass());
    }
  }

  public NamespacePerspectiveOptions<K, V> getNSPOptions() {
    return nspOptions;
  }

  public KeyCreator<K> getKeyCreator() {
    return keyCreator;
  }

  public BufferDestSerializer<V> getValueSerializer() {
    return valueSerializer;
  }

  public BufferSourceDeserializer<V> getValueDeserializer() {
    return valueDeserializer;
  }

  public PutOptions getDefaultPutOptions() {
    return nspOptions.getDefaultPutOptions();
  }

  public InvalidationOptions getDefaultInvalidationOptions() {
    return nspOptions.getDefaultInvalidationOptions();
  }

  public GetOptions getDefaultGetOptions() {
    return nspOptions.getDefaultGetOptions();
  }

  public WaitOptions getDefaultWaitOptions() {
    return nspOptions.getDefaultWaitOptions();
  }

  private static <K> KeyCreator<K> keyCreatorFor(
      SerializationRegistry serializationRegistry, Class keyClass, KeyDigestType keyDigestType) {
    if (keyClass == String.class) {
      switch (keyDigestType) {
        case MD5:
          return (KeyCreator<K>) new StringMD5KeyCreator();
        default:
          throw new RuntimeException("Unsupported <keyClass, KeyDigestType>");
      }
    } else if (keyClass == Integer.class) {
      switch (keyDigestType) {
        case MD5:
          return (KeyCreator<K>) new IntegerMD5KeyCreator();
        default:
          throw new RuntimeException("Unsupported <keyClass, KeyDigestType>");
      }
    } else if (keyClass == Long.class) {
      switch (keyDigestType) {
        case MD5:
          return (KeyCreator<K>) new LongMD5KeyCreator();
        default:
          throw new RuntimeException("Unsupported <keyClass, KeyDigestType>");
      }
    } else if (keyClass == UUID.class) {
      switch (keyDigestType) {
        case MD5:
          return (KeyCreator<K>) new UUIDMD5KeyCreator();
        default:
          throw new RuntimeException("Unsupported <keyClass, KeyDigestType>");
      }
    } else if (keyClass == byte[].class) {
      switch (keyDigestType) {
        case MD5:
          return (KeyCreator<K>) new ArrayMD5KeyCreator();
        default:
          throw new RuntimeException("Unsupported <keyClass, KeyDigestType>");
      }
    } else {
      BufferDestSerializer<K> s;

      s = serializationRegistry.getSerializer(keyClass);
      if (s != null) {
        return new SerializedMD5KeyCreator(s);
      } else {
        throw new IllegalArgumentException(
            "Unsupported key class. "
                + "No custom serializer, and no serializer found: "
                + keyClass);
      }
    }
  }

  public NamespacePerspectiveOptionsImpl<K, V> defaultPutOptions(PutOptions defaultPutOptions) {
    return new NamespacePerspectiveOptionsImpl(
        nspOptions.defaultPutOptions(defaultPutOptions), serializationRegistry);
  }

  public NamespacePerspectiveOptionsImpl<K, V> defaultInvalidationOptions(
      InvalidationOptions defaultInvalidationOptions) {
    return new NamespacePerspectiveOptionsImpl(
        nspOptions.defaultInvalidationOptions(defaultInvalidationOptions), serializationRegistry);
  }

  public NamespacePerspectiveOptionsImpl<K, V> defaultGetOptions(GetOptions defaultGetOptions) {
    return new NamespacePerspectiveOptionsImpl(
        nspOptions.defaultGetOptions(defaultGetOptions), serializationRegistry);
  }

  public NamespacePerspectiveOptionsImpl<K, V> defaultWaitOptions(WaitOptions defaultWaitOptions) {
    return new NamespacePerspectiveOptionsImpl(
        nspOptions.defaultWaitOptions(defaultWaitOptions), serializationRegistry);
  }

  private static void ensureCompatible(
      NamespacePerspectiveOptions o1, NamespacePerspectiveOptions o2) {
    if (!o1.getKeyClass().equals(o2.getKeyClass())) {
      throw new RuntimeException(
          "Incompatible key classes: " + o1.getKeyClass() + "\t" + o2.getKeyClass());
    }
    if (!o1.getValueClass().equals(o2.getValueClass())) {
      throw new RuntimeException(
          "Incompatible value classes: " + o1.getValueClass() + "\t" + o2.getValueClass());
    }
  }

  public NamespacePerspectiveOptionsImpl namespacePerspectiveOptions(
      NamespacePerspectiveOptions nspOptions) {
    ensureCompatible(this.nspOptions, nspOptions);
    return new NamespacePerspectiveOptionsImpl(nspOptions, serializationRegistry);
  }
}
