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
package com.ms.silverking;

import com.ms.silverking.cloud.dht.net.ChainedIdSerialization;
import optimus.breadcrumbs.ChainedID;
import com.ms.silverking.proto.*;
import optimus.legacy.DoNotUseLegacySerializer;

public class LegacyChainedIdSerializer {
  // TODO (OPTIMUS-40255): remove this LegacyChainedIdSerializer once we have migrated to using new serialization for trace IDs
  public Silverking.ChainedIdProto serialize(ChainedID chainedID){
   return Silverking.ChainedIdProto
          .newBuilder()
          .addAllData(chainedID.asList())
          .build();
  }

  public ChainedID deserialize(Silverking.ChainedIdProto chainedIdProto){
    return DoNotUseLegacySerializer.fromList(chainedIdProto.getDataList());
  }
}

abstract class LegacyChainedIdSerialization {
  ChainedIdSerialization chained = new ChainedIdSerialization();
  LegacyChainedIdSerializer legacyChainedSerializer = new LegacyChainedIdSerializer();

   final Silverking.ChainedIdProto toProto(ChainedID chainedID){
     return legacyChainedSerializer.serialize(chainedID);
  }

  final ChainedID fromProto(Silverking.ChainedIdProto proto){
     return legacyChainedSerializer.deserialize(proto);
  }
}