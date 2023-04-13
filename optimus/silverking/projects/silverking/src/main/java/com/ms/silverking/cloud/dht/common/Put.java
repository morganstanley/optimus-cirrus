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
package com.ms.silverking.cloud.dht.common;

public interface Put {
  public DHTKey getKey();

  public Version getVersion();
    /*
    private final Namespace ns;
    private final DHTKey    key;
    private final Version   version;
    private final long      opTimeMillis;
    
    public Retrieval(Namespace ns, DHTKey key, Version version, long opTimeMillis) {
        this.ns = ns;
        this.key = key;
        this.version = version;
        this.opTimeMillis = opTimeMillis;
    }
    
    public Namespace getNamespace() {
        return ns;
    }
    
    public DHTKey getKey() {
        return key;
    }
    
    public Version getVersion() {
        return version;
    }
    
    @Override
    public int hashCode() {
        return key.hashCode() ^ ns.hashCode() ^ version.hashCode();
    }
    
    @Override
    public boolean equals(Object o) {
        Retrieval   other;
        
        other = (Retrieval)o;
        return other.ns.equals(ns)
            && other.key.equals(key)
            && other.version.equals(version)
            && other.opTimeMillis == opTimeMillis;
    }
    */
}
