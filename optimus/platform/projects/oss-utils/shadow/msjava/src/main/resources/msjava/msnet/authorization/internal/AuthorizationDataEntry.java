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
package msjava.msnet.authorization.internal;
import java.util.Locale;
public class AuthorizationDataEntry {
    private final String entry;
    
    private final int id;
    public AuthorizationDataEntry(String s, int i) {
        assert s != null;
        
        entry = s.toLowerCase(Locale.ENGLISH);
        id = i;
    }
    
    public AuthorizationDataEntry(String s) {
        this(s, -1);
    }
    @Override
    public String toString() {
        return entry + " (id: " + id + ")";
    }
    @Override
    public boolean equals(Object obj) {
        return obj instanceof AuthorizationDataEntry && entry.equals(((AuthorizationDataEntry) obj).entry);
    }
    @Override
    public int hashCode() {
        return entry.hashCode();
    }
}