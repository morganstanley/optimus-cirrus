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

package msjava.msnet;
import java.util.EventObject;
public class MSNetEvent extends EventObject {
    protected String _tag;
    
    public MSNetEvent(Object source_) {
        this(source_, null);
    }
    
    public MSNetEvent(Object source_, String tag_) {
        super(source_);
        _tag = tag_;
    }
    
    public void setSource(Object source_) {
        source = source_;
    }
    
    public String getTag() {
        return _tag;
    }
    
    public void setTag(String tag_) {
        _tag = tag_;
    }
}
