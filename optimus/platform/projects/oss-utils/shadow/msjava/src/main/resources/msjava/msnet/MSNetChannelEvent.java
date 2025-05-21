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
public class MSNetChannelEvent extends MSNetEvent {
    protected MSNetLoop _loop;
    public MSNetChannelEvent(Object source_, MSNetLoop loop_) {
        this(source_, null, loop_);
    }
    public MSNetChannelEvent(Object source_, String tag_, MSNetLoop loop_) {
        super(source_, tag_);
        _loop = loop_;
    }
    
    public MSNetLoop getLoop() {
        return _loop;
    }
    
    public void setLoop(MSNetLoop loop_) {
        _loop = loop_;
    }
}
