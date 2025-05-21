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
public class MSNetTCPAcceptorStateEnum implements MSNetState {
    public static final MSNetTCPAcceptorStateEnum INVALID = new MSNetTCPAcceptorStateEnum("Invalid");
    public static final MSNetTCPAcceptorStateEnum LISTEN_ERROR = new MSNetTCPAcceptorStateEnum("Listen Error");
    public static final MSNetTCPAcceptorStateEnum ACCEPT_ERROR = new MSNetTCPAcceptorStateEnum("Accept Error");
    public static final MSNetTCPAcceptorStateEnum LISTEN = new MSNetTCPAcceptorStateEnum("Listen");
    public static final MSNetTCPAcceptorStateEnum CLOSED = new MSNetTCPAcceptorStateEnum("Closed");
    protected final String name;
    protected MSNetTCPAcceptorStateEnum(String name) {
        this.name = name;
    }
    public String name() {
        return name;
    }
    public String toString() {
        return name;
    }
}
