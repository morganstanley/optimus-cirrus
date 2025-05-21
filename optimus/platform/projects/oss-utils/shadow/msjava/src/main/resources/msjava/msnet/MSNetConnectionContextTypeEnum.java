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
public class MSNetConnectionContextTypeEnum {
    
    public static final MSNetConnectionContextTypeEnum ACCEPTED_CONNECTION = new MSNetConnectionContextTypeEnum(
            "ACCEPTED_CONNECTION");
    
    public static final MSNetConnectionContextTypeEnum MANAGED_CONNECTION = new MSNetConnectionContextTypeEnum(
            "MANAGED_CONNECTION");
    
    public static final MSNetConnectionContextTypeEnum OPENED_CONNECTION = new MSNetConnectionContextTypeEnum(
            "OPENED_CONNECTION");
    final String name;
    MSNetConnectionContextTypeEnum(String name) {
        this.name = name;
    }
    public String toString() {
        return name;
    }
}
