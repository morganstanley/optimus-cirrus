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

package msjava.tools.util;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import msjava.base.util.internal.SystemPropertyUtils;
import msjava.tools.nativ.MSToolsJNIManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class MSProcess {
    private static final Logger log = LoggerFactory.getLogger(MSProcess.class);
    
    protected static boolean _loadedLib = false;
    
    static final class Pid {
        static final long pid = readPid(); 
    } 
    protected MSProcess() {
    }
    public static long getPID() {
        return Pid.pid;
    }
    
    static long readPid() {
        if (SystemPropertyUtils.getBoolean("msjava.tools.util.MSProcess.native_pid", false, log)) {
            return jniPid();
        } else {
            try {
                return mxBeanPid();
            } catch (Exception e) {
                log.warn("Cannot extract process id from the VM name", e);
                return jniPid();
            }
        }
    } 
    
    static long mxBeanPid() {
        String rtName = ManagementFactory.getRuntimeMXBean().getName();
        return Long.parseLong(rtName.substring(0, rtName.indexOf('@')));
    }
    static long jniPid() {
        if (!_loadedLib)
            loadLibrary();
        return _getPID();
    }
    protected static void loadLibrary() {
        MSToolsJNIManager.loadJNI("util_msprocess", MSProcess.class);
        _loadedLib = true;
    }
    protected static native long _getPID();
    public static String getHostName() throws UnknownHostException {
        return InetAddress.getLocalHost().getHostName();
    }
    public static String getHostAddress() throws UnknownHostException {
        return InetAddress.getLocalHost().getHostAddress();
    }
}
