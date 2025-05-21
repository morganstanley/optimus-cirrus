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

package msjava.webauthd.kerberos.experimental.ticketvalidator;
import com.ms.msjava.webauthd.authenticator.exceptions.AccessDeniedException;
import com.ms.msjava.webauthd.authenticator.exceptions.AccessErrorException;
import com.ms.msjava.webauthd.authenticator.gssapi.WebauthdKerberosTicketAuthenticator;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.kerberos.authentication.KerberosTicketValidation;
import org.springframework.security.kerberos.authentication.KerberosTicketValidator;
public class WebauthdKerberosTicketValidator implements KerberosTicketValidator {
    private final WebauthdKerberosTicketAuthenticator authenticator;
    private final String servicePrincipal;
    public WebauthdKerberosTicketValidator() {
        this(new WebauthdKerberosTicketAuthenticator());
    }
    public WebauthdKerberosTicketValidator(WebauthdKerberosTicketAuthenticator authenticator) {
        this(authenticator, defaultSpnegoPrincipal());
    }
    private static String defaultSpnegoPrincipal() {
        try {
            return "HTTP/" + InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            return "HTTP/???";
        }
    }
    
    WebauthdKerberosTicketValidator(WebauthdKerberosTicketAuthenticator authenticator, String servicePrincipal) {
        this.authenticator = authenticator;
        this.servicePrincipal = servicePrincipal;
    }
    
    @Override
    public KerberosTicketValidation validateTicket(byte[] token) throws BadCredentialsException {
        try {
        	String authenticatedUser = authenticator.authenticate(new String(Base64.getEncoder().encode(token), StandardCharsets.US_ASCII));
        	return new KerberosTicketValidation(authenticatedUser, servicePrincipal, null, null);
        } catch (AccessDeniedException e) {
            throw new BadCredentialsException("Authentication was DENIED", e);
        } catch (AccessErrorException e) {
            throw new WebauthdKerberosTicketValidatorException("ERROR during validation", e);
        }
    }
    public WebauthdKerberosTicketAuthenticator getAuthenticator() {
        return authenticator;
    }
}
