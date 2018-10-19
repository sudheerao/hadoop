/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a.auth.delegation;

import java.net.URI;

import org.apache.hadoop.fs.s3a.auth.SessionCredentials;
import org.apache.hadoop.io.Text;

/**
 * The full credentials are the same of that for a session token; a different
 * name is used to ensure that the class which is marshalled is different
 * at the far end.
 *
 * Token kind is {@link DelegationConstants#FULL_TOKEN_KIND}.
 */
public class FullCredentialsTokenIdentifier extends SessionTokenIdentifier {

  public FullCredentialsTokenIdentifier() {
    super(DelegationConstants.FULL_TOKEN_KIND);
  }

  public FullCredentialsTokenIdentifier(final URI uri,
      final Text owner,
      final SessionCredentials sessionCredentials,
      final EncryptionSecrets encryptionSecrets) {
    super(DelegationConstants.FULL_TOKEN_KIND,
        owner,
        uri,
        sessionCredentials,
        encryptionSecrets);
  }
}
