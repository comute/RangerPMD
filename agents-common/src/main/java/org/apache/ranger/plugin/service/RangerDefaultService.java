/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.plugin.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RangerDefaultService extends RangerBaseService {
    private static final Logger LOG = LoggerFactory.getLogger(RangerDefaultService.class);

    public static final String ERROR_MSG_VALIDATE_CONFIG_NOT_IMPLEMENTED = "Configuration validation is not implemented";

    @Override
    public Map<String, Object> validateConfig() throws Exception {
        LOG.debug("RangerDefaultService.validateConfig Service: ({} ), " + ERROR_MSG_VALIDATE_CONFIG_NOT_IMPLEMENTED, serviceName);

        throw new Exception(ERROR_MSG_VALIDATE_CONFIG_NOT_IMPLEMENTED);
    }

    @Override
    public List<String> lookupResource(ResourceLookupContext context) throws Exception {
        LOG.debug("RangerDefaultService.lookupResource Context: ({}), returning empty list", context);

        return Collections.emptyList();
    }
}
