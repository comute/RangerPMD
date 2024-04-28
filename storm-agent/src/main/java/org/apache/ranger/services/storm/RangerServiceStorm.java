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
package org.apache.ranger.services.storm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.services.storm.client.StormResourceMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RangerServiceStorm extends RangerBaseService {

	private static final Logger LOG = LoggerFactory.getLogger(RangerServiceStorm.class);
	public static final String ACCESS_TYPE_GET_TOPOLOGY  = "getTopology";
	public static final String ACCESS_TYPE_GET_TOPOLOGY_CONF  = "getTopologyConf";
	public static final String ACCESS_TYPE_GET_USER_TOPOLOGY  = "getUserTopology";
	public static final String ACCESS_TYPE_GET_TOPOLOGY_INFO  = "getTopologyInfo";
	
	public RangerServiceStorm() {
		super();
	}
	
	@Override
	public void init(RangerServiceDef serviceDef, RangerService service) {
		super.init(serviceDef, service);
	}

	@Override
	public List<RangerPolicy> getDefaultRangerPolicies() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerServiceStorm.getDefaultRangerPolicies()");
		}

		List<RangerPolicy> ret = super.getDefaultRangerPolicies();
		for (RangerPolicy defaultPolicy : ret) {
			if (defaultPolicy.getName().contains("all") && StringUtils.isNotBlank(lookUpUser)) {
				List<RangerPolicyItemAccess> accessListForLookupUser = new ArrayList<RangerPolicyItemAccess>();
				accessListForLookupUser.add(new RangerPolicyItemAccess(ACCESS_TYPE_GET_TOPOLOGY));
				accessListForLookupUser.add(new RangerPolicyItemAccess(ACCESS_TYPE_GET_TOPOLOGY_CONF));
				accessListForLookupUser.add(new RangerPolicyItemAccess(ACCESS_TYPE_GET_USER_TOPOLOGY));
				accessListForLookupUser.add(new RangerPolicyItemAccess(ACCESS_TYPE_GET_TOPOLOGY_INFO));
				RangerPolicyItem policyItemForLookupUser = new RangerPolicyItem();
				policyItemForLookupUser.setUsers(Collections.singletonList(lookUpUser));
				policyItemForLookupUser.setAccesses(accessListForLookupUser);
				policyItemForLookupUser.setDelegateAdmin(false);
				defaultPolicy.addPolicyItem(policyItemForLookupUser);
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerServiceStorm.getDefaultRangerPolicies()");
		}
		return ret;
	}

	@Override
	public Map<String,Object> validateConfig() throws Exception {
		Map<String, Object> ret = new HashMap<String, Object>();
		String 	serviceName  	    = getServiceName();
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerServiceStorm.validateConfig Service: (" + serviceName + " )");
		}
		if ( configs != null) {
			try  {
				ret = StormResourceMgr.validateConfig(serviceName, configs);
			} catch (Exception e) {
				LOG.error("<== RangerServiceStorm.validateConfig Error:" + e);
				throw e;
			}
		}
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerServiceStorm.validateConfig Response : (" + ret + " )");
		}
		return ret;
	}

	@Override
	public List<String> lookupResource(ResourceLookupContext context) throws Exception {
		
		List<String> ret = new ArrayList<String>();
		String 	serviceName  	   = getServiceName();
		Map<String,String> configs = getConfigs();
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerServiceStorm.lookupResource Context: (" + context + ")");
		}
		if (context != null) {
			try {
				ret  = StormResourceMgr.getStormResources(serviceName,configs,context);
						
			} catch (Exception e) {
			  LOG.error( "<==RangerServiceStorm.lookupResource Error : " + e);
			  throw e;
			}
		}
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerServiceStorm.lookupResource Response: (" + ret + ")");
		}
		return ret;
	}
}
