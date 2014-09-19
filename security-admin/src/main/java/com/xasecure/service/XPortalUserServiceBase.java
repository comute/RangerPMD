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

 package com.xasecure.service;

/**
 * 
 */

import java.util.ArrayList;
import java.util.List;

import com.xasecure.common.*;
import com.xasecure.entity.*;
import com.xasecure.view.*;
import com.xasecure.service.*;

public abstract class XPortalUserServiceBase<T extends XXPortalUser, V extends VXPortalUser>
		extends AbstractBaseResourceService<T, V> {
	public static final String NAME = "XPortalUser";

	public XPortalUserServiceBase() {

	}

	@SuppressWarnings("unchecked")
	@Override
	protected XXPortalUser mapViewToEntityBean(VXPortalUser vObj, XXPortalUser mObj, int OPERATION_CONTEXT) {
		mObj.setFirstName( vObj.getFirstName());
		mObj.setLastName( vObj.getLastName());
		mObj.setPublicScreenName( vObj.getPublicScreenName());
		mObj.setLoginId( vObj.getLoginId());
		mObj.setPassword( vObj.getPassword());
		mObj.setEmailAddress( vObj.getEmailAddress());
		mObj.setStatus( vObj.getStatus());
		mObj.setUserSource( vObj.getUserSource());
		mObj.setNotes( vObj.getNotes());
		return mObj;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected VXPortalUser mapEntityToViewBean(VXPortalUser vObj, XXPortalUser mObj) {
		vObj.setFirstName( mObj.getFirstName());
		vObj.setLastName( mObj.getLastName());
		vObj.setPublicScreenName( mObj.getPublicScreenName());
		vObj.setLoginId( mObj.getLoginId());
		vObj.setPassword( mObj.getPassword());
		vObj.setEmailAddress( mObj.getEmailAddress());
		vObj.setStatus( mObj.getStatus());
		vObj.setUserSource( mObj.getUserSource());
		vObj.setNotes( mObj.getNotes());
		return vObj;
	}

	/**
	 * @param searchCriteria
	 * @return
	 */
	public VXPortalUserList searchXPortalUsers(SearchCriteria searchCriteria) {
		VXPortalUserList returnList = new VXPortalUserList();
		List<VXPortalUser> xPortalUserList = new ArrayList<VXPortalUser>();

		@SuppressWarnings("unchecked")
		List<XXPortalUser> resultList = (List<XXPortalUser>)searchResources(searchCriteria,
				searchFields, sortFields, returnList);

		// Iterate over the result list and create the return list
		for (XXPortalUser gjXPortalUser : resultList) {
			@SuppressWarnings("unchecked")
			VXPortalUser vXPortalUser = populateViewBean((T)gjXPortalUser);
			xPortalUserList.add(vXPortalUser);
		}

		returnList.setVXPortalUsers(xPortalUserList);
		return returnList;
	}

}
