/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 *
 */
package org.apache.qpid.server.management.plugin.servlet.rest;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.qpid.server.management.plugin.servlet.rest.action.ListAuthenticationProviderAttributes;
import org.apache.qpid.server.management.plugin.servlet.rest.action.ListMessageStoreTypes;
import org.apache.qpid.server.model.Broker;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

public class HelperServlet extends AbstractServlet
{
    private static final String PARAM_ACTION = "action";

    private Map<String, Action> _actions;
    private ObjectMapper _mapper;

    public HelperServlet()
    {
        _mapper = new ObjectMapper();
        _mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
        _actions = new HashMap<String, Action>();
        Action listProviderAttributes = new ListAuthenticationProviderAttributes();
        _actions.put(listProviderAttributes.getName(), listProviderAttributes);
        Action listMessageStoreTypes = new ListMessageStoreTypes();
        _actions.put(listMessageStoreTypes.getName(), listMessageStoreTypes);
    }

    @Override
    protected void doGetWithSubjectAndActor(HttpServletRequest request, HttpServletResponse response) throws ServletException,
            IOException
    {
        perform(request, response);
    }

    @Override
    protected void doPostWithSubjectAndActor(HttpServletRequest request, HttpServletResponse response) throws ServletException,
            IOException
    {
        perform(request, response);
    }

    private void perform(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
    {
        String actionName = request.getParameter(PARAM_ACTION);
        Action action = _actions.get(actionName);
        if (action == null)
        {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        Map<String, Object> parameters = new HashMap<String, Object>();
        @SuppressWarnings("unchecked")
        Enumeration<String> names = request.getParameterNames();
        while (names.hasMoreElements())
        {
            String name = (String) names.nextElement();
            String[] values = request.getParameterValues(name);
            if (values.length == 1)
            {
                parameters.put(name, values[0]);
            }
            else
            {
                parameters.put(name, values);
            }
        }

        Object output = action.perform(parameters, (Broker) getServletContext().getAttribute(ATTR_BROKER));
        if (output == null)
        {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return;
        }
        response.setContentType("application/json");
        final Writer writer = new BufferedWriter(response.getWriter());
        _mapper.writeValue(writer, output);

        response.setStatus(HttpServletResponse.SC_OK);

    }
}
