/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.qpid.server.management.plugin.servlet.rest;

import java.io.IOException;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.apache.qpid.server.management.plugin.Management;

@SuppressWarnings("serial")
public class LogoutServlet extends HttpServlet
{
    public static final String RETURN_URL_INIT_PARAM = "qpid.webui_logout_redirect";
    private String _returnUrl = Management.ENTRY_POINT_PATH;

    @Override
    public void init(ServletConfig config) throws ServletException
    {
        super.init(config);

        String initValue = config.getServletContext().getInitParameter(RETURN_URL_INIT_PARAM);
        if(initValue != null)
        {
            _returnUrl = initValue;
        }
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse resp) throws ServletException, IOException
    {
        HttpSession session = request.getSession(false);
        if(session != null)
        {
            // Invalidating the session will cause LoginLogoutReporter to log the user logoff.
            session.invalidate();
        }

        resp.sendRedirect(_returnUrl);
    }

}
