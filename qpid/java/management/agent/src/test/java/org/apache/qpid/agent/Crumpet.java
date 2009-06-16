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
package org.apache.qpid.agent;

import java.util.ArrayList;

import org.apache.qpid.agent.annotations.QMFSeeAlso;
import org.apache.qpid.agent.annotations.QMFType;

/**
 * Crumpet
 * 
 */
@QMFType(className = "Crumpet", packageName = "org.apache.test")
@QMFSeeAlso(
{ Pikelet.class })
public class Crumpet
{
    private String foo = "fooValue";
    private String bar = "barValue";
    private ArrayList<String> ingredients = new ArrayList<String>();

    public String getFoo()
    {
        return foo;
    }

    public void setFoo(String foo)
    {
        this.foo = foo;
    }

    public String getBar()
    {
        return bar;
    }

    public void setBar(String bar)
    {
        this.bar = bar;
    }

    public ArrayList<String> getIngredients()
    {
        return ingredients;
    }

    public void setIngredients(ArrayList<String> ingredients)
    {
        this.ingredients = ingredients;
    }
}
