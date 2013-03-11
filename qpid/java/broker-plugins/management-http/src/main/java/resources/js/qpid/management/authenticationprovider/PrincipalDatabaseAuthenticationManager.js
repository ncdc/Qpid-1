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
define(["dojo/_base/xhr",
        "dojo/dom",
        "dojo/parser",
        "dojo/query",
        "dojo/dom-construct",
        "dojo/_base/connect",
        "dojo/_base/window",
        "dojo/_base/event",
        "dojo/_base/json",
        "dijit/registry",
        "qpid/common/util",
        "qpid/common/properties",
        "qpid/common/updater",
        "qpid/common/UpdatableStore",
        "dojox/grid/EnhancedGrid",
        "dojox/grid/enhanced/plugins/Pagination",
        "dojox/grid/enhanced/plugins/IndirectSelection",
        "dojox/validate/us", "dojox/validate/web",
        "dijit/Dialog",
        "dijit/form/TextBox",
        "dijit/form/ValidationTextBox",
        "dijit/form/TimeTextBox", "dijit/form/Button",
        "dijit/form/Form",
        "dijit/form/DateTextBox",
        "dojo/domReady!"],
    function (xhr, dom, parser, query, construct, connect, win, event, json, registry, util, properties, updater, UpdatableStore, EnhancedGrid) {
        function DatabaseAuthManager(containerNode, authProviderObj, controller, authenticationManagerUpdater) {
            var node = construct.create("div", null, containerNode, "last");
            var that = this;
            this.name = authProviderObj.name;
            xhr.get({url: "authenticationprovider/showPrincipalDatabaseAuthenticationManager.html",
                                    sync: true,
                                    load:  function(data) {
                                        node.innerHTML = data;
                                        parser.parse(node);


                                        that.authDatabaseUpdater= new AuthProviderUpdater(node, authProviderObj, controller, authenticationManagerUpdater);
                                        updater.add( that.authDatabaseUpdater);

                                        that.authDatabaseUpdater.update();


                                    }});
        }

        DatabaseAuthManager.prototype.update = function() {
            this.authDatabaseUpdater.update();
        };

        DatabaseAuthManager.prototype.close = function() {
            updater.remove( this.authDatabaseUpdater );
        };

        function AuthProviderUpdater(node, authProviderObj, controller, authenticationManagerUpdater)
        {
            this.controller = controller;
            this.query = "rest/authenticationprovider?id="+encodeURIComponent(authProviderObj.id);
            this.name = authProviderObj.name;
            this.authenticationManagerUpdater = authenticationManagerUpdater;
            var that = this;

            xhr.get({url: this.query, sync: properties.useSyncGet, handleAs: "json"})
               .then(function(data) {
                     that.authProviderData = data[0];
                     that.name = data[0].name
                     util.flattenStatistics( that.authProviderData );

                     var userDiv = query(".users")[0];

                     var gridProperties = {
                                            height: 400,
                                            keepSelection: true,
                                            plugins: {
                                                      pagination: {
                                                          pageSizes: ["10", "25", "50", "100"],
                                                          description: true,
                                                          sizeSwitch: true,
                                                          pageStepper: true,
                                                          gotoButton: true,
                                                          maxPageStep: 4,
                                                          position: "bottom"
                                                      },
                                                      indirectSelection: true

                                             }};


                     that.usersGrid =
                        new UpdatableStore(that.authProviderData.users, userDiv,
                                        [ { name: "User Name",    field: "name",      width: "100%" }
                                        ], function(obj) {
                                                connect.connect(obj.grid, "onRowDblClick", obj.grid,
                                                function(evt){
                                                    var idx = evt.rowIndex,
                                                    theItem = this.getItem(idx);
                                                    var name = obj.dataStore.getValue(theItem,"name");
                                                    var id = obj.dataStore.getValue(theItem,"id");
                                                    setPassword.show(that.name, {name: name, id: id});
                                                });
                                        }, gridProperties, EnhancedGrid);


                     var addUserButton = query(".addUserButton", node)[0];
                     connect.connect(registry.byNode(addUserButton), "onClick", function(evt){ addUser.show(that.name) });

                     var deleteMessagesButton = query(".deleteUserButton", node)[0];
                                                var deleteWidget = registry.byNode(deleteMessagesButton);
                                                connect.connect(deleteWidget, "onClick",
                                                                function(evt){
                                                                    event.stop(evt);
                                                                    that.deleteUsers();
                                                                });
                });
        }

        AuthProviderUpdater.prototype.deleteUsers = function()
        {
            var grid = this.usersGrid.grid;
            var data = grid.selection.getSelected();
            if(data.length) {
                var that = this;
                if(confirm("Delete " + data.length + " users?")) {
                    var i, queryParam;
                    for(i = 0; i<data.length; i++) {
                        if(queryParam) {
                            queryParam += "&";
                        } else {
                            queryParam = "?";
                        }

                        queryParam += "id=" + data[i].id;
                    }
                    var query = "rest/user/"+ encodeURIComponent(that.name)
                       + queryParam;
                    that.success = true
                    xhr.del({url: query, sync: true, handleAs: "json"}).then(
                        function(data) {
                            grid.setQuery({id: "*"});
                            grid.selection.deselectAll();
                            that.update();
                        },
                        function(error) {that.success = false; that.failureReason = error;});
                    if(!that.success ) {
                        alert("Error:" + this.failureReason);
                    }
                }
}
        };

        AuthProviderUpdater.prototype.update = function()
        {

            var that = this;

            xhr.get({url: this.query, sync: properties.useSyncGet, handleAs: "json"})
                .then(function(data) {
                    that.authProviderData = data[0];
                    that.name = data[0].name
                    util.flattenStatistics( that.authProviderData );

                    that.usersGrid.update(that.authProviderData.users);

                    that.authenticationManagerUpdater.authProviderData = data[0];
                    that.authenticationManagerUpdater.updateHeader();
                });


        };

        var addUser = {};

        var node = construct.create("div", null, win.body(), "last");

        var convertToUser = function convertToUser(formValues) {
                var newUser = {};
                newUser.name = formValues.name;
                for(var propName in formValues)
                {
                    if(formValues.hasOwnProperty(propName)) {
                            if(formValues[ propName ] !== "") {
                                newUser[ propName ] = formValues[propName];
                            }
                    }
                }

                return newUser;
            };


        xhr.get({url: "authenticationprovider/addUser.html",
                 sync: true,
                 load:  function(data) {
                            var theForm;
                            node.innerHTML = data;
                            addUser.dialogNode = dom.byId("addUser");
                            parser.instantiate([addUser.dialogNode]);

                            var that = this;

                            theForm = registry.byId("formAddUser");
                            theForm.on("submit", function(e) {

                                event.stop(e);
                                if(theForm.validate()){

                                    var newUser = convertToUser(theForm.getValues());


                                    var url = "rest/user/"+encodeURIComponent(addUser.authProvider) +
                                        "/"+encodeURIComponent(newUser.name);

                                    xhr.put({url: url, sync: true, handleAs: "json",
                                             headers: { "Content-Type": "application/json"},
                                             putData: json.toJson(newUser),
                                             load: function(x) {that.success = true; },
                                             error: function(error) {that.success = false; that.failureReason = error;}});

                                    if(that.success === true) {
                                        registry.byId("addUser").hide();
                                    } else {
                                        alert("Error:" + that.failureReason);
                                    }

                                    return false;


                                }else{
                                    alert('Form contains invalid data.  Please correct first');
                                    return false;
                                }

                            });
                        }});

        addUser.show = function(authProvider) {
            addUser.authProvider = authProvider;
            registry.byId("formAddUser").reset();
            registry.byId("addUser").show();
        };


        var setPassword = {};

        var setPasswordNode = construct.create("div", null, win.body(), "last");

        xhr.get({url: "authenticationprovider/setPassword.html",
                 sync: true,
                 load:  function(data) {
                    var theForm;
                    setPasswordNode.innerHTML = data;
                    setPassword.dialogNode = dom.byId("setPassword");
                    parser.instantiate([setPassword.dialogNode]);

                    var that = this;

                    theForm = registry.byId("formSetPassword");
                    theForm.on("submit", function(e) {

                        event.stop(e);
                        if(theForm.validate()){

                            var newUser = convertToUser(theForm.getValues());
                            newUser.name = setPassword.name;
                            newUser.id = setPassword.id;

                            var url = "rest/user/"+encodeURIComponent(setPassword.authProvider) +
                                "/"+encodeURIComponent(newUser.name);

                            xhr.put({url: url, sync: true, handleAs: "json",
                                     headers: { "Content-Type": "application/json"},
                                     putData: json.toJson(newUser),
                                     load: function(x) {that.success = true; },
                                     error: function(error) {that.success = false; that.failureReason = error;}});

                            if(that.success === true) {
                                registry.byId("setPassword").hide();
                            } else {
                                alert("Error:" + that.failureReason);
                            }

                            return false;


                        }else{
                            alert('Form contains invalid data.  Please correct first');
                            return false;
                        }

                    });
                }});

        setPassword.show = function(authProvider, user) {
            setPassword.authProvider = authProvider;
            setPassword.name = user.name;
            setPassword.id = user.id;
            registry.byId("formSetPassword").reset();

            var namebox = registry.byId("formSetPassword.name");
            namebox.set("value", user.name);
            namebox.set("disabled", true);

            registry.byId("setPassword").show();

        };



        return DatabaseAuthManager;
    });
