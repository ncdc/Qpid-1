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
        "dojo/parser",
        "dojo/query",
        "dojo/_base/connect",
        "qpid/common/properties",
        "qpid/common/updater",
        "qpid/common/util",
        "qpid/common/formatter",
        "qpid/common/UpdatableStore",
        "dojo/domReady!"],
       function (xhr, parser, query, connect, properties, updater, util, formatter, UpdatableStore) {

           function Connection(name, parent, controller) {
               this.name = name;
               this.controller = controller;
               this.modelObj = { type: "exchange", name: name };
               if(parent) {
                   this.modelObj.parent = {};
                   this.modelObj.parent[ parent.type] = parent;
               }
           }

           Connection.prototype.getTitle = function()
           {
               return "Connection: " + this.name;
           };

           Connection.prototype.open = function(contentPane) {
               var that = this;
               this.contentPane = contentPane;
               xhr.get({url: "showConnection.html",
                        sync: true,
                        load:  function(data) {
                            contentPane.containerNode.innerHTML = data;
                            parser.parse(contentPane.containerNode);

                            that.connectionUpdater = new ConnectionUpdater(contentPane.containerNode, that.modelObj, that.controller);

                            updater.add( that.connectionUpdater );

                            that.connectionUpdater.update();

                        }});
           };

           Connection.prototype.close = function() {
               updater.remove( this.connectionUpdater );
           };

           function ConnectionUpdater(containerNode, connectionObj, controller)
           {
               var that = this;

               function findNode(name) {
                   return query("." + name, containerNode)[0];
               }

               function storeNodes(names)
               {
                  for(var i = 0; i < names.length; i++) {
                      that[names[i]] = findNode(names[i]);
                  }
               }

               storeNodes(["name",
                           "state",
                           "durable",
                           "lifetimePolicy",
                           "msgInRate",
                           "bytesInRate",
                           "bytesInRateUnits",
                           "msgOutRate",
                           "bytesOutRate",
                           "bytesOutRateUnits"]);



               this.query = "rest/connection/"+ encodeURIComponent(connectionObj.parent.virtualhost.name) + "/" + encodeURIComponent(connectionObj.name);

               xhr.get({url: this.query, sync: properties.useSyncGet, handleAs: "json"}).then(function(data)
                               {
                                   that.connectionData = data[0];

                                   util.flattenStatistics( that.connectionData );

                                   that.updateHeader();
                                   that.sessionsGrid = new UpdatableStore(that.connectionData.sessions, findNode("sessions"),
                                                            [ { name: "Name",    field: "name",      width: "70px"},
                                                              { name: "Mode", field: "distributionMode", width: "70px"},
                                                              { name: "Msgs Rate", field: "msgRate",
                                                              width: "150px"},
                                                              { name: "Bytes Rate", field: "bytesRate",
                                                                 width: "100%"}
                                                            ]);


                               });

           }

           ConnectionUpdater.prototype.updateHeader = function()
           {
              this.name.innerHTML = this.connectionData[ "name" ];
              this.state.innerHTML = this.connectionData[ "state" ];
              this.durable.innerHTML = this.connectionData[ "durable" ];
              this.lifetimePolicy.innerHTML = this.connectionData[ "lifetimePolicy" ];

           };

           ConnectionUpdater.prototype.update = function()
           {

              var that = this;

              xhr.get({url: this.query, sync: properties.useSyncGet, handleAs: "json"}).then(function(data)
                   {
                       that.connectionData = data[0];

                       util.flattenStatistics( that.connectionData );

                       var sessions = that.connectionData[ "sessions" ];

                       that.updateHeader();

                       var sampleTime = new Date();
                       var messageIn = that.connectionData["messagesIn"];
                       var bytesIn = that.connectionData["bytesIn"];
                       var messageOut = that.connectionData["messagesOut"];
                       var bytesOut = that.connectionData["bytesOut"];

                       if(that.sampleTime)
                       {
                           var samplePeriod = sampleTime.getTime() - that.sampleTime.getTime();

                           var msgInRate = (1000 * (messageIn - that.messageIn)) / samplePeriod;
                           var msgOutRate = (1000 * (messageOut - that.messageOut)) / samplePeriod;
                           var bytesInRate = (1000 * (bytesIn - that.bytesIn)) / samplePeriod;
                           var bytesOutRate = (1000 * (bytesOut - that.bytesOut)) / samplePeriod;

                           that.msgInRate.innerHTML = msgInRate.toFixed(0);
                           var bytesInFormat = formatter.formatBytes( bytesInRate );
                           that.bytesInRate.innerHTML = "(" + bytesInFormat.value;
                           that.bytesInRateUnits.innerHTML = bytesInFormat.units + "/s)";

                           that.msgOutRate.innerHTML = msgOutRate.toFixed(0);
                           var bytesOutFormat = formatter.formatBytes( bytesOutRate );
                           that.bytesOutRate.innerHTML = "(" + bytesOutFormat.value;
                           that.bytesOutRateUnits.innerHTML = bytesOutFormat.units + "/s)";

                           if(sessions && that.sessions)
                           {
                               for(var i=0; i < sessions.length; i++)
                               {
                                   var session = sessions[i];
                                   for(var j = 0; j < that.sessions.length; j++)
                                   {
                                       var oldSession = that.sessions[j];
                                       if(oldSession.id == session.id)
                                       {
                                           var msgRate = (1000 * (session.messagesOut - oldSession.messagesOut)) /
                                                           samplePeriod;
                                           session.msgRate = msgRate.toFixed(0) + "msg/s";

                                           var bytesRate = (1000 * (session.bytesOut - oldSession.bytesOut)) /
                                                           samplePeriod;
                                           var bytesRateFormat = formatter.formatBytes( bytesRate );
                                           session.bytesRate = bytesRateFormat.value + bytesRateFormat.units + "/s";
                                       }


                                   }

                               }
                           }

                       }

                       that.sampleTime = sampleTime;
                       that.messageIn = messageIn;
                       that.bytesIn = bytesIn;
                       that.messageOut = messageOut;
                       that.bytesOut = bytesOut;
                       that.sessions = sessions;


                       // update sessions
                       that.sessionsGrid.update(that.connectionData.sessions)
                   });
           };


           return Connection;
       });