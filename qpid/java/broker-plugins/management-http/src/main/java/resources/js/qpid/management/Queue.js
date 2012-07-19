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
        "dijit/registry",
        "dojo/_base/connect",
        "dojo/_base/event",
        "dojo/json",
        "qpid/common/properties",
        "qpid/common/updater",
        "qpid/common/util",
        "qpid/common/formatter",
        "qpid/common/UpdatableStore",
        "qpid/management/addBinding",
        "qpid/management/moveCopyMessages",
        "qpid/management/showMessage",
        "dojo/store/JsonRest",
        "dojox/grid/EnhancedGrid",
        "dojo/data/ObjectStore",
        "dojox/grid/enhanced/plugins/Pagination",
        "dojox/grid/enhanced/plugins/IndirectSelection",
        "dojo/domReady!"],
       function (xhr, parser, query, registry, connect, event, json, properties, updater, util, formatter,
                 UpdatableStore, addBinding, moveMessages, showMessage, JsonRest, EnhancedGrid, ObjectStore) {

           function Queue(name, parent, controller) {
               this.name = name;
               this.controller = controller;
               this.modelObj = { type: "queue", name: name };
               if(parent) {
                   this.modelObj.parent = {};
                   this.modelObj.parent[ parent.type] = parent;
               }
           }

           Queue.prototype.getQueueName = function()
           {
               return this.name;
           };


           Queue.prototype.getVirtualHostName = function()
           {
               return this.modelObj.parent.virtualhost.name;
           };

           Queue.prototype.getTitle = function()
           {
               return "Queue: " + this.name;
           };

           Queue.prototype.open = function(contentPane) {
               var that = this;
               this.contentPane = contentPane;
               xhr.get({url: "showQueue.html",
                        sync: true,
                        load:  function(data) {
                            contentPane.containerNode.innerHTML = data;
                            parser.parse(contentPane.containerNode);

                            that.queueUpdater = new QueueUpdater(contentPane.containerNode, that, that.controller);

                            updater.add( that.queueUpdater );

                            that.queueUpdater.update();

                            var myStore = new JsonRest({target:"rest/message/"+ encodeURIComponent(that.getVirtualHostName()) +
                                                                               "/" + encodeURIComponent(that.getQueueName())});
                            var messageGridDiv = query(".messages",contentPane.containerNode)[0];
                            that.dataStore = new ObjectStore({objectStore: myStore});
                            that.grid = new EnhancedGrid({
                                store: that.dataStore,
                                autoHeight: 10,
                                keepSelection: true,
                                structure: [
                                    {name:"Size", field:"size", width: "60px"},
                                    {name:"State", field:"state", width: "120px"},

                                    {name:"Arrival", field:"arrivalTime", width: "100%",
                                        formatter: function(val) {
                                            var d = new Date(0);
                                            d.setUTCSeconds(val/1000);

                                            return d.toLocaleString();
                                        } }
                                ],
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
                                }
                            }, messageGridDiv);

                            connect.connect(that.grid, "onRowDblClick", that.grid,
                                             function(evt){
                                                 var idx = evt.rowIndex,
                                                     theItem = this.getItem(idx);
                                                 var id = that.dataStore.getValue(theItem,"id");
                                                 showMessage.show({ messageNumber: id,
                                                                    queue: that.getQueueName(),
                                                                    virtualhost: that.getVirtualHostName() });
                                             });

                            var deleteMessagesButton = query(".deleteMessagesButton", contentPane.containerNode)[0];
                            var deleteWidget = registry.byNode(deleteMessagesButton);
                            connect.connect(deleteWidget, "onClick",
                                            function(evt){
                                                event.stop(evt);
                                                that.deleteMessages();
                                            });
                            var moveMessagesButton = query(".moveMessagesButton", contentPane.containerNode)[0];
                            connect.connect(registry.byNode(moveMessagesButton), "onClick",
                                            function(evt){
                                                event.stop(evt);
                                                that.moveOrCopyMessages({move: true});
                                            });


                            var copyMessagesButton = query(".copyMessagesButton", contentPane.containerNode)[0];
                            connect.connect(registry.byNode(copyMessagesButton), "onClick",
                                            function(evt){
                                                event.stop(evt);
                                                that.moveOrCopyMessages({move: false});
                                            });

                            var addBindingButton = query(".addBindingButton", contentPane.containerNode)[0];
                            connect.connect(registry.byNode(addBindingButton), "onClick",
                                            function(evt){
                                                event.stop(evt);
                                                addBinding.show({ virtualhost: that.getVirtualHostName(),
                                                                  queue: that.getQueueName()});
                                            });

                        }});



           };

           Queue.prototype.deleteMessages = function() {
               var data = this.grid.selection.getSelected();
               if(data.length) {
                   var that = this;
                   if(confirm("Delete " + data.length + " messages?")) {
                       var i, queryParam;
                       for(i = 0; i<data.length; i++) {
                           if(queryParam) {
                               queryParam += "&";
                           } else {
                               queryParam = "?";
                           }

                           queryParam += "id=" + data[i].id;
                       }
                       var query = "rest/message/"+ encodeURIComponent(that.getVirtualHostName())
                           + "/" + encodeURIComponent(that.getQueueName()) + queryParam;
                       that.success = true
                       xhr.del({url: query, sync: true, handleAs: "json"}).then(
                           function(data) {
                               that.grid.setQuery({id: "*"});
                               that.grid.selection.deselectAll();
                               that.queueUpdater.update();
                           },
                           function(error) {that.success = false; that.failureReason = error;});
                        if(!that.success ) {
                            alert("Error:" + this.failureReason);
                        }
                   }
               }
           };

           Queue.prototype.moveOrCopyMessages = function(obj) {
               var that = this;
               var move = obj.move;
               var data = this.grid.selection.getSelected();
               if(data.length) {
                   var that = this;
                   var i, putData = { messages:[] };
                   if(move) {
                       putData.move = true;
                   }
                   for(i = 0; i<data.length; i++) {
                       putData.messages.push(data[i].id);
                   }
                   moveMessages.show({ virtualhost: this.getVirtualHostName(),
                                       queue: this.getQueueName(),
                                       data: putData}, function() {
                                         if(move)
                                         {
                                            that.grid.setQuery({id: "*"});
                                            that.grid.selection.deselectAll();
                                         }
                                     });

               }



           };

           Queue.prototype.startup = function() {
               this.grid.startup();
           };

           Queue.prototype.close = function() {
               updater.remove( this.queueUpdater );
           };

           var queueTypeKeys = {
                   priority: "priorities",
                   lvq: "lvqKey",
                   sorted: "sortKey"
               };

           var queueTypeKeyNames = {
                   priority: "Number of priorities",
                   lvq: "LVQ key",
                   sorted: "Sort key"
               };

           function QueueUpdater(containerNode, queueObj, controller)
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
                           "type",
                           "keyName",
                           "keyValue",
                           "alertRepeatGap",
                           "alertRepeatGapUnits",
                           "alertThresholdMessageAge",
                           "alertThresholdMessageAgeUnits",
                           "alertThresholdMessageSize",
                           "alertThresholdMessageSizeUnits",
                           "alertThresholdQueueDepthBytes",
                           "alertThresholdQueueDepthBytesUnits",
                           "alertThresholdQueueDepthMessages",
                           "queueDepthMessages",
                           "queueDepthBytes",
                           "queueDepthBytesUnits",
                           "unacknowledgedMessages",
                           "unacknowledgedBytes",
                           "unacknowledgedBytesUnits",
                           "msgInRate",
                           "bytesInRate",
                           "bytesInRateUnits",
                           "msgOutRate",
                           "bytesOutRate",
                           "bytesOutRateUnits"]);



               this.query = "rest/queue/"+ encodeURIComponent(queueObj.getVirtualHostName()) + "/" + encodeURIComponent(queueObj.getQueueName());

               xhr.get({url: this.query, sync: properties.useSyncGet, handleAs: "json"}).then(function(data)
                               {
                                   that.queueData = data[0];

                                   util.flattenStatistics( that.queueData );

                                   that.updateHeader();
                                   that.bindingsGrid = new UpdatableStore(that.queueData.bindings, findNode("bindings"),
                                                            [ { name: "Exchange",    field: "exchange",      width: "90px"},
                                                              { name: "Binding Key", field: "name",          width: "120px"},
                                                              { name: "Arguments",   field: "argumentString",     width: "100%"}
                                                            ]);

                                   that.consumersGrid = new UpdatableStore(that.queueData.consumers, findNode("consumers"),
                                                            [ { name: "Name",    field: "name",      width: "70px"},
                                                              { name: "Mode", field: "distributionMode", width: "70px"},
                                                              { name: "Msgs Rate", field: "msgRate",
                                                              width: "150px"},
                                                              { name: "Bytes Rate", field: "bytesRate",
                                                                 width: "100%"}
                                                            ]);




                               });

           }

           QueueUpdater.prototype.updateHeader = function()
           {

               var bytesDepth;
               this.name.innerHTML = this.queueData[ "name" ];
               this.state.innerHTML = this.queueData[ "state" ];
               this.durable.innerHTML = this.queueData[ "durable" ];
               this.lifetimePolicy.innerHTML = this.queueData[ "lifetimePolicy" ];
               this.type.innerHTML = this.queueData[ "type" ];

               this.queueDepthMessages.innerHTML = this.queueData["queueDepthMessages"];
               bytesDepth = formatter.formatBytes( this.queueData["queueDepthBytes"] );
               this.queueDepthBytes.innerHTML = "(" + bytesDepth.value;
               this.queueDepthBytesUnits.innerHTML = bytesDepth.units + ")";

               this.unacknowledgedMessages.innerHTML = this.queueData["unacknowledgedMessages"];
               bytesDepth = formatter.formatBytes( this.queueData["unacknowledgedBytes"] );
               this.unacknowledgedBytes.innerHTML = "(" + bytesDepth.value;
               this.unacknowledgedBytesUnits.innerHTML = bytesDepth.units + ")";
               if (this.queueData.type == "standard")
               {
                   this.keyName.style.display = "none";
                   this.keyValue.style.display = "none";
               }
               else
               {
                   this.keyName.innerHTML = queueTypeKeyNames[this.queueData.type] + ":";
                   this.keyValue.innerHTML = this.queueData[queueTypeKeys[this.queueData.type]];
               }

           };

           QueueUpdater.prototype.update = function()
           {

               var thisObj = this;

               xhr.get({url: this.query, sync: properties.useSyncGet, handleAs: "json"}).then(function(data) {
                       var i,j;
                       thisObj.queueData = data[0];
                       util.flattenStatistics( thisObj.queueData );

                       var bindings = thisObj.queueData[ "bindings" ];
                       var consumers = thisObj.queueData[ "consumers" ];

                       for(i=0; i < bindings.length; i++) {
                           bindings[i].argumentString = json.stringify(bindings[i].arguments);
                       }

                       thisObj.updateHeader();


                       // update alerting info
                       var alertRepeatGap = formatter.formatTime( thisObj.queueData["alertRepeatGap"] );

                       thisObj.alertRepeatGap.innerHTML = alertRepeatGap.value;
                       thisObj.alertRepeatGapUnits.innerHTML = alertRepeatGap.units;


                       var alertMsgAge = formatter.formatTime( thisObj.queueData["alertThresholdMessageAge"] );

                       thisObj.alertThresholdMessageAge.innerHTML = alertMsgAge.value;
                       thisObj.alertThresholdMessageAgeUnits.innerHTML = alertMsgAge.units;

                       var alertMsgSize = formatter.formatBytes( thisObj.queueData["alertThresholdMessageSize"] );

                       thisObj.alertThresholdMessageSize.innerHTML = alertMsgSize.value;
                       thisObj.alertThresholdMessageSizeUnits.innerHTML = alertMsgSize.units;

                       var alertQueueDepth = formatter.formatBytes( thisObj.queueData["alertThresholdQueueDepthBytes"] );

                       thisObj.alertThresholdQueueDepthBytes.innerHTML = alertQueueDepth.value;
                       thisObj.alertThresholdQueueDepthBytesUnits.innerHTML = alertQueueDepth.units;

                       thisObj.alertThresholdQueueDepthMessages.innerHTML = thisObj.queueData["alertThresholdQueueDepthMessages"];

                       var sampleTime = new Date();
                       var messageIn = thisObj.queueData["totalEnqueuedMessages"];
                       var bytesIn = thisObj.queueData["totalEnqueuedBytes"];
                       var messageOut = thisObj.queueData["totalDequeuedMessages"];
                       var bytesOut = thisObj.queueData["totalDequeuedBytes"];

                       if(thisObj.sampleTime) {
                           var samplePeriod = sampleTime.getTime() - thisObj.sampleTime.getTime();

                           var msgInRate = (1000 * (messageIn - thisObj.messageIn)) / samplePeriod;
                           var msgOutRate = (1000 * (messageOut - thisObj.messageOut)) / samplePeriod;
                           var bytesInRate = (1000 * (bytesIn - thisObj.bytesIn)) / samplePeriod;
                           var bytesOutRate = (1000 * (bytesOut - thisObj.bytesOut)) / samplePeriod;

                           thisObj.msgInRate.innerHTML = msgInRate.toFixed(0);
                           var bytesInFormat = formatter.formatBytes( bytesInRate );
                           thisObj.bytesInRate.innerHTML = "(" + bytesInFormat.value;
                           thisObj.bytesInRateUnits.innerHTML = bytesInFormat.units + "/s)";

                           thisObj.msgOutRate.innerHTML = msgOutRate.toFixed(0);
                           var bytesOutFormat = formatter.formatBytes( bytesOutRate );
                           thisObj.bytesOutRate.innerHTML = "(" + bytesOutFormat.value;
                           thisObj.bytesOutRateUnits.innerHTML = bytesOutFormat.units + "/s)";

                           if(consumers && thisObj.consumers) {
                               for(i=0; i < consumers.length; i++) {
                                   var consumer = consumers[i];
                                   for(j = 0; j < thisObj.consumers.length; j++) {
                                       var oldConsumer = thisObj.consumers[j];
                                       if(oldConsumer.id == consumer.id) {
                                           var msgRate = (1000 * (consumer.messagesOut - oldConsumer.messagesOut)) /
                                                           samplePeriod;
                                           consumer.msgRate = msgRate.toFixed(0) + "msg/s";

                                           var bytesRate = (1000 * (consumer.bytesOut - oldConsumer.bytesOut)) /
                                                           samplePeriod;
                                           var bytesRateFormat = formatter.formatBytes( bytesRate );
                                           consumer.bytesRate = bytesRateFormat.value + bytesRateFormat.units + "/s";
                                       }
                                   }
                               }
                           }

                       }

                      thisObj.sampleTime = sampleTime;
                      thisObj.messageIn = messageIn;
                      thisObj.bytesIn = bytesIn;
                      thisObj.messageOut = messageOut;
                      thisObj.bytesOut = bytesOut;
                      thisObj.consumers = consumers;

                      // update bindings
                      thisObj.bindingsGrid.update(thisObj.queueData.bindings);

                      // update consumers
                      thisObj.consumersGrid.update(thisObj.queueData.consumers)

                   });
           };


           return Queue;
       });
