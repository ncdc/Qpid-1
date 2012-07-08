/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

define(["dojo/_base/xhr",
        "dojo/dom",
        "dojo/dom-construct",
        "dojo/dom-class",
        "dojo/_base/window",
        "dijit/registry",
        "dojo/parser",
        "dojo/_base/array",
        "dojo/_base/event",
        'dojo/_base/json',
        "dojo/query",
        "dojo/_base/connect",
        "qpid/common/properties",
        "dojox/html/entities",
        "dojo/domReady!"],
    function (xhr, dom, construct, domClass, win, registry, parser, array, event, json, query, connect, properties, entities) {


        function encode(val){
            return typeof val === 'string' ? entities.encode(val) : val;
        }

        var showMessage = {};

        showMessage.hide = function () {
            if(this.populatedFields) {
                for(var i = 0 ; i < this.populatedFields.length; i++) {
                    this.populatedFields[i].innerHTML = "";
                }
                this.populatedFields = [];
            }
            registry.byId("showMessage").hide();
        };

        showMessage.loadViewMessage = function(data) {
            var that = this;
            node.innerHTML = data;
            showMessage.dialogNode = dom.byId("showMessage");
            parser.instantiate([showMessage.dialogNode]);

            var closeButton = query(".closeViewMessage")[0];
            connect.connect(closeButton, "onclick",
                            function (evt) {
                                event.stop(evt);
                                showMessage.hide();
                            });
        };

        showMessage.populateShowMessage = function(data) {

            this.populatedFields = [];

            for(var attrName in data) {
                if(data.hasOwnProperty(attrName)) {
                    var fields = query(".message-"+attrName, this.dialogNode);
                    if(fields && fields.length != 0) {
                        var field = fields[0];
                        this.populatedFields.push(field);
                        var val = data[attrName];
                        if(val) {
                            if(domClass.contains(field,"map")) {
                                var tableStr = "<table style='border: 1pt'><tr><th style='width: 6em; font-weight: bold'>Header</th><th style='font-weight: bold'>Value</th></tr>";
                                for(var name in val) {
                                    if(val.hasOwnProperty(name)) {

                                        tableStr += "<tr><td>"+encode(name)+"</td>";
                                        tableStr += "<td>"+encode(val[ name ])+"</td></tr>";
                                    }
                                    field.innerHTML = tableStr;
                                }
                                tableStr += "</table>";
                            } else if(domClass.contains(field,"datetime")) {
                                var d = new Date(0);
                                d.setUTCSeconds(val/1000);
                                field.innerHTML = d.toLocaleString();
                            } else {
                                field.innerHTML = encode(val);
                            }
                        }
                    }
                }
            }
            var contentField = query(".message-content", this.dialogNode)[0];

            if(data.mimeType && data.mimeType.match(/text\/.*/)) {
                xhr.get({url: "rest/message-content/" + encodeURIComponent(showMessage.virtualhost)
                                            + "/" + encodeURIComponent(showMessage.queue)
                                            + "/" + encodeURIComponent(showMessage.messageNumber),
                                     sync: true

                                    }).then(function(obj) { contentField.innerHTML = encode(obj) });
            } else {
                contentField.innerHTML = "<a href=\"" + "rest/message-content/" + encodeURIComponent(showMessage.virtualhost)
                                                            + "/" + encodeURIComponent(showMessage.queue)
                                                            + "/" + encodeURIComponent(showMessage.messageNumber)
                                        + "\" target=\"_blank\">Download</a>";
            }
            this.populatedFields.push(contentField);

            registry.byId("showMessage").show();
        };

        showMessage.show = function(obj) {
            showMessage.virtualhost = obj.virtualhost;
            showMessage.queue = obj.queue;
            showMessage.messageNumber = obj.messageNumber;

            xhr.get({url: "rest/message/" + encodeURIComponent(obj.virtualhost)
                            + "/" + encodeURIComponent(obj.queue)
                            + "/" + encodeURIComponent(obj.messageNumber),
                     sync: properties.useSyncGet,
                     handleAs: "json",
                     load: this.populateShowMessage
                    });
        };

        var node = construct.create("div", null, win.body(), "last");

        xhr.get({url: "showMessage.html",
                 sync: true,
                 load: showMessage.loadViewMessage
                });

        return showMessage;
    });
