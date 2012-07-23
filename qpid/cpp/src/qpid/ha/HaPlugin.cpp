/*
 *
 * Copyright (c) 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
#include "HaBroker.h"
#include "Settings.h"
#include "qpid/Plugin.h"
#include "qpid/Options.h"
#include "qpid/broker/Broker.h"
#include <boost/bind.hpp>

namespace qpid {
namespace ha {

using namespace std;

struct Options : public qpid::Options {
    Settings& settings;
    Options(Settings& s) : qpid::Options("HA Options"), settings(s) {
        addOptions()
            ("ha-cluster", optValue(settings.cluster, "yes|no"),
             "Join a HA active/passive cluster.")
            ("ha-brokers-url", optValue(settings.brokerUrl,"URL"),
             "URL with address of each broker in the cluster.")
            ("ha-public-url", optValue(settings.clientUrl,"URL"),
             "URL advertized to clients to connect to the cluster.")
            ("ha-replicate",
             optValue(settings.replicateDefault, "LEVEL"),
            "Replication level for creating queues and exchanges if there is no qpid.replicate argument supplied. LEVEL is 'none', 'configuration' or 'all'")
            ("ha-username", optValue(settings.username, "USER"),
             "Username for connections between HA brokers")
            ("ha-password", optValue(settings.password, "PASS"),
             "Password for connections between HA brokers")
            ("ha-mechanism", optValue(settings.mechanism, "MECH"),
             "Authentication mechanism for connections between HA brokers")
            ("ha-backup-timeout", optValue(settings.backupTimeout, "SECONDS"),
             "Maximum time to wait for an expected backup to connect and become ready.")
            ;
    }
};

struct HaPlugin : public Plugin {

    Settings settings;
    Options options;
    auto_ptr<HaBroker> haBroker;

    HaPlugin() : options(settings) {}

    Options* getOptions() { return &options; }

    void earlyInitialize(Plugin::Target& target) {
        broker::Broker* broker = dynamic_cast<broker::Broker*>(&target);
        if (broker) {
            // Must create the HaBroker in earlyInitialize so it can set up its
            // connection observer before clients start connecting.
            haBroker.reset(new ha::HaBroker(*broker, settings));
            broker->addFinalizer(boost::bind(&HaPlugin::finalize, this));
        }
    }

    void initialize(Plugin::Target& target) {
        broker::Broker* broker = dynamic_cast<broker::Broker*>(&target);
        if (broker) haBroker->initialize();
    }

    void finalize() {
        haBroker.reset();
    }
};

HaPlugin instance;              // Static initialization.

}} // namespace qpid::ha
