// Copyright 2018 The Beam Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "node_client.h"
#include <mutex>
#include <ctime>
#include "pow/external_pow.h"
#include "utility/logger.h"
#include "utility/hex.h"
#include <boost/filesystem.hpp>

namespace
{
    constexpr int kVerificationThreadsMaxAvailable = -1;

    boost::filesystem::path pathFromStdString(const std::string& path)
    {
#ifdef WIN32
        boost::filesystem::path boostPath{ beam::Utf8toUtf16(path.c_str()) };
#else
        boost::filesystem::path boostPath{ path };
#endif
        return boostPath;
    }

    void removeNodeDataIfNeeded(const std::string& nodePathStr)
    {
        try
        {
            auto nodePath = pathFromStdString(nodePathStr);
            auto appDataPath = nodePath.parent_path();

            if (!boost::filesystem::exists(appDataPath) || 
                !boost::filesystem::exists(nodePath))
            {
                return;
            }
            try
            {
                beam::NodeDB nodeDB;
                nodeDB.Open(nodePathStr.c_str());
                return;
            }
            catch (const beam::NodeDBUpgradeException&)
            {
            }

            boost::filesystem::remove(nodePath);

            std::vector<boost::filesystem::path> macroBlockFiles;
            for (boost::filesystem::directory_iterator endDirIt, it{ appDataPath }; it != endDirIt; ++it)
            {
                if (it->path().filename().wstring().find(L"tempmb") == 0)
                {
                    macroBlockFiles.push_back(it->path());
                }
            }

            for (auto& path : macroBlockFiles)
            {
                boost::filesystem::remove(path);
            }
        }
        catch (std::exception& e)
        {
            BEAM_LOG_ERROR() << e.what();
        }
    }
}

namespace beam
{
    NodeClient::NodeClient(const Rules& rules, INodeClientObserver* observer)
        : m_rules(rules)
        , m_observer(observer)
        , m_shouldStartNode(false)
        , m_shouldTerminateModel(false)
        , m_isRunning(false)
    {
    }

    NodeClient::~NodeClient()
    {
        try
        {
            m_shouldTerminateModel = true;
            m_waiting.notify_all();
            {
                {
                    auto r = m_reactor.lock();
                    if (r)
                    {
                        r->stop();
                    }
                }
                {
                    if (m_thread && m_thread->joinable())
                    {
                        // TODO: check this
                        m_thread->join();
                    }
                }
            }
        }
        catch (const std::exception& e)
        {
            BEAM_LOG_UNHANDLED_EXCEPTION() << "what = " << e.what();
        }
        catch (...) {
            BEAM_LOG_UNHANDLED_EXCEPTION();
        }
    }

    void NodeClient::setBeforeStartAction(std::function<void()> action)
    {
        m_beforeStartAction = std::move(action);
    }

    void NodeClient::setKdf(beam::Key::IKdf::Ptr kdf)
    {
        m_pKdf = kdf;
    }

    void NodeClient::setOwnerKey(beam::Key::IPKdf::Ptr key)
    {
        m_ownerKey = key;
    }

    void NodeClient::startNode()
    {
        std::unique_lock<std::mutex> lock(m_startMutex);
        m_shouldStartNode = true;
        m_waiting.notify_one();
    }

    void NodeClient::stopNode()
    {
        {
            std::unique_lock<std::mutex> lock(m_startMutex);
            m_shouldStartNode = false;
            m_waiting.notify_one();
        }
        auto reactor = m_reactor.lock();
        if (reactor)
        {
            reactor->stop();
        }
    }

    void NodeClient::start()
    {
        m_thread = std::make_shared<std::thread>([this]()
        {
            try
            {
                Rules::Scope scopeRules(m_rules);

                if (m_beforeStartAction)
                {
                    m_beforeStartAction();
                }
                removeNodeDataIfNeeded(m_observer->getLocalNodeStorage());
                auto reactor = io::Reactor::create();
                m_reactor = reactor;// store weak ref
                io::Reactor::Scope scope(*reactor);

                while (!m_shouldTerminateModel)
                {
                    {
                        std::unique_lock<std::mutex> lock(m_startMutex);
                        m_waiting.wait(lock, [&]() {return m_shouldStartNode || m_shouldTerminateModel; });
                        m_shouldStartNode = false;
                    }

                    if (!m_shouldTerminateModel)
                    {
                        bool bErr = true;
                        bool recreate = false;
                        try
                        {
                            runLocalNode();
                            bErr = false;
                        }
                        catch (const io::Exception& ex)
                        {
                            BEAM_LOG_ERROR() << ex.what();
                            m_observer->onFailedToStartNode(ex.errorCode);
                            bErr = false;
                            recreate = true;
                        }
                        catch (const std::runtime_error& ex)
                        {
                            BEAM_LOG_ERROR() << ex.what();
                        }
                        catch (const CorruptionException& ex)
                        {
                            BEAM_LOG_ERROR() << "Corruption: " << ex.m_sErr;
                        }

                        if (bErr)
                            m_observer->onSyncError(Node::IObserver::Error::Unknown);

                        if (recreate)
                        {
                            setRecreateTimer(); // attempt to start again
                        }
                    }
                }
            }
            catch (const std::exception& e)
            {
                BEAM_LOG_UNHANDLED_EXCEPTION() << "what = " << e.what();
            }
            // commented intentionally to be able to catch crash
            //catch (...)
            //{
            //    BEAM_LOG_UNHANDLED_EXCEPTION();
            //}

            m_observer->onNodeThreadFinished();
        });
    }

    bool NodeClient::isNodeRunning() const
    {
        return m_isRunning;
    }

    void NodeClient::runLocalNode()
    {
        class ScopedNotifier final {
        public:
            explicit ScopedNotifier(INodeClientObserver& observer)
                : _observer(observer)
                , _nodeCreated(false)
            {}

            void notifyNodeCreated()
            {
                assert(!_nodeCreated);
                _nodeCreated = true;
                _observer.onNodeCreated();
            }

            ~ScopedNotifier()
            {
                if (_nodeCreated)
                {
                    _observer.onNodeDestroyed();
                }
            }
        private:
            INodeClientObserver& _observer;
            bool _nodeCreated = false;
        } notifier(*m_observer);

        // Scope, just for clarity. Notifier created above
        // should be destroyed the last
        {
            Node node;
            node.m_Cfg.m_Listen.port(m_observer->getLocalNodePort());
            node.m_Cfg.m_Listen.ip(INADDR_ANY);
            node.m_Cfg.m_sPathLocal = m_observer->getLocalNodeStorage();
            node.m_Cfg.m_MiningThreads = 0;
            node.m_Cfg.m_VerificationThreads = kVerificationThreadsMaxAvailable;
            node.m_Cfg.m_PeersPersistent = m_observer->getPeersPersistent();

            if (m_pKdf)
            {
                node.m_Keys.SetSingleKey(m_pKdf);
            }
            else if (m_ownerKey)
            {
                node.m_Keys.m_pOwner = m_ownerKey;
            }

            node.m_Cfg.m_Horizon.SetStdFastSync();

            auto peers = m_observer->getLocalNodePeers();

            for (const auto& peer : peers)
            {
                io::Address peer_addr;
                if (peer_addr.resolve(peer.c_str()))
                {
                    node.m_Cfg.m_Connect.emplace_back(peer_addr);
                }
                else
                {
                    BEAM_LOG_ERROR() << "Unable to resolve node address: " << peer;
                }
            }

            BEAM_LOG_INFO() << "starting a node on " << node.m_Cfg.m_Listen.port() << " port...";

            class MyObserver final : public Node::IObserver, public ILongAction
            {
            public:
                MyObserver(Node& node, NodeClient& model)
                    : m_node(node)
                    , m_model(model)
                {
                    assert(m_model.m_observer);
                }

                ~MyObserver()
                {
                    assert(m_model.m_observer);
                    if (m_reportedStarted) m_model.m_observer->onStoppedNode();
                }

                void OnSyncProgress() override
                {
                    Node::SyncStatus s = m_node.m_SyncStatus;

                    if (MaxHeight == m_Done0)
                        m_Done0 = s.m_Done;
                    s.ToRelative(m_Done0);

                    if (!m_reportedStarted && (s.m_Done == s.m_Total))
                    {
                        m_reportedStarted = true;
                        m_model.m_observer->onStartedNode();
                    }

                    AdjustProgress(s.m_Done, s.m_Total);
                    m_model.m_observer->onSyncProgressUpdated(static_cast<int>(s.m_Done), static_cast<int>(s.m_Total));
                }

                void OnSyncError(Node::IObserver::Error error) override
                {
                    m_model.m_observer->onSyncError(error);
                }

                void OnStateChanged() override
                {
                    m_LastTip = m_node.get_Processor().m_Cursor.m_hh.m_Height;
                }

                void OnRolledBack() override
                {
                    uint64_t to = m_node.get_Processor().m_Cursor.m_hh.m_Height;
                    if (m_LastTip > to)
                    {
                        NodeReorgSnapshot::Event e;
                        e.m_Time = (uint64_t) time(nullptr);
                        e.m_FromHeight = m_LastTip;
                        e.m_ToHeight = to;
                        e.m_Depth = (uint32_t)(m_LastTip - to);
                        m_Reorgs.push_back(e);
                        if (m_Reorgs.size() > 64) m_Reorgs.erase(m_Reorgs.begin());
                        ++m_ReorgCount;
                        if (e.m_Depth > m_ReorgDeepest) m_ReorgDeepest = e.m_Depth;
                    }
                    m_LastTip = to;
                }

                void InitializeUtxosProgress(uint64_t done, uint64_t total) override
                {
                    m_model.m_observer->onInitProgressUpdated(done, total);
                }

                ILongAction* GetLongActionHandler() override
                {
                    return this;
                }

                void Reset(const char* sz, uint64_t nTotal) override
                {
                    SetTotal(nTotal);
                    m_Last_ms = GetTime_ms();
                }

                void SetTotal(uint64_t nTotal) override
                {
                    m_Total = nTotal;
                }

                bool OnProgress(uint64_t pos) override
                {
                    if (m_model.m_shouldTerminateModel)
                    {
                        return false;
                    }
                    uint32_t dt_ms = GetTime_ms() - m_Last_ms;
                    const uint32_t nWindow_ms = 1000; // 1 sec
                    uint32_t n = dt_ms / nWindow_ms;
                    if (n)
                    {
                        m_Last_ms += n * nWindow_ms;
                        uint64_t total = m_Total;
                        AdjustProgress(pos, total);
                        m_model.m_observer->onSyncProgressUpdated(static_cast<int>(pos), static_cast<int>(total));
                    }
                    return true;
                }

                // Reorg state observed via OnStateChanged/OnRolledBack; read by the
                // peer-stats timer. Public so the timer lambda can snapshot them.
                uint64_t m_LastTip = 0;
                uint32_t m_ReorgCount = 0;
                uint32_t m_ReorgDeepest = 0;
                std::vector<NodeReorgSnapshot::Event> m_Reorgs;  // newest last, capped

            private:
                void AdjustProgress(uint64_t& done, uint64_t& total)
                {
                    // make sure no overflow during conversion from SyncStatus to int,int.
                    constexpr auto threshold = static_cast<unsigned int>(std::numeric_limits<int>::max());
                    while (total > threshold)
                    {
                        total >>= 1;
                        done >>= 1;
                    }
                }

            private:
                Node& m_node;
                NodeClient& m_model;
                Height m_Done0 = MaxHeight;
                uint64_t m_Total = 0;
                uint32_t m_Last_ms = 0;
                bool m_reportedStarted = false;
            } obs(node, *this);

            node.m_Cfg.m_Observer = &obs;
            node.Initialize();
            notifier.notifyNodeCreated();

            if (node.get_AcessiblePeerCount() == 0)
            {
                throw std::runtime_error("Resolved peer list is empty");
            }

            // Poll live peer stats every 2s on the node reactor and report them to the
            // observer (accessible/known count + the currently-connected peer addresses +
            // the full known/resolved peer address list). The same tick snapshots the BBS
            // (SBBS relay) store from the node DB.
            io::Timer::Ptr peerStatsTimer = io::Timer::create(io::Reactor::get_Current());
            peerStatsTimer->start(2000, true, [this, &node, &obs]() {
                auto toInfo = [](const std::string& addr, uint32_t raw, bool ratingKnown) {
                    NodePeerInfo e;
                    e.m_Address = addr;
                    e.m_RawRating = raw;
                    e.m_EffectiveBps = PeerManager::Rating::ToBps(raw);
                    e.m_Banned = ratingKnown && (raw == 0);
                    e.m_RatingKnown = ratingKnown;
                    return e;
                };

                std::vector<NodePeerInfo> connected;
                std::vector<Node::ConnectedPeerInfo> rawConnected;
                node.get_ConnectedPeers(rawConnected);
                for (const auto& c : rawConnected) {
                    NodePeerInfo e = toInfo(c.m_Address, c.m_RawRating, c.m_RatingKnown);
                    e.m_TipHeight = c.m_TipHeight;
                    e.m_Inbound = c.m_Inbound;
                    e.m_LoginFlags = c.m_LoginFlags;
                    connected.push_back(e);
                }

                std::vector<NodePeerInfo> known;
                for (const auto& a : node.get_AcessiblePeerAddrs())
                    known.push_back(toInfo(a.m_Value.str(), a.get_ParentObj().m_RawRating.m_Value, true));

                m_observer->onPeerStats(node.get_AcessiblePeerCount(), connected, known);

                NodeBbsSnapshot snap;
                snap.m_TimeoutS = node.m_Cfg.m_Bbs.m_MessageTimeout_s;
                NodeDB& db = node.get_Processor().get_DB();

                NodeDB::BbsTotals totals = { 0, 0 };
                db.get_BbsTotals(totals);
                snap.m_Count = totals.m_Count;
                snap.m_SizeBytes = totals.m_Size;
                snap.m_MaxTimePosted = totals.m_Count ? db.get_BbsMaxTime() : 0;

                struct Histogram : NodeDB::IBbsHistogram {
                    std::vector<std::pair<uint64_t, uint64_t>>& m_Out;
                    Histogram(std::vector<std::pair<uint64_t, uint64_t>>& out) : m_Out(out) {}
                    bool OnChannel(BbsChannel ch, uint64_t nCount) override {
                        m_Out.emplace_back(ch, nCount);
                        return true;
                    }
                } hist(snap.m_Channels);
                db.EnumBbs(hist);

                // Most recent messages (by rowid; IDs autoincrement so the lower bound is
                // an approximation when expired rows were deleted, which only widens it).
                const uint64_t nMaxMsgs = 200;
                const size_t nPreviewBytes = 256;
                NodeDB::WalkerBbs wlk;
                uint64_t nLastID = db.get_BbsLastID();
                wlk.m_ID = (nLastID > nMaxMsgs) ? (nLastID - nMaxMsgs) : 0;
                for (db.EnumAllBbsFull(wlk); wlk.MoveNext(); )
                {
                    NodeBbsSnapshot::Msg& m = snap.m_Msgs.emplace_back();
                    m.m_ID = wlk.m_ID;
                    m.m_KeyHex = to_hex(wlk.m_Data.m_Key.m_pData, wlk.m_Data.m_Key.nBytes);
                    m.m_Channel = wlk.m_Data.m_Channel;
                    m.m_TimePosted = wlk.m_Data.m_TimePosted;
                    m.m_Size = wlk.m_Data.m_Message.n;
                    m.m_Nonce = wlk.m_Data.m_Nonce;
                    m.m_PayloadPreviewHex = to_hex(wlk.m_Data.m_Message.p,
                        std::min<size_t>(wlk.m_Data.m_Message.n, nPreviewBytes));
                    m.m_RelayedBy = node.get_BbsRelayedBy(wlk.m_ID);
                }
                m_observer->onBbsStats(snap);

                Node::DeferredTxStats dts;
                node.get_DeferredTxStats(dts);
                NodeTxQueueSnapshot qs;
                qs.m_Depth = dts.m_Depth;
                qs.m_Cap = dts.m_Cap;
                qs.m_TotalDeferred = dts.m_TotalDeferred;
                qs.m_TotalDropped = dts.m_TotalDropped;
                qs.m_Recent.reserve(dts.m_Recent.size());
                for (const auto& d : dts.m_Recent)
                {
                    NodeTxQueueSnapshot::Tx& t = qs.m_Recent.emplace_back();
                    t.m_Seq = d.m_Seq;
                    t.m_Time = d.m_Time;
                    t.m_From = d.m_From;
                    if (!(d.m_Sender == Zero))
                        t.m_SenderHex = to_hex(d.m_Sender.m_pData, d.m_Sender.nBytes);
                    t.m_Fluff = d.m_Fluff;
                }
                m_observer->onTxQueueStats(qs);

                NodeReorgSnapshot rs;
                rs.m_Count = obs.m_ReorgCount;
                rs.m_Deepest = obs.m_ReorgDeepest;
                rs.m_Recent = obs.m_Reorgs;
                m_observer->onReorgStats(rs);

                NodeMempoolSnapshot ms;
                for (const auto& profit : node.m_TxPool.m_setProfit) {
                    const TxPool::Fluff::Element& e = profit.get_ParentObj();
                    NodeMempoolSnapshot::Tx tx;
                    tx.m_Fee = e.m_Profit.m_Stats.m_Fee;
                    tx.m_SizeBytes = e.m_Profit.m_Stats.m_Size;
                    if (e.m_pValue && !e.m_pValue->m_vKernels.empty()) {
                        const Merkle::Hash& kid = e.m_pValue->m_vKernels.front()->get_ID();
                        tx.m_KernelIdHex = to_hex(kid.m_pData, kid.nBytes);
                    }
                    ms.m_TotalFees += tx.m_Fee;
                    ms.m_Count++;
                    if (ms.m_Txs.size() < 200)
                        ms.m_Txs.push_back(std::move(tx));
                }
                m_observer->onMempoolStats(ms);
            });

            m_isRunning = true;
            io::Reactor::get_Current().run();
            m_isRunning = false;
        }
    }

    void NodeClient::setRecreateTimer()
    {
        if (!m_timer)
        {
            m_timer = io::Timer::create(io::Reactor::get_Current());
        }
        m_timer->start(5000, false, [this]()
        {
            io::Reactor::get_Current().stop();
            startNode();
        });
        io::Reactor::get_Current().run();

    }

}