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

#pragma once

#include <thread>
#include <memory>
#include <atomic>
#include <condition_variable>
#include "node/node.h"
#include "core/block_crypt.h"
#include "utility/io/errorhandling.h"
#include "utility/io/reactor.h"

namespace beam
{
    struct NodePeerInfo
    {
        std::string m_Address;          // "ip:port", or whatever the caller annotates later
        uint32_t m_RawRating = 0;       // PeerManager raw rating
        uint32_t m_EffectiveBps = 0;    // PeerManager::Rating::ToBps(m_RawRating)
        bool m_Banned = false;          // m_RatingKnown && m_RawRating == 0
        bool m_RatingKnown = false;     // false => no PeerInfo (connected-but-unidentified)
        uint64_t m_TipHeight = 0;       // peer's reported chain tip; 0 if unknown / not connected
        bool m_Inbound = false;         // peer dialed us (inbound) vs we dialed them (outbound)
        uint32_t m_LoginFlags = 0;      // negotiated proto login capabilities
    };

    // Snapshot of the node's BBS (SBBS relay) store, polled while the node runs.
    // Plain std types only — consumers may be Beam-agnostic.
    struct NodeBbsSnapshot
    {
        uint32_t m_Count = 0;           // messages currently held
        uint64_t m_SizeBytes = 0;       // total payload bytes held
        uint64_t m_MaxTimePosted = 0;   // newest message TimePosted (unix s), 0 if none
        uint32_t m_TimeoutS = 0;        // retention window (m_Cfg.m_Bbs.m_MessageTimeout_s)
        std::vector<std::pair<uint64_t, uint64_t>> m_Channels; // (channel, count)

        struct Msg
        {
            uint64_t m_ID = 0;              // Bbs table rowid
            std::string m_KeyHex;           // 256-bit message key, hex
            uint64_t m_Channel = 0;
            uint64_t m_TimePosted = 0;      // unix s
            uint64_t m_Size = 0;            // payload bytes
            uint32_t m_Nonce = 0;           // PoW nonce
            std::string m_PayloadPreviewHex; // first bytes of the (encrypted) payload, hex
            std::string m_RelayedBy;        // network peer that relayed it ("ip:port");
                                            // empty for messages stored before this session
        };
        std::vector<Msg> m_Msgs;            // most recent messages, ascending by ID
    };

    // Snapshot of the node's deferred-tx queue (incoming txs parked for validation; the
    // OOM-capped list). Plain std types only — consumers may be Beam-agnostic.
    struct NodeTxQueueSnapshot
    {
        uint32_t m_Depth = 0;           // current queue length
        uint32_t m_Cap = 0;             // m_MaxDeferredTransactions
        uint64_t m_TotalDeferred = 0;   // lifetime enqueued
        uint64_t m_TotalDropped = 0;    // lifetime evicted by the cap
        struct Tx
        {
            uint64_t m_Seq = 0;         // monotonic enqueue counter (stable identity)
            uint64_t m_Time = 0;        // unix s, enqueue time
            std::string m_From;         // relaying network peer ("ip:port"); empty = local requeue
            std::string m_SenderHex;    // wallet-level sender PeerID, hex; empty when unknown
            bool m_Fluff = false;
        };
        std::vector<Tx> m_Recent;       // most recent enqueues, oldest first
    };

    class INodeClientObserver
    {
    public:
        virtual void onNodeCreated() = 0;
        virtual void onNodeDestroyed() = 0;
        virtual void onInitProgressUpdated(uint64_t done, uint64_t total) = 0;
        virtual void onSyncProgressUpdated(int done, int total) = 0;
        virtual void onStartedNode() = 0;
        virtual void onStoppedNode() = 0;
        virtual void onFailedToStartNode(io::ErrorCode errorCode) = 0;
        virtual void onSyncError(Node::IObserver::Error error) = 0;
        // Live peer stats, polled while the node runs: accessible/known peer count (incl.
        // temporarily banned), the list of currently-connected peer addresses, and the full
        // known/resolved peer address list. Non-pure (optional) so existing observers need
        // not implement it.
        virtual void onPeerStats(uint32_t /*accessible*/, const std::vector<NodePeerInfo>& /*connected*/,
                                 const std::vector<NodePeerInfo>& /*known*/) {}
        // BBS (SBBS relay) store stats, polled while the node runs alongside onPeerStats.
        // Non-pure (optional) so existing observers need not implement it.
        virtual void onBbsStats(const NodeBbsSnapshot& /*snapshot*/) {}
        // Deferred-tx queue stats, polled on the same tick. Non-pure (optional).
        virtual void onTxQueueStats(const NodeTxQueueSnapshot& /*snapshot*/) {}

        virtual uint16_t getLocalNodePort() const = 0;
        virtual std::string getLocalNodeStorage() const = 0;
        virtual std::string getTempDir() const = 0;
        virtual std::vector<std::string> getLocalNodePeers() const = 0;
        virtual bool getPeersPersistent() const = 0;

        virtual void onNodeThreadFinished() = 0;
    };

    class NodeClient
    {
    public:
        NodeClient(const Rules& rules, INodeClientObserver* observer);
        ~NodeClient();
        void setBeforeStartAction(std::function<void()> action);
        void setKdf(beam::Key::IKdf::Ptr);
        void setOwnerKey(beam::Key::IPKdf::Ptr);
        void startNode();
        void stopNode();

        void start();

        bool isNodeRunning() const;

    private:
        void runLocalNode();
        void setRecreateTimer();

    private:
        const Rules& m_rules;
        INodeClientObserver* m_observer;
        std::shared_ptr<std::thread> m_thread;
        std::weak_ptr<beam::io::Reactor> m_reactor;
        std::mutex m_startMutex;
        bool m_shouldStartNode;
        std::atomic<bool> m_shouldTerminateModel;
        std::atomic<bool> m_isRunning;
        std::condition_variable m_waiting;
        Key::IKdf::Ptr m_pKdf;
        Key::IPKdf::Ptr m_ownerKey;
        io::Timer::Ptr m_timer;
        std::function<void()> m_beforeStartAction;
    };
}