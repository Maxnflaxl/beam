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

#include "wallet/core/base_transaction.h"
#include "wallet/core/base_tx_builder.h"
#include "common.h"

#include "second_side.h"

namespace beam::wallet
{
    void FillSwapTxParams(TxParameters* params,
                          const WalletID& myID,
                          Height minHeight,
                          Amount amount,
                          Amount beamFee,
                          AtomicSwapCoin swapCoin,
                          Amount swapAmount,
                          Amount swapFeeRate,
                          bool isBeamSide = true,
                          Height responseTime = kDefaultTxResponseTime,
                          Height lifetime = kDefaultTxLifetime);

    void FillSwapTxParams(TxParameters* params,
                          const WalletID& myID,
                          Height minHeight,
                          Amount amount,
                          Amount beamFee,
                          AtomicSwapCoin swapCoin,
                          ECC::uintBig swapAmount,
                          ECC::uintBig gas,
                          ECC::uintBig gasPrice,
                          bool isBeamSide = true,
                          Height responseTime = kDefaultTxResponseTime,
                          Height lifetime = kDefaultTxLifetime);

    void FillSwapFee(
        TxParameters* params, Amount beamFee,
        Amount swapFeeRate, bool isBeamSide = true);

    void FillSwapFee(
        TxParameters* params, Amount beamFee, ECC::uintBig gas, ECC::uintBig gasPrice, bool isBeamSide = true);

    TxParameters MirrorSwapTxParams(const TxParameters& original,
                                    bool isOwn = true);

    TxParameters PrepareSwapTxParamsForTokenization(
        const TxParameters& original);

    TxParameters CreateSwapTransactionParameters(
        const boost::optional<TxID>& oTxId = boost::none);

    class SecondSideFactoryNotRegisteredException : public std::runtime_error
    {
    public:
        explicit SecondSideFactoryNotRegisteredException()
            : std::runtime_error("second side factory is not registered")
        {
        }

    };

    class ISecondSideFactory
    {
    public:
        using Ptr = std::shared_ptr<ISecondSideFactory>;
        virtual ~ISecondSideFactory() = default;
        virtual SecondSide::Ptr CreateSecondSide(BaseTransaction& tx, bool isBeamSide) = 0;
    };

    template<typename BridgeSide, typename Bridge, typename SettingsProvider>
    class SecondSideFactory : public ISecondSideFactory
    {
    public:
        SecondSideFactory(std::function<typename Bridge::Ptr()> bridgeCreator, SettingsProvider& settingsProvider)
            : m_bridgeCreator{ bridgeCreator }
            , m_settingsProvider{ settingsProvider }
        {
        }
    private:
        SecondSide::Ptr CreateSecondSide(BaseTransaction& tx, bool isBeamSide) override
        {
            return std::make_shared<BridgeSide>(tx, m_bridgeCreator(), m_settingsProvider, isBeamSide);
        }
    private:
        std::function<typename Bridge::Ptr()> m_bridgeCreator;
        SettingsProvider& m_settingsProvider;
    };

    template<typename BridgeSide, typename Bridge, typename SettingsProvider>
    ISecondSideFactory::Ptr MakeSecondSideFactory(std::function<typename Bridge::Ptr()> bridgeCreator, SettingsProvider& settingsProvider)
    {
        return std::make_shared<SecondSideFactory<BridgeSide, Bridge, SettingsProvider>>(bridgeCreator, settingsProvider);
    }

    class LockTxBuilder;
    class SharedTxBuilder;

    class AtomicSwapTransaction : public BaseTransaction
    {
        enum class SubTxState : uint8_t
        {
            Initial,
            Invitation,
            Constructed
        };

        class UninitilizedSecondSide : public std::exception
        {
        };

        class ISecondSideProvider
        {
        public:
            virtual SecondSide::Ptr GetSecondSide(BaseTransaction& tx) = 0;
        };

        class WrapperSecondSide
        {
        public:
            WrapperSecondSide(ISecondSideProvider& gateway, BaseTransaction& tx);
            SecondSide::Ptr operator -> ();
            SecondSide::Ptr GetSecondSide();

        private:
            ISecondSideProvider& m_gateway;
            BaseTransaction& m_tx;
            SecondSide::Ptr m_secondSide;
        };

    public:
        enum class State : uint8_t
        {
            Initial,

            BuildingBeamLockTX,
            BuildingBeamRefundTX,
            BuildingBeamRedeemTX,

            HandlingContractTX,
            SendingRefundTX,
            SendingRedeemTX,

            SendingBeamLockTX,
            SendingBeamRefundTX,
            SendingBeamRedeemTX,

            Canceled,

            CompleteSwap,
            Failed,
            Refunded
        };

    public:

        class Creator : public BaseTransaction::Creator
                      , public ISecondSideProvider
        {
        public:
            Creator(IWalletDB::Ptr walletDB);
            void RegisterFactory(AtomicSwapCoin coinType, ISecondSideFactory::Ptr factory);
        private:
            BaseTransaction::Ptr Create(const TxContext& context) override;
            TxParameters CheckAndCompleteParameters(const TxParameters& parameters) override;

            SecondSide::Ptr GetSecondSide(BaseTransaction& tx) override;
        private:
            std::map<AtomicSwapCoin, ISecondSideFactory::Ptr> m_factories;
            IWalletDB::Ptr m_walletDB;
        };

        AtomicSwapTransaction(const TxContext& context
                            , ISecondSideProvider& secondSideProvider);

        bool CanCancel() const override;
        void Cancel() override;

        bool Rollback(Height height) override;

        bool IsTxParameterExternalSettable(TxParameterID paramID, SubTxID subTxID) const override;

    private:
        void SetNextState(State state);

        TxType GetType() const override;
        bool IsInSafety() const override;
        State GetState(SubTxID subTxID) const;
        SubTxState GetSubTxState(SubTxID subTxID) const;
        Amount GetWithdrawFee() const;
        void UpdateImpl() override;
        void NotifyFailure(TxFailureReason) override;
        void OnFailed(TxFailureReason reason, bool notify) override;
        bool CheckExpired() override;
        bool CheckExternalFailures() override;
        void SendInvitation();
        void SendExternalTxDetails();

        void SendQuickRefundPrivateKey();

        SubTxState BuildBeamLockTx();
        void BuildBeamLockTxGuarded(SubTxState&);
        void BuildBeamWithdrawTxGuarded(SubTxState&, SubTxID, Transaction::Ptr&);
        bool SetWithdrawParams(bool bTxOwner, SubTxID);

        SubTxState BuildBeamSubTx(SubTxID subTxID, Transaction::Ptr& pRes);

        SubTxState BuildBeamWithdrawTx(SubTxID subTxID, Transaction::Ptr& resultTx);
        bool CompleteBeamWithdrawTx(SubTxID subTxID);
                
        bool SendSubTx(Transaction::Ptr transaction, SubTxID subTxID);

        bool IsBeamLockTimeExpired() const;
        bool IsBeamRedeemTxRegistered() const;
        bool IsSafeToSendBeamRedeemTx() const;

        // wait SubTX in BEAM chain(request kernel proof), returns true if got kernel proof
        bool CompleteSubTx(SubTxID subTxID);

        bool GetKernelFromChain(SubTxID subTxID) const;

        Amount GetAmount() const;
        bool IsSender() const;
        bool IsBeamSide() const;

        void OnSubTxFailed(TxFailureReason reason, SubTxID subTxID, bool notify = false);
        void CheckSubTxFailures();
        void ExtractSecret();
        void ExtractSecretPrivateKey();
        bool IsHashlockScheme() const;

        mutable boost::optional<bool> m_IsBeamSide;
        mutable boost::optional<bool> m_IsSender;
        mutable boost::optional<beam::Amount> m_Amount;

        Transaction::Ptr m_LockTx;
        Transaction::Ptr m_WithdrawTx;

        std::shared_ptr<LockTxBuilder> m_pLockBuiler;
        std::shared_ptr<SharedTxBuilder> m_pSharedBuiler;

        WrapperSecondSide m_secondSide;
    };
}
