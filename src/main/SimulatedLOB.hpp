#pragma once

#include "BookTypes.hpp"
#include "LimitOrderBook.hpp"
#include "MarketDataEvent.hpp"

#include <cstddef>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

// HistoricalLOB is the shared basement reconstructed from historical L3 replay.
// It is intentionally one copy per instrument.  Many EngineView overlays can read
// it concurrently through snapshot()/bestBid()/bestAsk(), while replay mutates it
// under an exclusive lock.
class HistoricalLOB {
public:
    void applyEvent(const MarketDataEvent& event);

    std::optional<double> bestBid() const;
    std::optional<double> bestAsk() const;
    long long bestBidSize() const;
    long long bestAskSize() const;

    // depth=0 returns all aggregated levels.  Non-zero returns top N per side.
    BookSnapshot snapshot(std::size_t depth = 0) const;
    void printSnapshot(std::ostream& os, int depth = 5) const;

    long long totalAdds() const;
    long long totalCancels() const;
    long long totalTrades() const;
    long long totalClears() const;

private:
    mutable std::shared_mutex mutex_;
    LimitOrderBook book_;
};

enum class FillLiquiditySource {
    HistoricalTouch
};

struct SyntheticFill {
    std::string engineId;
    std::string clientOrderId;
    char side = 'N';
    double price = 0.0;
    long long size = 0;
    FillLiquiditySource source = FillLiquiditySource::HistoricalTouch;
};

struct LimitOrderPlacementResult {
    std::string engineId;
    std::string clientOrderId;
    char side = 'N';
    double limitPrice = 0.0;
    long long requestedSize = 0;
    long long filledSize = 0;
    long long restingSize = 0;
    std::vector<SyntheticFill> fills;

    bool hasFill() const { return filledSize > 0; }
    bool isResting() const { return restingSize > 0; }
};

// EngineView is the per-strategy/per-engine diff layer.
// It stores only:
//   1) this engine's synthetic resting limit orders;
//   2) this engine's privately consumed historical liquidity.
// It never copies the full L3 book, so N engines scale as O(shared historical LOB + N * diffs).
class EngineView {
public:
    explicit EngineView(std::string engineId);

    const std::string& engineId() const noexcept;

    LimitOrderPlacementResult placeLimitOrder(
        const HistoricalLOB& historical,
        const std::string& clientOrderId,
        char side,
        double limitPrice,
        long long size);

    bool cancelOwnOrder(const std::string& clientOrderId);

    // Engine-private book: historical book after this engine's private consumption
    // plus this engine's own synthetic resting orders.  Other engines' synthetic
    // orders and private consumption are intentionally invisible.
    BookSnapshot visibleSnapshot(const HistoricalLOB& historical, std::size_t depth = 5) const;

    std::optional<double> visibleBestBid(const HistoricalLOB& historical) const;
    std::optional<double> visibleBestAsk(const HistoricalLOB& historical) const;
    long long visibleBestBidSize(const HistoricalLOB& historical) const;
    long long visibleBestAskSize(const HistoricalLOB& historical) const;

    std::size_t liveOwnOrderCount() const;

private:
    struct SyntheticOrder {
        std::string clientOrderId;
        char side = 'N';
        double price = 0.0;
        long long remainingSize = 0;
    };

    using OwnBidMap = std::map<double, std::map<std::string, long long>, std::greater<double>>;
    using OwnAskMap = std::map<double, std::map<std::string, long long>>;
    using ConsumedBidMap = std::map<double, long long, std::greater<double>>;
    using ConsumedAskMap = std::map<double, long long>;

    static bool isBuy(char side);
    static bool isSell(char side);

    std::optional<BookLevel> bestPrivateHistoricalBidLocked(const BookSnapshot& historical) const;
    std::optional<BookLevel> bestPrivateHistoricalAskLocked(const BookSnapshot& historical) const;
    BookSnapshot mergeLocked(const BookSnapshot& historical, std::size_t depth) const;

    long long consumedAtLocked(char historicalSide, double price) const;
    void addConsumedLocked(char historicalSide, double price, long long size);
    void addRestingOrderLocked(const SyntheticOrder& order);
    bool removeRestingOrderLocked(const std::string& clientOrderId);

    std::string engineId_;
    mutable std::mutex mutex_;
    OwnBidMap ownBids_;
    OwnAskMap ownAsks_;
    std::unordered_map<std::string, SyntheticOrder> ownOrders_;
    ConsumedBidMap consumedHistoricalBids_;
    ConsumedAskMap consumedHistoricalAsks_;
};

// Lightweight facade used by a trading engine.  It binds one shared HistoricalLOB
// to one private EngineView and exposes only that engine's consistent private LOB.
class SimulatedLOB {
public:
    SimulatedLOB(std::shared_ptr<HistoricalLOB> historical, std::shared_ptr<EngineView> view);

    LimitOrderPlacementResult placeLimitOrder(
        const std::string& clientOrderId,
        char side,
        double limitPrice,
        long long size);

    bool cancelOwnOrder(const std::string& clientOrderId);

    BookSnapshot snapshot(std::size_t depth = 5) const;
    std::optional<double> bestBid() const;
    std::optional<double> bestAsk() const;
    long long bestBidSize() const;
    long long bestAskSize() const;

    const std::string& engineId() const noexcept;

private:
    std::shared_ptr<HistoricalLOB> historical_;
    std::shared_ptr<EngineView> view_;
};
