#include "SimulatedLOB.hpp"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace {
void addLevel(std::map<double, long long, std::greater<double>>& levels, double price, long long size)
{
    if (size > 0)
        levels[price] += size;
}

void addLevel(std::map<double, long long>& levels, double price, long long size)
{
    if (size > 0)
        levels[price] += size;
}

BookSnapshot makeSnapshot(
    const std::map<double, long long, std::greater<double>>& bids,
    const std::map<double, long long>& asks,
    std::size_t depth)
{
    BookSnapshot out;

    std::size_t count = 0;
    for (const auto& [price, size] : bids)
    {
        if (depth != 0 && count >= depth)
            break;
        if (size > 0)
        {
            out.bids.push_back(BookLevel{price, size});
            ++count;
        }
    }

    count = 0;
    for (const auto& [price, size] : asks)
    {
        if (depth != 0 && count >= depth)
            break;
        if (size > 0)
        {
            out.asks.push_back(BookLevel{price, size});
            ++count;
        }
    }

    return out;
}
} // namespace

void HistoricalLOB::applyEvent(const MarketDataEvent& event)
{
    std::unique_lock<std::shared_mutex> lock(mutex_);
    book_.applyEvent(event);
}

std::optional<double> HistoricalLOB::bestBid() const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return book_.bestBid();
}

std::optional<double> HistoricalLOB::bestAsk() const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return book_.bestAsk();
}

long long HistoricalLOB::bestBidSize() const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return book_.bestBidSize();
}

long long HistoricalLOB::bestAskSize() const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return book_.bestAskSize();
}

BookSnapshot HistoricalLOB::snapshot(std::size_t depth) const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return book_.snapshot(depth);
}

void HistoricalLOB::printSnapshot(std::ostream& os, int depth) const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    book_.printSnapshot(os, depth);
}

long long HistoricalLOB::totalAdds() const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return book_.totalAdds;
}

long long HistoricalLOB::totalCancels() const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return book_.totalCancels;
}

long long HistoricalLOB::totalTrades() const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return book_.totalTrades;
}

long long HistoricalLOB::totalClears() const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return book_.totalClears;
}

EngineView::EngineView(std::string engineId) : engineId_(std::move(engineId))
{
    if (engineId_.empty())
        throw std::invalid_argument("EngineView requires a non-empty engine id");
}

const std::string& EngineView::engineId() const noexcept
{
    return engineId_;
}

bool EngineView::isBuy(char side)
{
    return side == 'B' || side == 'b';
}

bool EngineView::isSell(char side)
{
    return side == 'A' || side == 'S' || side == 'a' || side == 's';
}

LimitOrderPlacementResult EngineView::placeLimitOrder(
    const HistoricalLOB& historical,
    const std::string& clientOrderId,
    char side,
    double limitPrice,
    long long size)
{
    if (clientOrderId.empty())
        throw std::invalid_argument("clientOrderId must be non-empty");
    if (!isBuy(side) && !isSell(side))
        throw std::invalid_argument("side must be B/b for buy or A/S/a/s for sell");
    if (size <= 0)
        throw std::invalid_argument("size must be positive");

    // One atomic point-in-time read of the basement.  The per-engine lock below
    // serializes this engine's own private consumption/order overlay without
    // blocking other engines.
    BookSnapshot historicalSnapshot = historical.snapshot(0);

    std::lock_guard<std::mutex> lock(mutex_);

    // A duplicate client id replaces the previous still-resting synthetic order.
    removeRestingOrderLocked(clientOrderId);

    LimitOrderPlacementResult result;
    result.engineId = engineId_;
    result.clientOrderId = clientOrderId;
    result.side = isBuy(side) ? 'B' : 'A';
    result.limitPrice = limitPrice;
    result.requestedSize = size;

    long long remaining = size;

    if (isBuy(side))
    {
        const auto privateAsk = bestPrivateHistoricalAskLocked(historicalSnapshot);
        if (privateAsk.has_value() && limitPrice >= privateAsk->price)
        {
            const long long fillSize = std::min(remaining, privateAsk->size);
            addConsumedLocked('A', privateAsk->price, fillSize);
            remaining -= fillSize;
            result.fills.push_back(SyntheticFill{engineId_, clientOrderId, 'B', privateAsk->price, fillSize});
            result.filledSize += fillSize;
        }
    }
    else
    {
        const auto privateBid = bestPrivateHistoricalBidLocked(historicalSnapshot);
        if (privateBid.has_value() && limitPrice <= privateBid->price)
        {
            const long long fillSize = std::min(remaining, privateBid->size);
            addConsumedLocked('B', privateBid->price, fillSize);
            remaining -= fillSize;
            result.fills.push_back(SyntheticFill{engineId_, clientOrderId, 'A', privateBid->price, fillSize});
            result.filledSize += fillSize;
        }
    }

    if (remaining > 0)
    {
        addRestingOrderLocked(SyntheticOrder{clientOrderId, result.side, limitPrice, remaining});
        result.restingSize = remaining;
    }

    return result;
}

bool EngineView::cancelOwnOrder(const std::string& clientOrderId)
{
    std::lock_guard<std::mutex> lock(mutex_);
    return removeRestingOrderLocked(clientOrderId);
}

BookSnapshot EngineView::visibleSnapshot(const HistoricalLOB& historical, std::size_t depth) const
{
    // Need extra historical depth because private consumption can hide the first
    // levels for this engine.  depth=0 already means all levels.
    BookSnapshot historicalSnapshot = historical.snapshot(depth == 0 ? 0 : depth + 32);

    std::lock_guard<std::mutex> lock(mutex_);
    return mergeLocked(historicalSnapshot, depth);
}

std::optional<double> EngineView::visibleBestBid(const HistoricalLOB& historical) const
{
    BookSnapshot snap = visibleSnapshot(historical, 1);
    if (snap.bids.empty())
        return std::nullopt;
    return snap.bids.front().price;
}

std::optional<double> EngineView::visibleBestAsk(const HistoricalLOB& historical) const
{
    BookSnapshot snap = visibleSnapshot(historical, 1);
    if (snap.asks.empty())
        return std::nullopt;
    return snap.asks.front().price;
}

long long EngineView::visibleBestBidSize(const HistoricalLOB& historical) const
{
    BookSnapshot snap = visibleSnapshot(historical, 1);
    if (snap.bids.empty())
        return 0;
    return snap.bids.front().size;
}

long long EngineView::visibleBestAskSize(const HistoricalLOB& historical) const
{
    BookSnapshot snap = visibleSnapshot(historical, 1);
    if (snap.asks.empty())
        return 0;
    return snap.asks.front().size;
}

std::size_t EngineView::liveOwnOrderCount() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return ownOrders_.size();
}

std::optional<BookLevel> EngineView::bestPrivateHistoricalBidLocked(const BookSnapshot& historical) const
{
    for (const BookLevel& level : historical.bids)
    {
        const long long visibleSize = level.size - consumedAtLocked('B', level.price);
        if (visibleSize > 0)
            return BookLevel{level.price, visibleSize};
    }

    return std::nullopt;
}

std::optional<BookLevel> EngineView::bestPrivateHistoricalAskLocked(const BookSnapshot& historical) const
{
    for (const BookLevel& level : historical.asks)
    {
        const long long visibleSize = level.size - consumedAtLocked('A', level.price);
        if (visibleSize > 0)
            return BookLevel{level.price, visibleSize};
    }

    return std::nullopt;
}

BookSnapshot EngineView::mergeLocked(const BookSnapshot& historical, std::size_t depth) const
{
    std::map<double, long long, std::greater<double>> bids;
    std::map<double, long long> asks;

    for (const BookLevel& level : historical.bids)
        addLevel(bids, level.price, level.size - consumedAtLocked('B', level.price));

    for (const BookLevel& level : historical.asks)
        addLevel(asks, level.price, level.size - consumedAtLocked('A', level.price));

    for (const auto& [price, orders] : ownBids_)
    {
        long long total = 0;
        for (const auto& [orderId, size] : orders)
        {
            (void)orderId;
            total += size;
        }
        addLevel(bids, price, total);
    }

    for (const auto& [price, orders] : ownAsks_)
    {
        long long total = 0;
        for (const auto& [orderId, size] : orders)
        {
            (void)orderId;
            total += size;
        }
        addLevel(asks, price, total);
    }

    return makeSnapshot(bids, asks, depth);
}

long long EngineView::consumedAtLocked(char historicalSide, double price) const
{
    if (historicalSide == 'B')
    {
        const auto it = consumedHistoricalBids_.find(price);
        return it == consumedHistoricalBids_.end() ? 0 : it->second;
    }

    const auto it = consumedHistoricalAsks_.find(price);
    return it == consumedHistoricalAsks_.end() ? 0 : it->second;
}

void EngineView::addConsumedLocked(char historicalSide, double price, long long size)
{
    if (size <= 0)
        return;

    if (historicalSide == 'B')
        consumedHistoricalBids_[price] += size;
    else
        consumedHistoricalAsks_[price] += size;
}

void EngineView::addRestingOrderLocked(const SyntheticOrder& order)
{
    if (order.remainingSize <= 0)
        return;

    ownOrders_[order.clientOrderId] = order;
    if (order.side == 'B')
        ownBids_[order.price][order.clientOrderId] = order.remainingSize;
    else
        ownAsks_[order.price][order.clientOrderId] = order.remainingSize;
}

bool EngineView::removeRestingOrderLocked(const std::string& clientOrderId)
{
    const auto orderIt = ownOrders_.find(clientOrderId);
    if (orderIt == ownOrders_.end())
        return false;

    const SyntheticOrder order = orderIt->second;
    ownOrders_.erase(orderIt);

    if (order.side == 'B')
    {
        auto levelIt = ownBids_.find(order.price);
        if (levelIt != ownBids_.end())
        {
            levelIt->second.erase(clientOrderId);
            if (levelIt->second.empty())
                ownBids_.erase(levelIt);
        }
    }
    else
    {
        auto levelIt = ownAsks_.find(order.price);
        if (levelIt != ownAsks_.end())
        {
            levelIt->second.erase(clientOrderId);
            if (levelIt->second.empty())
                ownAsks_.erase(levelIt);
        }
    }

    return true;
}

SimulatedLOB::SimulatedLOB(std::shared_ptr<HistoricalLOB> historical, std::shared_ptr<EngineView> view)
    : historical_(std::move(historical)), view_(std::move(view))
{
    if (!historical_)
        throw std::invalid_argument("SimulatedLOB requires a non-null HistoricalLOB");
    if (!view_)
        throw std::invalid_argument("SimulatedLOB requires a non-null EngineView");
}

LimitOrderPlacementResult SimulatedLOB::placeLimitOrder(
    const std::string& clientOrderId,
    char side,
    double limitPrice,
    long long size)
{
    return view_->placeLimitOrder(*historical_, clientOrderId, side, limitPrice, size);
}

bool SimulatedLOB::cancelOwnOrder(const std::string& clientOrderId)
{
    return view_->cancelOwnOrder(clientOrderId);
}

BookSnapshot SimulatedLOB::snapshot(std::size_t depth) const
{
    return view_->visibleSnapshot(*historical_, depth);
}

std::optional<double> SimulatedLOB::bestBid() const
{
    return view_->visibleBestBid(*historical_);
}

std::optional<double> SimulatedLOB::bestAsk() const
{
    return view_->visibleBestAsk(*historical_);
}

long long SimulatedLOB::bestBidSize() const
{
    return view_->visibleBestBidSize(*historical_);
}

long long SimulatedLOB::bestAskSize() const
{
    return view_->visibleBestAskSize(*historical_);
}

const std::string& SimulatedLOB::engineId() const noexcept
{
    return view_->engineId();
}
