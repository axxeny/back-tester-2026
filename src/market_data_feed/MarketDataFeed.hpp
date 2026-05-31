#pragma once

#include "common/BasicTypes.hpp"
#include "common/LockFreeQueue.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <vector>

namespace cmf
{

struct BookLevel
{
    Side side = Side::None;
    ScaledPrice price = 0;
    uint32_t size = 0;
};

struct BookUpdate
{
    uint32_t instrument_id = 0;
    NanoTime timestamp_ns = 0;
    uint64_t seq_no = 0;
    Side side = Side::None;
    ScaledPrice price = 0;
    uint32_t size = 0;
};

struct BookSnapshot
{
    uint32_t instrument_id = 0;
    NanoTime timestamp_ns = 0;
    uint64_t seq_no = 0;
    std::vector<BookLevel> levels;
};

struct Trade
{
    uint32_t instrument_id = 0;
    NanoTime timestamp_ns = 0;
    uint64_t seq_no = 0;
    Side side = Side::None;
    ScaledPrice price = 0;
    uint32_t size = 0;
};

enum class MarketDataMessageType
{
    BookUpdate,
    BookSnapshot,
    Trade,
};

struct MarketDataMessage
{
    MarketDataMessageType type = MarketDataMessageType::BookUpdate;
    BookUpdate update;
    BookSnapshot snapshot;
    Trade trade;

    [[nodiscard]] uint32_t instrument_id() const noexcept;
    [[nodiscard]] uint64_t seq_no() const noexcept;
};

struct FeedStatus
{
    bool has_gap = false;
    uint64_t expected_seq_no = 0;
    uint64_t received_seq_no = 0;
};

template <std::size_t QueueCapacity = 8192>
class MarketDataFeed
{
  private:
    using Queue = LockFreeQueue<MarketDataMessage, QueueCapacity>;

    struct SubscriberState
    {
        uint32_t instrument_id = 0;
        std::function<void(const MarketDataMessage&, const FeedStatus&)> callback;
        Queue queue;
        std::atomic<bool> active{true};
        bool has_last_seq = false;
        uint64_t last_seq_no = 0;
    };

  public:
    class Subscription
    {
      public:
        Subscription() = default;
        explicit Subscription(std::weak_ptr<SubscriberState> state)
            : state_(std::move(state)) {}
        ~Subscription() { close(); }

        Subscription(const Subscription&) = delete;
        Subscription& operator=(const Subscription&) = delete;

        Subscription(Subscription&& other) noexcept
            : state_(std::move(other.state_))
        {
            other.state_.reset();
        }

        Subscription& operator=(Subscription&& other) noexcept
        {
            if (this != &other)
            {
                close();
                state_ = std::move(other.state_);
                other.state_.reset();
            }
            return *this;
        }

        [[nodiscard]] std::size_t drain();
        void close() noexcept;

      private:
        std::weak_ptr<SubscriberState> state_;
    };

    [[nodiscard]] Subscription subscribe(
        uint32_t instrument_id,
        std::function<void(const MarketDataMessage&, const FeedStatus&)> callback);

    void publishUpdate(const BookUpdate& update);
    void publishSnapshot(const BookSnapshot& snapshot);
    void publishTrade(const Trade& trade);

  private:
    void publish(const MarketDataMessage& message);

    std::mutex subscribers_mutex_;
    std::vector<std::shared_ptr<SubscriberState>> subscribers_;
};

inline uint32_t MarketDataMessage::instrument_id() const noexcept
{
    switch (type)
    {
    case MarketDataMessageType::BookUpdate:
        return update.instrument_id;
    case MarketDataMessageType::BookSnapshot:
        return snapshot.instrument_id;
    case MarketDataMessageType::Trade:
        return trade.instrument_id;
    }
    return 0;
}

inline uint64_t MarketDataMessage::seq_no() const noexcept
{
    switch (type)
    {
    case MarketDataMessageType::BookUpdate:
        return update.seq_no;
    case MarketDataMessageType::BookSnapshot:
        return snapshot.seq_no;
    case MarketDataMessageType::Trade:
        return trade.seq_no;
    }
    return 0;
}

template <std::size_t QueueCapacity>
std::size_t MarketDataFeed<QueueCapacity>::Subscription::drain()
{
    auto state = state_.lock();
    if (!state || !state->active.load(std::memory_order_acquire))
        return 0;

    std::size_t drained = 0;
    while (!state->queue.empty())
    {
        const bool popped = state->queue.pop(
            [&](MarketDataMessage&& message)
            {
                FeedStatus status;
                const uint64_t seq_no = message.seq_no();
                if (state->has_last_seq && seq_no != state->last_seq_no + 1)
                {
                    status.has_gap = true;
                    status.expected_seq_no = state->last_seq_no + 1;
                    status.received_seq_no = seq_no;
                }
                state->last_seq_no = seq_no;
                state->has_last_seq = true;
                state->callback(message, status);
                ++drained;
            });
        if (!popped)
            break;
    }
    return drained;
}

template <std::size_t QueueCapacity>
void MarketDataFeed<QueueCapacity>::Subscription::close() noexcept
{
    auto state = state_.lock();
    if (!state)
        return;
    if (state->active.exchange(false, std::memory_order_acq_rel))
    {
        state->queue.close();
    }
    state_.reset();
}

template <std::size_t QueueCapacity>
typename MarketDataFeed<QueueCapacity>::Subscription
MarketDataFeed<QueueCapacity>::subscribe(
    uint32_t instrument_id,
    std::function<void(const MarketDataMessage&, const FeedStatus&)> callback)
{
    auto state = std::make_shared<SubscriberState>();
    state->instrument_id = instrument_id;
    state->callback = std::move(callback);

    {
        std::scoped_lock lock{subscribers_mutex_};
        subscribers_.push_back(state);
    }

    return Subscription{state};
}

template <std::size_t QueueCapacity>
void MarketDataFeed<QueueCapacity>::publishUpdate(const BookUpdate& update)
{
    MarketDataMessage message;
    message.type = MarketDataMessageType::BookUpdate;
    message.update = update;
    publish(message);
}

template <std::size_t QueueCapacity>
void MarketDataFeed<QueueCapacity>::publishSnapshot(const BookSnapshot& snapshot)
{
    MarketDataMessage message;
    message.type = MarketDataMessageType::BookSnapshot;
    message.snapshot = snapshot;
    publish(message);
}

template <std::size_t QueueCapacity>
void MarketDataFeed<QueueCapacity>::publishTrade(const Trade& trade)
{
    MarketDataMessage message;
    message.type = MarketDataMessageType::Trade;
    message.trade = trade;
    publish(message);
}

template <std::size_t QueueCapacity>
void MarketDataFeed<QueueCapacity>::publish(const MarketDataMessage& message)
{
    std::scoped_lock lock{subscribers_mutex_};
    for (const auto& subscriber : subscribers_)
    {
        if (!subscriber->active.load(std::memory_order_acquire) ||
            subscriber->instrument_id != message.instrument_id())
        {
            continue;
        }
        try
        {
            subscriber->queue.push(message);
        }
        catch (const std::runtime_error&)
        {
            // A subscription may close after the active check while another
            // thread is publishing. Dropping that closed-subscriber copy keeps
            // unsubscribe idempotent without affecting other subscribers.
        }
    }
}

} // namespace cmf
