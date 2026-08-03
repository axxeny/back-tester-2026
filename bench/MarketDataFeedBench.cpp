#include "market_data_feed/MarketDataFeed.hpp"

#include <benchmark/benchmark.h>

#include <memory>
#include <vector>

namespace
{

cmf::BookUpdate make_update(uint64_t seq_no)
{
    return cmf::BookUpdate{.instrument_id = 10,
                           .timestamp_ns = static_cast<cmf::NanoTime>(seq_no),
                           .seq_no = seq_no,
                           .side = cmf::Side::Buy,
                           .price = 100'000'000'000 + static_cast<int64_t>(seq_no),
                           .size = 100};
}

} // namespace

static void BM_MarketDataFeed_PublishDrain(benchmark::State& state)
{
    const auto event_count = static_cast<uint64_t>(state.range(0));
    const auto subscriber_count = static_cast<std::size_t>(state.range(1));

    for (auto _ : state)
    {
        cmf::MarketDataFeed<(1 << 15)> feed;
        std::vector<uint64_t> delivered(subscriber_count, 0);
        std::vector<cmf::MarketDataFeed<(1 << 15)>::Subscription> subscriptions;
        subscriptions.reserve(subscriber_count);

        for (std::size_t i = 0; i < subscriber_count; ++i)
        {
            subscriptions.push_back(feed.subscribe(
                10, [&delivered, i](const cmf::MarketDataMessage&,
                                    const cmf::FeedStatus&)
                { ++delivered[i]; }));
        }

        for (uint64_t seq_no = 1; seq_no <= event_count; ++seq_no)
            feed.publishUpdate(make_update(seq_no));

        for (auto& subscription : subscriptions)
            benchmark::DoNotOptimize(subscription.drain());

        benchmark::DoNotOptimize(delivered);
    }

    state.SetItemsProcessed(state.iterations() * event_count *
                            subscriber_count);
}

BENCHMARK(BM_MarketDataFeed_PublishDrain)
    ->Name("MarketDataFeed/PublishDrain")
    ->Args({1 << 10, 1})
    ->Args({1 << 10, 2})
    ->Args({1 << 10, 4})
    ->Args({1 << 10, 8})
    ->Unit(benchmark::kMicrosecond)
    ->MinWarmUpTime(0.1)
    ->MinTime(0.2)
    ->UseRealTime();
