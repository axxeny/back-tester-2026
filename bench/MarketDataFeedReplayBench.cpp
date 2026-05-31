#include "ingestion/JsonNativeDataParser.hpp"
#include "market_data_feed/MarketDataFeed.hpp"

#include <benchmark/benchmark.h>

#include <algorithm>
#include <array>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <vector>

namespace
{

constexpr std::size_t QueueCapacity = 1 << 15;
constexpr uint64_t DrainEvery = 1024;

std::vector<std::filesystem::path> replay_files(benchmark::State& state)
{
    const char* bench_dir_env = std::getenv("BENCH_DIR");
    if (bench_dir_env == nullptr)
    {
        state.SkipWithError("BENCH_DIR env var not set.");
        return {};
    }

    const std::filesystem::path bench_dir{bench_dir_env};
    if (!std::filesystem::is_directory(bench_dir))
    {
        state.SkipWithError("BENCH_DIR is not a directory: " +
                            bench_dir.string());
        return {};
    }

    std::vector<std::filesystem::path> files;
    for (const auto& entry :
         std::filesystem::recursive_directory_iterator{bench_dir})
    {
        if (!entry.is_regular_file())
            continue;

        const auto path = entry.path();
        if (path.string().ends_with(cmf::JsonNativeDataParser::filename_ext))
            files.push_back(path);
    }

    std::sort(files.begin(), files.end());

    std::size_t file_limit = 20;
    if (const char* limit_env = std::getenv("BENCH_FILE_LIMIT");
        limit_env != nullptr)
    {
        file_limit = std::stoull(limit_env);
    }

    if (files.size() < file_limit)
    {
        state.SkipWithError("BENCH_DIR has fewer replay files than requested.");
        return {};
    }

    files.resize(file_limit);
    return files;
}

cmf::MarketDataMessage to_feed_message(const cmf::MarketDataEvent& event)
{
    cmf::MarketDataMessage message;
    if (event.action == cmf::Action::Trade || event.action == cmf::Action::Fill)
    {
        message.type = cmf::MarketDataMessageType::Trade;
        message.trade = cmf::Trade{.instrument_id = event.instrument_id,
                                   .timestamp_ns = event.ts_recv,
                                   .seq_no = event.sequence,
                                   .side = event.side,
                                   .price = event.price,
                                   .size = event.size};
        return message;
    }

    message.type = cmf::MarketDataMessageType::BookUpdate;
    message.update = cmf::BookUpdate{.instrument_id = event.instrument_id,
                                     .timestamp_ns = event.ts_recv,
                                     .seq_no = event.sequence,
                                     .side = event.side,
                                     .price = event.price,
                                     .size = event.size};
    return message;
}

void publish_message(cmf::MarketDataFeed<QueueCapacity>& feed,
                     const cmf::MarketDataMessage& message)
{
    switch (message.type)
    {
    case cmf::MarketDataMessageType::BookUpdate:
        feed.publishUpdate(message.update);
        break;
    case cmf::MarketDataMessageType::BookSnapshot:
        feed.publishSnapshot(message.snapshot);
        break;
    case cmf::MarketDataMessageType::Trade:
        feed.publishTrade(message.trade);
        break;
    }
}

} // namespace

static void BM_MarketDataFeedReplay_ParseOnly(benchmark::State& state)
{
    const auto files = replay_files(state);
    if (files.empty())
        return;

    uint64_t total_events = 0;
    for (auto _ : state)
    {
        uint64_t event_count = 0;
        for (const auto& file : files)
        {
            cmf::JsonNativeDataParser parser{file};
            parser.parse(
                [&](const cmf::MarketDataEvent&)
                { ++event_count; });
        }
        benchmark::DoNotOptimize(event_count);
        total_events = event_count;
    }

    state.SetItemsProcessed(state.iterations() * total_events);
    state.counters["source_events"] = benchmark::Counter(
        static_cast<double>(total_events), benchmark::Counter::kAvgIterations);
}

static void BM_MarketDataFeedReplay_ParseAndFanout(benchmark::State& state)
{
    const auto files = replay_files(state);
    if (files.empty())
        return;

    const auto subscriber_count = static_cast<std::size_t>(state.range(0));
    uint64_t total_events = 0;
    uint64_t total_delivered = 0;
    uint64_t total_gaps = 0;

    for (auto _ : state)
    {
        cmf::MarketDataFeed<QueueCapacity> feed;
        std::vector<cmf::MarketDataFeed<QueueCapacity>::Subscription>
            subscriptions;
        std::vector<uint64_t> delivered(subscriber_count, 0);
        uint64_t gap_count = 0;
        subscriptions.reserve(subscriber_count);

        for (std::size_t i = 0; i < subscriber_count; ++i)
        {
            subscriptions.push_back(feed.subscribe(
                0, [&delivered, &gap_count, i](const cmf::MarketDataMessage&,
                                               const cmf::FeedStatus& status)
                {
                    ++delivered[i];
                    if (status.has_gap)
                        ++gap_count;
                }));
        }

        uint64_t event_count = 0;
        uint64_t pending_since_drain = 0;
        for (const auto& file : files)
        {
            cmf::JsonNativeDataParser parser{file};
            parser.parse(
                [&](const cmf::MarketDataEvent& event)
                {
                    auto message = to_feed_message(event);
                    message.update.instrument_id = 0;
                    message.trade.instrument_id = 0;
                    publish_message(feed, message);
                    ++event_count;
                    ++pending_since_drain;

                    if (pending_since_drain == DrainEvery)
                    {
                        for (auto& subscription : subscriptions)
                            benchmark::DoNotOptimize(subscription.drain());
                        pending_since_drain = 0;
                    }
                });
        }

        for (auto& subscription : subscriptions)
            benchmark::DoNotOptimize(subscription.drain());

        uint64_t delivered_count = 0;
        for (const auto count : delivered)
            delivered_count += count;

        benchmark::DoNotOptimize(delivered_count);
        total_events = event_count;
        total_delivered = delivered_count;
        total_gaps = gap_count;
    }

    state.SetItemsProcessed(state.iterations() * total_delivered);
    state.counters["source_events"] = benchmark::Counter(
        static_cast<double>(total_events), benchmark::Counter::kAvgIterations);
    state.counters["delivered"] = benchmark::Counter(
        static_cast<double>(total_delivered), benchmark::Counter::kAvgIterations);
    state.counters["gaps"] = benchmark::Counter(static_cast<double>(total_gaps),
                                                benchmark::Counter::kAvgIterations);
}

BENCHMARK(BM_MarketDataFeedReplay_ParseOnly)
    ->Name("MarketDataFeedReplay/ParseOnly20Files")
    ->Unit(benchmark::kSecond)
    ->Iterations(1)
    ->UseRealTime();

BENCHMARK(BM_MarketDataFeedReplay_ParseAndFanout)
    ->Name("MarketDataFeedReplay/ParseAndFanout20Files")
    ->Arg(1)
    ->Arg(2)
    ->Arg(4)
    ->Arg(8)
    ->Unit(benchmark::kSecond)
    ->Iterations(1)
    ->UseRealTime();
