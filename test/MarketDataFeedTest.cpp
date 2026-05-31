#include "catch2/catch_all.hpp"
#include "market_data_feed/MarketDataFeed.hpp"

#include <vector>

using namespace cmf;

TEST_CASE("MarketDataFeed routes updates by instrument", "[MarketDataFeed]")
{
    MarketDataFeed<> feed;
    std::vector<MarketDataMessage> first;
    std::vector<MarketDataMessage> second;

    auto sub1 = feed.subscribe(10, [&](const MarketDataMessage& message,
                                       [[maybe_unused]] const FeedStatus& status)
                               { first.push_back(message); });
    auto sub2 = feed.subscribe(20, [&](const MarketDataMessage& message,
                                       [[maybe_unused]] const FeedStatus& status)
                               { second.push_back(message); });

    feed.publishUpdate(BookUpdate{.instrument_id = 10,
                                  .timestamp_ns = 100,
                                  .seq_no = 1,
                                  .side = Side::Buy,
                                  .price = 123'000'000'000,
                                  .size = 50});

    REQUIRE(sub1.drain() == 1);
    REQUIRE(sub2.drain() == 0);
    REQUIRE(first.size() == 1);
    REQUIRE(second.empty());
    REQUIRE(first.front().type == MarketDataMessageType::BookUpdate);
    REQUIRE(first.front().update.price == 123'000'000'000);
}

TEST_CASE("MarketDataFeed preserves snapshot and trade payloads",
          "[MarketDataFeed]")
{
    MarketDataFeed<> feed;
    std::vector<MarketDataMessage> messages;
    auto sub = feed.subscribe(42, [&](const MarketDataMessage& message,
                                      [[maybe_unused]] const FeedStatus& status)
                              { messages.push_back(message); });

    BookSnapshot snapshot;
    snapshot.instrument_id = 42;
    snapshot.timestamp_ns = 200;
    snapshot.seq_no = 1;
    snapshot.levels.push_back(
        BookLevel{.side = Side::Buy, .price = 100'000'000'000, .size = 10});
    snapshot.levels.push_back(
        BookLevel{.side = Side::Sell, .price = 101'000'000'000, .size = 12});
    feed.publishSnapshot(snapshot);

    feed.publishTrade(Trade{.instrument_id = 42,
                            .timestamp_ns = 300,
                            .seq_no = 2,
                            .side = Side::Sell,
                            .price = 100'500'000'000,
                            .size = 7});

    REQUIRE(sub.drain() == 2);
    REQUIRE(messages[0].type == MarketDataMessageType::BookSnapshot);
    REQUIRE(messages[0].snapshot.levels.size() == 2);
    REQUIRE(messages[0].snapshot.levels[1].price == 101'000'000'000);
    REQUIRE(messages[1].type == MarketDataMessageType::Trade);
    REQUIRE(messages[1].trade.size == 7);
}

TEST_CASE("MarketDataFeed reports per-instrument sequence gaps",
          "[MarketDataFeed]")
{
    MarketDataFeed<> feed;
    std::vector<FeedStatus> statuses;
    auto sub = feed.subscribe(7, [&](const MarketDataMessage&,
                                     const FeedStatus& status)
                              { statuses.push_back(status); });

    feed.publishUpdate(BookUpdate{.instrument_id = 7,
                                  .timestamp_ns = 100,
                                  .seq_no = 10,
                                  .side = Side::Buy,
                                  .price = 10,
                                  .size = 1});
    feed.publishUpdate(BookUpdate{.instrument_id = 7,
                                  .timestamp_ns = 101,
                                  .seq_no = 12,
                                  .side = Side::Buy,
                                  .price = 11,
                                  .size = 2});

    REQUIRE(sub.drain() == 2);
    REQUIRE(statuses.size() == 2);
    REQUIRE_FALSE(statuses[0].has_gap);
    REQUIRE(statuses[1].has_gap);
    REQUIRE(statuses[1].expected_seq_no == 11);
    REQUIRE(statuses[1].received_seq_no == 12);
}
