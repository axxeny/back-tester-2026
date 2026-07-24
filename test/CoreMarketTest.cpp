#include "market/HistoricalLOBStore.hpp"
#include "market/JsonlReader.hpp"
#include "market/Parsing.hpp"

#include "MiniTest.hpp"
#include "TempFile.hpp"

#include <fstream>
#include <string>

namespace {

using cmf::InstrumentMeta;
using cmf::Side;
using cmf::market::BookError;
using cmf::market::HistoricalLOBStore;
using cmf::market::JsonlReader;
using TypedLimitOrderBook = cmf::market::LimitOrderBook;
using cmf::market::MarketAction;
using TypedMarketDataEvent = cmf::market::MarketDataEvent;
using cmf::market::SourceError;

TypedMarketDataEvent event(MarketAction action, cmf::ExchangeOrderId order_id,
                           Side side = Side::None,
                           std::optional<cmf::PriceTicks> price = std::nullopt,
                           cmf::Quantity quantity = 0,
                           cmf::InstrumentId instrument_id = 1) {
  TypedMarketDataEvent result;
  result.action = action;
  result.exchange_order_id = order_id;
  result.side = side;
  result.price_ticks = price;
  result.quantity = quantity;
  result.instrument_id = instrument_id;
  return result;
}

void write_file(const cmf::TempFile &file, const std::string &contents) {
  std::ofstream output(file.getPath());
  output << contents;
}

template <typename Function> bool throws_source_error(Function &&function) {
  try {
    function();
  } catch (const SourceError &) {
    return true;
  }
  return false;
}

template <typename Function> bool throws_book_error(Function &&function) {
  try {
    function();
  } catch (const BookError &) {
    return true;
  }
  return false;
}

} // namespace

TEST_CASE("Market parsing converts exact decimals to integer ticks",
          "[CoreMarket]") {
  using cmf::market::parse_decimal_ticks;

  REQUIRE(parse_decimal_ticks("1.2345", 10'000, 5) == 12'345);
  REQUIRE(parse_decimal_ticks("001.2300", 10'000, 5) == 12'300);
  REQUIRE(parse_decimal_ticks("-1.25", 100, 5) == -125);
  REQUIRE(parse_decimal_ticks("+2", 100, 25) == 200);

  bool invalid_precision = false;
  try {
    (void)parse_decimal_ticks("1.23456", 10'000, 1);
  } catch (const std::invalid_argument &) {
    invalid_precision = true;
  }
  REQUIRE(invalid_precision);

  bool invalid_tick = false;
  try {
    (void)parse_decimal_ticks("1.2344", 10'000, 5);
  } catch (const std::invalid_argument &) {
    invalid_tick = true;
  }
  REQUIRE(invalid_tick);
}

TEST_CASE("Market parsing converts UTC timestamps once at nanosecond precision",
          "[CoreMarket]") {
  using cmf::market::parse_iso8601_timestamp_ns;

  REQUIRE(parse_iso8601_timestamp_ns("1970-01-01T00:00:00Z") == 0);
  REQUIRE(parse_iso8601_timestamp_ns("1970-01-01T00:00:00.000000001Z") == 1);
  REQUIRE(parse_iso8601_timestamp_ns("1970-01-01T00:00:01.5Z") ==
          1'500'000'000);
  REQUIRE(parse_iso8601_timestamp_ns("2000-02-29T00:00:00Z") -
              parse_iso8601_timestamp_ns("2000-02-28T00:00:00Z") ==
          86'400'000'000'000);

  bool invalid_date = false;
  try {
    (void)parse_iso8601_timestamp_ns("2026-02-29T00:00:00Z");
  } catch (const std::invalid_argument &) {
    invalid_date = true;
  }
  REQUIRE(invalid_date);
}

TEST_CASE("JSONL reader streams typed rows and preserves atomic flags",
          "[CoreMarket]") {
  cmf::TempFile file("backtester-core-market-valid.jsonl");
  write_file(
      file,
      R"({"ts_recv":"2026-04-07T09:00:00.000000001Z","hd":{"ts_event":"2026-04-07T09:00:00.000000000Z","instrument_id":42},"action":"A","side":"B","price":"100.25","size":10,"order_id":"18446744073709551615","flags":128,"sequence":7})"
      "\n"
      R"({"ts_recv":"2026-04-07T09:00:00.000000002Z","hd":{"ts_event":"2026-04-07T09:00:00.000000000Z","instrument_id":42},"action":"T","side":"A","price":"100.25","size":2,"flags":0,"sequence":8})");

  JsonlReader::InstrumentMap instruments{
      {42, InstrumentMeta{42, 25, 100, 100}}};
  JsonlReader reader(file.getPath().string(), instruments);
  TypedMarketDataEvent first;
  TypedMarketDataEvent second;
  REQUIRE(reader.next(first));
  REQUIRE(reader.next(second));
  REQUIRE_FALSE(reader.next(second));

  REQUIRE(first.instrument_id == 42);
  REQUIRE(first.exchange_order_id ==
          std::numeric_limits<cmf::ExchangeOrderId>::max());
  REQUIRE(first.price_ticks.has_value());
  REQUIRE(*first.price_ticks == 10'025);
  REQUIRE(first.quantity == 10);
  REQUIRE(first.is_last_in_group());
  REQUIRE(second.action == MarketAction::Trade);
  REQUIRE(second.source_sequence == 8);
  REQUIRE(second.exchange_ts_ns == first.exchange_ts_ns);
}

TEST_CASE("JSONL reader fails fast with source row and context",
          "[CoreMarket]") {
  cmf::TempFile malformed("backtester-core-market-malformed.jsonl");
  write_file(malformed, "{\"hd\":");
  JsonlReader malformed_reader(malformed.getPath().string());
  TypedMarketDataEvent output;
  bool saw_context = false;
  try {
    (void)malformed_reader.next(output);
  } catch (const SourceError &error) {
    saw_context = error.row() == 1 &&
                  error.path() == malformed.getPath().string() &&
                  error.context() == "{\"hd\":" &&
                  std::string(error.what()).find(":1:") != std::string::npos;
  }
  REQUIRE(saw_context);

  cmf::TempFile missing("backtester-core-market-missing.jsonl");
  write_file(
      missing,
      R"({"ts_recv":"2026-04-07T09:00:00Z","hd":{"ts_event":"2026-04-07T09:00:00Z","instrument_id":1},"action":"A","side":"B","price":"1","order_id":"1","sequence":1})");
  JsonlReader missing_reader(missing.getPath().string());
  REQUIRE(throws_source_error([&] { (void)missing_reader.next(output); }));
}

TEST_CASE("JSONL reader rejects chronology and source sequence regressions",
          "[CoreMarket]") {
  const std::string prefix =
      R"({"ts_recv":"2026-04-07T09:00:00Z","hd":{"ts_event":")";
  const std::string suffix =
      R"(","instrument_id":1},"action":"T","side":"B","price":"1","size":1,"sequence":)";

  cmf::TempFile chronology("backtester-core-market-chronology.jsonl");
  write_file(chronology, prefix + "2026-04-07T09:00:01Z" + suffix + "1}\n" +
                             prefix + "2026-04-07T09:00:00Z" + suffix + "2}");
  JsonlReader chronology_reader(chronology.getPath().string());
  TypedMarketDataEvent output;
  REQUIRE(chronology_reader.next(output));
  REQUIRE(throws_source_error([&] { (void)chronology_reader.next(output); }));

  cmf::TempFile sequence("backtester-core-market-sequence.jsonl");
  write_file(sequence, prefix + "2026-04-07T09:00:00Z" + suffix + "2}\n" +
                           prefix + "2026-04-07T09:00:00Z" + suffix + "2}");
  JsonlReader sequence_reader(sequence.getPath().string());
  REQUIRE(sequence_reader.next(output));
  REQUIRE(throws_source_error([&] { (void)sequence_reader.next(output); }));
}

TEST_CASE("Historical L3 book handles all source actions and partial fills",
          "[CoreMarket]") {
  TypedLimitOrderBook book;
  book.apply(event(MarketAction::Add, 1, Side::Buy, 100, 10));
  book.apply(event(MarketAction::Add, 2, Side::Sell, 102, 12));
  REQUIRE(book.best_bid()->quantity == 10);
  REQUIRE(book.best_ask()->quantity == 12);

  book.apply(event(MarketAction::Fill, 2, Side::None, std::nullopt, 5));
  REQUIRE(book.best_ask()->quantity == 7);
  REQUIRE(book.order_count() == 2);
  book.apply(event(MarketAction::Fill, 2, Side::None, std::nullopt, 7));
  REQUIRE_FALSE(book.best_ask().has_value());
  REQUIRE(book.order_count() == 1);

  book.apply(event(MarketAction::Modify, 1, Side::Sell, 103, 4));
  REQUIRE_FALSE(book.best_bid().has_value());
  REQUIRE(book.best_ask()->price == 103);
  REQUIRE(book.best_ask()->quantity == 4);

  book.apply(event(MarketAction::Trade, 0, Side::Buy, 103, 1));
  REQUIRE(book.total_trades() == 1);
  book.apply(event(MarketAction::Cancel, 1));
  REQUIRE(book.order_count() == 0);
  book.apply(event(MarketAction::Cancel, 999));
  REQUIRE(book.total_cancels() == 1);

  book.apply(event(MarketAction::Add, 3, Side::Buy, 99, 3));
  book.apply(event(MarketAction::Clear, 0));
  REQUIRE_FALSE(book.best_bid().has_value());
  REQUIRE_FALSE(book.best_ask().has_value());
  REQUIRE(book.order_count() == 0);
  REQUIRE(book.total_fills() == 2);
  REQUIRE(book.total_modifies() == 1);
  REQUIRE(book.total_clears() == 1);
}

TEST_CASE("Historical duplicate policy cannot leave stale moved liquidity",
          "[CoreMarket]") {
  TypedLimitOrderBook book;
  const TypedMarketDataEvent original =
      event(MarketAction::Add, 10, Side::Buy, 100, 5);
  book.apply(original);
  const auto original_revision = book.best_bid()->revision;
  book.apply(original);
  REQUIRE(book.total_adds() == 1);
  REQUIRE(book.best_bid()->revision == original_revision);

  REQUIRE(throws_book_error(
      [&] { book.apply(event(MarketAction::Add, 10, Side::Buy, 101, 7)); }));
  REQUIRE(book.order_count() == 1);
  REQUIRE(book.best_bid()->price == 100);
  REQUIRE(book.best_bid()->quantity == 5);
  REQUIRE_FALSE(book.level(Side::Buy, 101).has_value());
}

TEST_CASE("Historical top-N is ordered, bounded and tracks level revisions",
          "[CoreMarket]") {
  TypedLimitOrderBook book;
  book.apply(event(MarketAction::Add, 1, Side::Buy, 100, 1));
  book.apply(event(MarketAction::Add, 2, Side::Buy, 102, 2));
  book.apply(event(MarketAction::Add, 3, Side::Buy, 101, 3));
  book.apply(event(MarketAction::Add, 4, Side::Sell, 104, 4));
  book.apply(event(MarketAction::Add, 5, Side::Sell, 103, 5));

  const auto bids = book.top_bids(2);
  const auto asks = book.top_asks(1);
  REQUIRE(bids.size() == 2);
  REQUIRE(bids[0].price == 102);
  REQUIRE(bids[1].price == 101);
  REQUIRE(asks.size() == 1);
  REQUIRE(asks[0].price == 103);
  REQUIRE(book.top_asks(0).empty());

  const auto old_revision = book.level(Side::Buy, 100)->revision;
  book.apply(event(MarketAction::Cancel, 1));
  book.apply(event(MarketAction::Add, 6, Side::Buy, 100, 9));
  const auto new_revision = book.level(Side::Buy, 100)->revision;
  REQUIRE(new_revision > old_revision);

  const auto before_clear = book.book_revision();
  book.apply(event(MarketAction::Clear, 0));
  REQUIRE(book.book_revision() > before_clear);
  book.apply(event(MarketAction::Add, 7, Side::Buy, 100, 2));
  REQUIRE(book.level(Side::Buy, 100)->revision > before_clear);
}

TEST_CASE("HistoricalLOBStore isolates instruments", "[CoreMarket]") {
  HistoricalLOBStore store;
  store.apply(event(MarketAction::Add, 1, Side::Buy, 100, 3, 11));
  store.apply(event(MarketAction::Add, 1, Side::Sell, 200, 4, 22));

  REQUIRE(store.size() == 2);
  REQUIRE(store.find(11)->best_bid()->price == 100);
  REQUIRE_FALSE(store.find(11)->best_ask().has_value());
  REQUIRE(store.find(22)->best_ask()->price == 200);
  REQUIRE_FALSE(store.find(22)->best_bid().has_value());
  REQUIRE(store.find(999) == nullptr);
  const auto ids = store.instrument_ids();
  REQUIRE(ids.size() == 2);
  REQUIRE(ids[0] == 11);
  REQUIRE(ids[1] == 22);
}

TEST_CASE("JSONL reader consumes a generated source incrementally",
          "[CoreMarket]") {
  cmf::TempFile file("backtester-core-market-stream.jsonl");
  {
    std::ofstream output(file.getPath());
    for (std::uint64_t sequence = 1; sequence <= 20'000; ++sequence) {
      output
          << R"({"ts_recv":"2026-04-07T09:00:00Z","hd":{"ts_event":"2026-04-07T09:00:00Z","instrument_id":1},"action":"T","side":"B","price":"1","size":1,"sequence":)"
          << sequence << "}\n";
    }
  }

  JsonlReader reader(file.getPath().string());
  TypedMarketDataEvent output;
  std::uint64_t count = 0;
  while (reader.next(output)) {
    ++count;
    REQUIRE(output.source_sequence == count);
  }
  REQUIRE(count == 20'000);
  REQUIRE(reader.row() == 20'000);
}
