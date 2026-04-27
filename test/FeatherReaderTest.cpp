#include "ingest/FeatherReader.hpp"
#include "ingest/MarketDataFile.hpp"
#include "TempFile.hpp"

#include <catch2/catch_test_macros.hpp>

#ifdef BACK_TESTER_HAVE_FEATHER
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#endif

#include <vector>

using namespace cmf;

#ifdef BACK_TESTER_HAVE_FEATHER
namespace {

void requireOk(const arrow::Status &status, const char *context) {
  if (!status.ok()) {
    FAIL(context << ": " << status.ToString());
  }
}

template <class Builder, class Values>
std::shared_ptr<arrow::Array> buildArray(Builder &builder, const Values &values) {
  requireOk(builder.AppendValues(values), "AppendValues");
  std::shared_ptr<arrow::Array> out;
  requireOk(builder.Finish(&out), "Finish");
  return out;
}

} // namespace
#endif

TEST_CASE("parseFeatherFile round-trips typed Feather rows", "[ingest][feather]") {
#ifndef BACK_TESTER_HAVE_FEATHER
  TempFile tmp("feather_reader_disabled.feather");
  REQUIRE_THROWS(parseFeatherFile(tmp.getPath(), [](const MarketDataEvent &) {}));
#else
  TempFile tmp("feather_reader_test.feather");

  arrow::UInt64Builder ts_recv_builder;
  arrow::UInt64Builder ts_event_builder;
  arrow::UInt8Builder rtype_builder;
  arrow::UInt16Builder publisher_builder;
  arrow::UInt32Builder instrument_builder;
  arrow::UInt64Builder order_builder;
  arrow::UInt8Builder action_builder;
  arrow::UInt8Builder side_builder;
  arrow::Int64Builder price_builder;
  arrow::UInt32Builder size_builder;
  arrow::UInt8Builder channel_builder;
  arrow::UInt8Builder flags_builder;
  arrow::Int32Builder delta_builder;
  arrow::UInt32Builder sequence_builder;
  arrow::StringBuilder symbol_builder;

  std::vector<std::shared_ptr<arrow::Array>> arrays{
      buildArray(ts_recv_builder,
                 std::vector<std::uint64_t>{100, 105, 103}),
      buildArray(ts_event_builder,
                 std::vector<std::uint64_t>{90, 95, 93}),
      buildArray(rtype_builder,
                 std::vector<std::uint8_t>{RTYPE_MBO, 1, RTYPE_MBO}),
      buildArray(publisher_builder,
                 std::vector<std::uint16_t>{7, 7, 7}),
      buildArray(instrument_builder,
                 std::vector<std::uint32_t>{11, 11, 12}),
      buildArray(order_builder,
                 std::vector<std::uint64_t>{1001, 1002, 1003}),
      buildArray(action_builder,
                 std::vector<std::uint8_t>{'A', 'A', 'C'}),
      buildArray(side_builder,
                 std::vector<std::uint8_t>{'B', 'A', 'N'}),
      buildArray(price_builder,
                 std::vector<std::int64_t>{1'000'000'000LL, 2'000'000'000LL,
                                            UNDEF_PRICE}),
      buildArray(size_builder, std::vector<std::uint32_t>{5, 6, 1}),
      buildArray(channel_builder, std::vector<std::uint8_t>{1, 1, 1}),
      buildArray(flags_builder, std::vector<std::uint8_t>{0, 0, 0}),
      buildArray(delta_builder, std::vector<std::int32_t>{0, 0, 0}),
      buildArray(sequence_builder, std::vector<std::uint32_t>{1, 2, 3}),
  };
  requireOk(symbol_builder.Append("AAA"), "Append symbol 1");
  requireOk(symbol_builder.Append("BBB"), "Append symbol 2");
  requireOk(symbol_builder.Append("CCC"), "Append symbol 3");
  std::shared_ptr<arrow::Array> symbol_array;
  requireOk(symbol_builder.Finish(&symbol_array), "Finish symbol");
  arrays.push_back(symbol_array);

  auto schema = arrow::schema(
      {
          arrow::field("ts_recv", arrow::uint64()),
          arrow::field("ts_event", arrow::uint64()),
          arrow::field("rtype", arrow::uint8()),
          arrow::field("publisher_id", arrow::uint16()),
          arrow::field("instrument_id", arrow::uint32()),
          arrow::field("order_id", arrow::uint64()),
          arrow::field("action", arrow::uint8()),
          arrow::field("side", arrow::uint8()),
          arrow::field("price", arrow::int64()),
          arrow::field("size", arrow::uint32()),
          arrow::field("channel_id", arrow::uint8()),
          arrow::field("flags", arrow::uint8()),
          arrow::field("ts_in_delta", arrow::int32()),
          arrow::field("sequence", arrow::uint32()),
          arrow::field("symbol", arrow::utf8()),
      });

  auto table = arrow::Table::Make(schema, arrays);
  auto sink = arrow::io::FileOutputStream::Open(tmp.getPath().string());
  REQUIRE(sink.ok());
  requireOk(arrow::ipc::feather::WriteTable(*table, sink.ValueUnsafe().get()),
            "WriteTable");
  requireOk(sink.ValueUnsafe()->Close(), "Close sink");

  std::vector<MarketDataEvent> seen;
  const auto stats =
      parseMarketDataFile(tmp.getPath(), [&](const MarketDataEvent &event) {
        seen.push_back(event);
      });

  REQUIRE(stats.consumed == 2);
  REQUIRE(stats.skipped_rtype == 1);
  REQUIRE(stats.out_of_order_ts_recv == 0);
  REQUIRE(seen.size() == 2);
  REQUIRE(seen[0].order_id == 1001);
  REQUIRE(seen[0].action == MdAction::Add);
  REQUIRE(seen[0].side == MdSide::Bid);
  REQUIRE(seen[1].order_id == 1003);
  REQUIRE(seen[1].action == MdAction::Cancel);
  REQUIRE(seen[1].instrument_id == 12);
#endif
}
