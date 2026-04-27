#include "ingest/FeatherReader.hpp"

#include <stdexcept>

#ifdef BACK_TESTER_HAVE_FEATHER
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#endif

namespace cmf {

bool isFeatherPath(const std::filesystem::path &path) {
  return path.extension() == ".feather";
}

#ifndef BACK_TESTER_HAVE_FEATHER

IngestStats parseFeatherFile(const std::filesystem::path &path,
                             const MarketDataEventVisitor &) {
  throw std::runtime_error(
      "parseFeatherFile: Feather ingest unavailable for " + path.string() +
      ". Run `conan install . --build=missing -s build_type=Release` and reconfigure CMake.");
}

IngestStats parseFeatherFile(const std::filesystem::path &path,
                             const MarketDataEventConsumer &) {
  throw std::runtime_error(
      "parseFeatherFile: Feather ingest unavailable for " + path.string() +
      ". Run `conan install . --build=missing -s build_type=Release` and reconfigure CMake.");
}

#else

namespace {

std::runtime_error arrowFailure(const std::string &context,
                                const arrow::Status &status) {
  return std::runtime_error(context + ": " + status.ToString());
}

template <class T>
T requireResult(const arrow::Result<T> &result, const std::string &context) {
  if (!result.ok()) {
    throw arrowFailure(context, result.status());
  }
  return result.ValueUnsafe();
}

std::shared_ptr<arrow::Array>
requireColumn(const std::shared_ptr<arrow::RecordBatch> &batch,
              const std::string &name) {
  const int index = batch->schema()->GetFieldIndex(name);
  if (index < 0) {
    throw std::runtime_error("parseFeatherFile: missing column `" + name + "`");
  }
  return batch->column(index);
}

template <class ArrayType>
std::shared_ptr<ArrayType>
requireArray(const std::shared_ptr<arrow::RecordBatch> &batch,
             const std::string &name) {
  auto array = std::dynamic_pointer_cast<ArrayType>(requireColumn(batch, name));
  if (!array) {
    throw std::runtime_error("parseFeatherFile: unexpected type for `" + name +
                             "`; reconvert with scripts/convert_to_feather.py");
  }
  return array;
}

MdAction toAction(std::uint8_t value) {
  switch (static_cast<char>(value)) {
  case 'A':
    return MdAction::Add;
  case 'M':
    return MdAction::Modify;
  case 'C':
    return MdAction::Cancel;
  case 'R':
    return MdAction::Clear;
  case 'T':
    return MdAction::Trade;
  case 'F':
    return MdAction::Fill;
  default:
    return MdAction::None;
  }
}

MdSide toSide(std::uint8_t value) {
  switch (static_cast<char>(value)) {
  case 'A':
    return MdSide::Ask;
  case 'B':
    return MdSide::Bid;
  default:
    return MdSide::None;
  }
}

} // namespace

IngestStats parseFeatherFile(const std::filesystem::path &path,
                             const MarketDataEventVisitor &visitor) {
  const auto file =
      requireResult(arrow::io::ReadableFile::Open(path.string()),
                    "parseFeatherFile: cannot open " + path.string());
  const auto reader = requireResult(
      arrow::ipc::feather::Reader::Open(file),
      "parseFeatherFile: cannot open Feather reader for " + path.string());

  std::shared_ptr<arrow::Table> table;
  if (const auto status = reader->Read(&table); !status.ok()) {
    throw arrowFailure("parseFeatherFile: cannot read Feather table for " +
                           path.string(),
                       status);
  }

  const auto batch = requireResult(
      table->CombineChunksToBatch(),
      "parseFeatherFile: cannot combine Feather chunks for " + path.string());

  const auto ts_recv = requireArray<arrow::UInt64Array>(batch, "ts_recv");
  const auto ts_event = requireArray<arrow::UInt64Array>(batch, "ts_event");
  const auto rtype = requireArray<arrow::UInt8Array>(batch, "rtype");
  const auto publisher_id =
      requireArray<arrow::UInt16Array>(batch, "publisher_id");
  const auto instrument_id =
      requireArray<arrow::UInt32Array>(batch, "instrument_id");
  const auto order_id = requireArray<arrow::UInt64Array>(batch, "order_id");
  const auto action = requireArray<arrow::UInt8Array>(batch, "action");
  const auto side = requireArray<arrow::UInt8Array>(batch, "side");
  const auto price = requireArray<arrow::Int64Array>(batch, "price");
  const auto size = requireArray<arrow::UInt32Array>(batch, "size");
  const auto channel_id = requireArray<arrow::UInt8Array>(batch, "channel_id");
  const auto flags = requireArray<arrow::UInt8Array>(batch, "flags");
  const auto ts_in_delta = requireArray<arrow::Int32Array>(batch, "ts_in_delta");
  const auto sequence = requireArray<arrow::UInt32Array>(batch, "sequence");

  IngestStats stats{};
  for (int64_t i = 0; i < batch->num_rows(); ++i) {
    if (!rtype->IsNull(i) && rtype->Value(i) != RTYPE_MBO) {
      ++stats.skipped_rtype;
      continue;
    }

    MarketDataEvent event{};
    if (ts_recv->IsNull(i) || ts_event->IsNull(i) || publisher_id->IsNull(i) ||
        instrument_id->IsNull(i) || order_id->IsNull(i) || action->IsNull(i) ||
        side->IsNull(i) || size->IsNull(i) || channel_id->IsNull(i) ||
        flags->IsNull(i) || ts_in_delta->IsNull(i) || sequence->IsNull(i)) {
      ++stats.skipped_parse;
      continue;
    }

    event.ts_recv = ts_recv->Value(i);
    event.ts_event = ts_event->Value(i);
    event.publisher_id = publisher_id->Value(i);
    event.instrument_id = instrument_id->Value(i);
    event.order_id = order_id->Value(i);
    event.action = toAction(action->Value(i));
    event.side = toSide(side->Value(i));
    event.price = price->IsNull(i) ? UNDEF_PRICE : price->Value(i);
    event.size = size->Value(i);
    event.channel_id = channel_id->Value(i);
    event.flags = flags->Value(i);
    event.ts_in_delta = ts_in_delta->Value(i);
    event.sequence = sequence->Value(i);

    if (stats.consumed == 0) {
      stats.first_ts_recv = event.ts_recv;
    } else if (event.ts_recv < stats.last_ts_recv) {
      ++stats.out_of_order_ts_recv;
    }
    stats.last_ts_recv = event.ts_recv;
    ++stats.consumed;
    if (!visitor(event)) {
      return stats;
    }
  }

  return stats;
}

IngestStats parseFeatherFile(const std::filesystem::path &path,
                             const MarketDataEventConsumer &consumer) {
  return parseFeatherFile(
      path, MarketDataEventVisitor{[&](const MarketDataEvent &event) {
        consumer(event);
        return true;
      }});
}

#endif

} // namespace cmf
