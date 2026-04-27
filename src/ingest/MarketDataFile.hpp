// Input-format dispatch for market-data files and folders.

#pragma once

#include "ingest/NdjsonReader.hpp"

namespace cmf {

enum class InputFormat {
  Ndjson,
  Feather,
};

InputFormat detectInputFormat(const std::filesystem::path &path);
const char *inputFormatName(InputFormat format) noexcept;

IngestStats parseMarketDataFile(const std::filesystem::path &path,
                                const MarketDataEventVisitor &visitor);

IngestStats parseMarketDataFile(const std::filesystem::path &path,
                                const MarketDataEventConsumer &consumer);

std::vector<std::filesystem::path>
listMarketDataFiles(const std::filesystem::path &folder);

} // namespace cmf
