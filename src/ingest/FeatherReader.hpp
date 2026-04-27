// Feather (Arrow IPC file) -> MarketDataEvent reader.

#pragma once

#include "ingest/NdjsonReader.hpp"

namespace cmf {

bool isFeatherPath(const std::filesystem::path &path);

IngestStats parseFeatherFile(const std::filesystem::path &path,
                             const MarketDataEventVisitor &visitor);

IngestStats parseFeatherFile(const std::filesystem::path &path,
                             const MarketDataEventConsumer &consumer);

} // namespace cmf
