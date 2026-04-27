#include "ingest/MarketDataFile.hpp"

#include "ingest/FeatherReader.hpp"

#include <algorithm>
#include <stdexcept>

namespace cmf {

namespace {

bool isNdjsonPath(const std::filesystem::path &path) {
  const auto ext = path.extension().string();
  return ext == ".json" || ext == ".ndjson";
}

} // namespace

InputFormat detectInputFormat(const std::filesystem::path &path) {
  if (isFeatherPath(path)) {
    return InputFormat::Feather;
  }
  if (isNdjsonPath(path)) {
    return InputFormat::Ndjson;
  }
  throw std::runtime_error("unsupported market data file: " + path.string());
}

const char *inputFormatName(InputFormat format) noexcept {
  switch (format) {
  case InputFormat::Ndjson:
    return "json";
  case InputFormat::Feather:
    return "feather";
  }
  return "unknown";
}

IngestStats parseMarketDataFile(const std::filesystem::path &path,
                                const MarketDataEventVisitor &visitor) {
  switch (detectInputFormat(path)) {
  case InputFormat::Ndjson:
    return parseNdjsonFile(path, visitor);
  case InputFormat::Feather:
    return parseFeatherFile(path, visitor);
  }
  throw std::runtime_error("unsupported market data file: " + path.string());
}

IngestStats parseMarketDataFile(const std::filesystem::path &path,
                                const MarketDataEventConsumer &consumer) {
  return parseMarketDataFile(
      path, MarketDataEventVisitor{[&](const MarketDataEvent &event) {
        consumer(event);
        return true;
      }});
}

std::vector<std::filesystem::path>
listMarketDataFiles(const std::filesystem::path &folder) {
  if (!std::filesystem::exists(folder)) {
    throw std::runtime_error("ingestFolder: path does not exist: " +
                             folder.string());
  }
  if (!std::filesystem::is_directory(folder)) {
    throw std::runtime_error("ingestFolder: expected directory: " +
                             folder.string());
  }

  std::vector<std::filesystem::path> files;
  std::optional<InputFormat> expected_format;
  for (const auto &entry : std::filesystem::directory_iterator(folder)) {
    if (!entry.is_regular_file()) {
      continue;
    }
    if (!isNdjsonPath(entry.path()) && !isFeatherPath(entry.path())) {
      continue;
    }
    const InputFormat format = detectInputFormat(entry.path());
    if (!expected_format.has_value()) {
      expected_format = format;
    } else if (*expected_format != format) {
      throw std::runtime_error("ingestFolder: mixed input formats in " +
                               folder.string());
    }
    files.push_back(entry.path());
  }

  std::sort(files.begin(), files.end());
  if (files.empty()) {
    throw std::runtime_error("ingestFolder: no .json, .ndjson, or .feather files in " +
                             folder.string());
  }
  return files;
}

} // namespace cmf
