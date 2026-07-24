#include "market/Parsing.hpp"

#include <cctype>
#include <limits>
#include <stdexcept>
#include <string>

namespace cmf::market {
namespace {

[[nodiscard]] bool is_leap_year(int year) noexcept {
  return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
}

[[nodiscard]] int days_in_month(int year, int month) noexcept {
  constexpr int month_days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  if (month == 2 && is_leap_year(year)) {
    return 29;
  }
  return month_days[month - 1];
}

// Howard Hinnant's civil-calendar conversion, with 1970-01-01 as day zero.
[[nodiscard]] constexpr std::int64_t days_from_civil(int year, unsigned month,
                                                     unsigned day) noexcept {
  year -= month <= 2U ? 1 : 0;
  const std::int64_t era =
      (year >= 0 ? year : year - 399) / static_cast<std::int64_t>(400);
  const unsigned year_of_era =
      static_cast<unsigned>(year - static_cast<int>(era * 400));
  const unsigned shifted_month = month > 2U ? month - 3U : month + 9U;
  const unsigned day_of_year = (153U * shifted_month + 2U) / 5U + day - 1U;
  const unsigned day_of_era =
      year_of_era * 365U + year_of_era / 4U - year_of_era / 100U + day_of_year;
  return era * 146097 + static_cast<std::int64_t>(day_of_era) - 719468;
}

[[nodiscard]] int parse_fixed_int(std::string_view text, std::size_t offset,
                                  std::size_t count, const char *name) {
  int value = 0;
  for (std::size_t index = offset; index < offset + count; ++index) {
    if (index >= text.size() ||
        std::isdigit(static_cast<unsigned char>(text[index])) == 0) {
      throw std::invalid_argument(std::string("invalid ISO-8601 ") + name);
    }
    value = value * 10 + (text[index] - '0');
  }
  return value;
}

[[nodiscard]] std::int64_t
checked_multiply(std::int64_t left, std::int64_t right, const char *message) {
  if (left > 0 && right > 0 &&
      left > std::numeric_limits<std::int64_t>::max() / right) {
    throw std::out_of_range(message);
  }
  if (left < 0 && right > 0 &&
      left < std::numeric_limits<std::int64_t>::min() / right) {
    throw std::out_of_range(message);
  }
  return left * right;
}

} // namespace

PriceTicks parse_decimal_ticks(std::string_view text, PriceTicks price_scale,
                               PriceTicks tick_size_ticks) {
  if (text.empty()) {
    throw std::invalid_argument("price is empty");
  }
  if (price_scale <= 0 || tick_size_ticks <= 0) {
    throw std::invalid_argument("price scale and tick size must be positive");
  }

  bool negative = false;
  std::size_t position = 0;
  if (text.front() == '-' || text.front() == '+') {
    negative = text.front() == '-';
    position = 1;
  }
  if (position == text.size()) {
    throw std::invalid_argument("price has no digits");
  }

  std::int64_t significand = 0;
  std::int64_t denominator = 1;
  bool seen_decimal = false;
  std::size_t digit_count = 0;
  for (; position < text.size(); ++position) {
    const char character = text[position];
    if (character == '.' && !seen_decimal) {
      seen_decimal = true;
      continue;
    }
    if (std::isdigit(static_cast<unsigned char>(character)) == 0) {
      throw std::invalid_argument("price contains a non-decimal character");
    }
    if (significand > (std::numeric_limits<std::int64_t>::max() - 9) / 10) {
      throw std::out_of_range("price significand overflows int64");
    }
    significand = significand * 10 + (character - '0');
    ++digit_count;
    if (seen_decimal) {
      if (denominator > std::numeric_limits<std::int64_t>::max() / 10) {
        throw std::out_of_range("price precision is too large");
      }
      denominator *= 10;
    }
  }
  if (digit_count == 0 || text.back() == '.') {
    throw std::invalid_argument("price has invalid decimal syntax");
  }

  const std::int64_t scaled = checked_multiply(significand, price_scale,
                                               "scaled price overflows int64");
  if (scaled % denominator != 0) {
    throw std::invalid_argument("price cannot be represented by price scale");
  }

  std::int64_t ticks = scaled / denominator;
  if (negative) {
    ticks = -ticks;
  }
  if (ticks % tick_size_ticks != 0) {
    throw std::invalid_argument("price is not aligned to tick size");
  }
  return ticks;
}

TimestampNs parse_iso8601_timestamp_ns(std::string_view text) {
  if (text.size() < 20 || text[4] != '-' || text[7] != '-' || text[10] != 'T' ||
      text[13] != ':' || text[16] != ':' || text.back() != 'Z') {
    throw std::invalid_argument("timestamp must be UTC ISO-8601 ending in Z");
  }

  const int year = parse_fixed_int(text, 0, 4, "year");
  const int month = parse_fixed_int(text, 5, 2, "month");
  const int day = parse_fixed_int(text, 8, 2, "day");
  const int hour = parse_fixed_int(text, 11, 2, "hour");
  const int minute = parse_fixed_int(text, 14, 2, "minute");
  const int second = parse_fixed_int(text, 17, 2, "second");

  if (month < 1 || month > 12 || day < 1 || day > days_in_month(year, month) ||
      hour > 23 || minute > 59 || second > 59) {
    throw std::invalid_argument("timestamp contains an out-of-range field");
  }

  std::int64_t fractional_ns = 0;
  if (text.size() > 20) {
    if (text[19] != '.') {
      throw std::invalid_argument("timestamp has invalid fractional syntax");
    }
    const std::size_t fractional_digits = text.size() - 21;
    if (fractional_digits == 0 || fractional_digits > 9) {
      throw std::invalid_argument(
          "timestamp fractional precision must be 1 to 9 digits");
    }
    for (std::size_t index = 20; index + 1 < text.size(); ++index) {
      if (std::isdigit(static_cast<unsigned char>(text[index])) == 0) {
        throw std::invalid_argument("timestamp fraction contains a non-digit");
      }
      fractional_ns = fractional_ns * 10 + (text[index] - '0');
    }
    for (std::size_t index = fractional_digits; index < 9; ++index) {
      fractional_ns *= 10;
    }
  }

  constexpr std::int64_t seconds_per_day = 86'400;
  constexpr std::int64_t nanoseconds_per_second = 1'000'000'000;
  const std::int64_t days = days_from_civil(year, static_cast<unsigned>(month),
                                            static_cast<unsigned>(day));
  const std::int64_t day_seconds =
      days * seconds_per_day + hour * 3'600 + minute * 60 + second;
  const std::int64_t whole_ns =
      checked_multiply(day_seconds, nanoseconds_per_second,
                       "timestamp is outside int64 nanosecond range");
  if (whole_ns > std::numeric_limits<std::int64_t>::max() - fractional_ns) {
    throw std::out_of_range("timestamp is outside int64 nanosecond range");
  }
  return whole_ns + fractional_ns;
}

} // namespace cmf::market
