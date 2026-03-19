#pragma once

#include <memory>
#include <spdlog/spdlog.h>

namespace s3plugin {
namespace logging {

const std::shared_ptr<spdlog::logger> &getLogger();

} // namespace logging
} // namespace s3plugin