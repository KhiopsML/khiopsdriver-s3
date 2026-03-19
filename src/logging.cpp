#include "logging.hpp"
#include "khiops_driver_common/logging.hpp"
#include "khiops_driver_common/util.hpp"
#include <memory>
#include <spdlog/spdlog.h>
#include <string>

using namespace std;

namespace s3plugin {
namespace logging {

static string loglevel;
static string logfile;
static shared_ptr<spdlog::logger> logger;
namespace {
struct LazyLoggerInitializer {
  LazyLoggerInitializer() {
    loglevel = khiops_driver_common::util::env::GetEnvVarOrDefault("S3_DRIVER_LOGLEVEL", "off", true);
    logfile = khiops_driver_common::util::env::GetEnvVar("S3_DRIVER_LOGFILE", true);
    logger = khiops_driver_common::logging::getLogger("s3driver", loglevel, logfile, false);
  }
};
} // anonymous namespace

const shared_ptr<spdlog::logger> &getLogger() {
  static LazyLoggerInitializer _;
  return logger;
}

} // namespace logging
} // namespace s3plugin