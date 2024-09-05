#pragma once

#include <memory>
#include <string>
#include <vector>

namespace s3plugin
{
constexpr int kSuccess{1};
constexpr int kFailure{0};

constexpr int kCloseSuccess{0};
constexpr int kCloseEOF{-1};

constexpr int kFalse{0};
constexpr int kTrue{1};

constexpr long long kBadSize{-1};

using tOffset = long long;

struct MultiPartFile
{
	std::string bucketname_;
	std::string filename_;
	tOffset offset_{0};
	// Added for multifile support
	tOffset common_header_length_{0};
	std::vector<std::string> filenames_;
	std::vector<tOffset> cumulative_sizes_;
	tOffset total_size_{0};
};

using ReaderPtr = std::unique_ptr<MultiPartFile>;

using HandleContainer = std::vector<ReaderPtr>;
using HandleIt = HandleContainer::iterator;
using HandlePtr = HandleContainer::value_type;
} // namespace s3plugin

// Driver state manipulations for tests

#include <aws/s3/S3Client.h>

#if defined(__unix__) || defined(__unix) || (defined(__APPLE__) && defined(__MACH__))
#define __unix_or_mac__
#else
#define __windows__
#endif

#ifdef __unix_or_mac__
#define VISIBLE __attribute__((visibility("default")))
#else
/* Windows Visual C++ only */
#define VISIBLE __declspec(dllexport)
#endif

/* Use of C linkage from C++ */
#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

	VISIBLE void test_setClient(Aws::S3::S3Client* mock_client);

	VISIBLE void test_unsetClient();

	VISIBLE void test_clearHandles();

	VISIBLE void test_cleanupClient();

	VISIBLE void* test_getActiveHandles();

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */