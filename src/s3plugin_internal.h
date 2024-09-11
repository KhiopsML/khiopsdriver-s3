#pragma once

#include <aws/s3/S3Client.h>
#include <aws/s3/model/CompletedPart.h>

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

using Parts = std::vector<Aws::S3::Model::CompletedPart>;

struct WriteFile
{
	static constexpr size_t buff_min_ = 5 * 1024 * 1024;
	static constexpr size_t buff_max_ = buff_min_ * 1024;

	Aws::S3::Model::CreateMultipartUploadResult writer_;
	std::vector<unsigned char> buffer_;
	Parts parts_;
	std::string bucketname_;
	std::string filename_;
	std::string append_target_;
	int part_tracker_{1};

	WriteFile() = default;
	WriteFile(Aws::S3::Model::CreateMultipartUploadResult&& create_upload_result)
	    : writer_{std::move(create_upload_result)}, buffer_(buff_min_), bucketname_{writer_.GetBucket()},
	      filename_{writer_.GetKey()}
	{
	}

	~WriteFile() = default;
	// do not allow copies, too expensive
	WriteFile(const WriteFile&) = delete;
	WriteFile& operator=(const WriteFile&) = delete;
	WriteFile(WriteFile&&) noexcept = default;
	WriteFile& operator=(WriteFile&&) noexcept = default;
};

using Reader = MultiPartFile;
using Writer = WriteFile;

using ReaderPtr = std::unique_ptr<Reader>;
using WriterPtr = std::unique_ptr<Writer>;

template <typename T> using HandleContainer = std::vector<T>;

template <typename T> using HandleIt = typename HandleContainer<T>::iterator;

template <typename T> using HandlePtr = typename HandleContainer<T>::value_type;
} // namespace s3plugin

// Driver state manipulations for tests

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

	VISIBLE void* test_getActiveReaderHandles();

	VISIBLE void* test_getActiveWriterHandles();

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */