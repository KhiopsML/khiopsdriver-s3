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
	Aws::String bucketname_;
	Aws::String filename_;
	tOffset offset_{0};
	// Added for multifile support
	tOffset common_header_length_{0};
	Aws::Vector<Aws::String> filenames_;
	Aws::Vector<tOffset> cumulative_sizes_;
	tOffset total_size_{0};

	MultiPartFile() = default;
	explicit MultiPartFile(Aws::String bucket, Aws::String filename, tOffset offset, tOffset common_header_length,
			       Aws::Vector<Aws::String> filenames, Aws::Vector<tOffset> cumulative_sizes)
	    : bucketname_{std::move(bucket)}, filename_{std::move(filename)}, offset_{offset},
	      common_header_length_{common_header_length}, filenames_{std::move(filenames)},
	      cumulative_sizes_{std::move(cumulative_sizes)}, total_size_{cumulative_sizes_.back()}
	{
	}
};

using Parts = Aws::Vector<Aws::S3::Model::CompletedPart>;

struct WriteFile
{
	static constexpr size_t buff_min_ = 5 * 1024 * 1024;
	static constexpr size_t buff_max_ = buff_min_ * 1024;

	Aws::S3::Model::CreateMultipartUploadResult writer_;
	Aws::Vector<unsigned char> buffer_;
	Parts parts_;
	Aws::String bucketname_;
	Aws::String filename_;
	Aws::String append_target_;
	int part_tracker_{1};

	WriteFile() = default;
	WriteFile(Aws::S3::Model::CreateMultipartUploadResult&& create_upload_result)
	    : writer_{std::move(create_upload_result)}, bucketname_{writer_.GetBucket()}, filename_{writer_.GetKey()}
	{
		buffer_.reserve(buff_min_);
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

using ReaderPtr = Aws::UniquePtr<Reader>;
using WriterPtr = Aws::UniquePtr<Writer>;

template <typename T> using HandleContainer = Aws::Vector<T>;

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

	VISIBLE void test_setClient(Aws::UniquePtr<Aws::S3::S3Client>&& mock_client);

	VISIBLE void test_unsetClient();

	VISIBLE void test_clearHandles();

	VISIBLE void test_cleanupClient();

	VISIBLE void* test_getActiveReaderHandles();

	VISIBLE void* test_getActiveWriterHandles();

	VISIBLE bool test_compareFiles(const char* local_file_path, const char* s3_uri);
	
#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
