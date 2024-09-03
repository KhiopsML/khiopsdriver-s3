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
} // namespace s3plugin