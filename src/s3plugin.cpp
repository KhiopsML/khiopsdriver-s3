#ifdef __CYGWIN__
#define _CRT_SECURE_NO_WARNINGS
#endif

#define S3_PLUGIN_EXPORT
#include "s3plugin.h"
#include "s3plugin_internal.h"
#include "khiops_driver_common/contrib.hpp"
#include "khiops_driver_common/logging.hpp"
#include "khiops_driver_common/util.hpp"
#include "contrib/ini.h"

#include "khiops_driver_common/logging.hpp"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartCopyRequest.h>
#include <aws/s3/model/UploadPartRequest.h>

#include <algorithm>
#include <assert.h>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <iomanip>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <aws/core/utils/logging/DefaultLogSystem.h>
#include <aws/core/utils/logging/ConsoleLogSystem.h>

using namespace Aws::Utils::Logging;
using namespace s3plugin;
using namespace khiops_driver_common;

namespace khiops_driver_common { spdlog::logger *GetLogger() { return GetLogger("s3driver", "S3_DRIVER_LOGFILE", "S3_DRIVER_LOGLEVEL"); } }

using S3Object = Aws::S3::Model::Object;

static int bIsConnected = false;

constexpr const char *version = DRIVER_VERSION;
constexpr const char *KHIOPS_S3 = "KHIOPS_S3";

static Aws::SDKOptions options;
static Aws::UniquePtr<Aws::S3::S3Client> client;

// Global bucket name
static Aws::String globalBucketName = "";

static HandleContainer<ReaderPtr> active_reader_handles;
static HandleContainer<WriterPtr> active_writer_handles;

// Error strings
static const char *ERR_NOT_CONNECTED = "Error: Driver is not connected.";
static const char *ERR_NULL_ARG = "Error passing null pointer to {}";
static const char *ERR_URL_PARSING = "Error parsing URL";

// test utilities

void test_setClient(Aws::UniquePtr<Aws::S3::S3Client> &&mock_client_ptr) {
  client = std::move(mock_client_ptr);
  bIsConnected = kTrue;
}

void test_unsetClient() {
  client.reset();
  bIsConnected = kFalse;
}

void test_clearHandles() {
  active_reader_handles.clear();
  active_writer_handles.clear();
}

void test_cleanupClient() {
  test_clearHandles();
  test_unsetClient();
}

void *test_getActiveReaderHandles() { return &active_reader_handles; }

void *test_getActiveWriterHandles() { return &active_writer_handles; }

template <typename RequestType>
RequestType MakeBaseRequest(const Aws::String &bucket,
                            const Aws::String &object) {
  RequestType request;
  request.WithBucket(bucket).WithKey(object);
  return request;
}

Aws::S3::Model::HeadObjectRequest
MakeHeadObjectRequest(const Aws::String &bucket, const Aws::String &object) {
  return MakeBaseRequest<Aws::S3::Model::HeadObjectRequest>(bucket, object);
}

Aws::S3::Model::GetObjectRequest
MakeGetObjectRequest(const Aws::String &bucket, const Aws::String &object,
                     const Aws::String &etag, Aws::String &&range = "") {
  auto request =
      MakeBaseRequest<Aws::S3::Model::GetObjectRequest>(bucket, object);
  if (!range.empty()) {
    request.SetRange(std::move(range));
  }
  if (!etag.empty()) {
    request.SetIfMatch(etag);
  }
  return request;
}

Aws::S3::Model::GetObjectOutcome GetObject(const Aws::String &bucket,
                                           const Aws::String &object,
                                           const Aws::String &etag,
                                           Aws::String &&range = "") {
  return client->GetObject(
      MakeGetObjectRequest(bucket, object, etag, std::move(range)));
}

Aws::S3::Model::HeadObjectOutcome HeadObject(const Aws::String &bucket,
                                             const Aws::String &object) {
  return client->HeadObject(MakeHeadObjectRequest(bucket, object));
}

template <typename H>
HandleIt<H> FindHandle(HandleContainer<H> &container, void *handle) {
  return std::find_if(container.begin(), container.end(), [handle](const H &h) {
    return handle == static_cast<void *>(h.get());
  });
}

template <typename H>
void EraseRemove(HandleContainer<H> &container, HandleIt<H> pos) {
  *pos = std::move(container.back());
  container.pop_back();
}

struct ParseUriResult {
  Aws::String bucket_;
  Aws::String object_;
};

using ObjectsVec = Aws::Vector<S3Object>;

bool parse_globbing_pattern(const std::string &pattern, std::string *prefix, std::string *suffix) {
    // 1) Output pointers must be non-null
    if (prefix == nullptr || suffix == nullptr) return false;

    // 2) Explicitly forbid "/" and "/*"
    if (pattern == "/" || pattern == "/*") return false;

    // 3) Forbid specific characters anywhere in the pattern
    //    Forbidden: '?', '!', '[', '^'
    if (pattern.find_first_of("?![^") != std::string::npos) return false;

    // 4) Exactly one '*'
    const std::size_t star_pos = pattern.find('*');
    if (star_pos == std::string::npos) return false;
    if (pattern.find('*', star_pos + 1) != std::string::npos) return false;

    // 5) Split
    *prefix = pattern.substr(0, star_pos);
    *suffix = pattern.substr(star_pos + 1);

    // 6) Prefix must be non-empty
    if (prefix->empty()) return false;

    // 7) Prefix must not end with a digit
    {
        const unsigned char c = static_cast<unsigned char>((*prefix)[prefix->size() - 1]);
        if (std::isdigit(c)) return false;
    }

    // 8) If suffix is non-empty, it must not start with a digit
    if (!suffix->empty()) {
        const unsigned char c = static_cast<unsigned char>((*suffix)[0]);
        if (std::isdigit(c)) return false;
    }

    return true;
}

// Definition of helper functions
Aws::String MakeByteRange(int64_t start, int64_t end) {
  Aws::StringStream range;
  range << "bytes=" << start << '-' << end;
  return range.str();
}

int DownloadFileRangeToVector(long long *size, const Aws::String &bucket,
                              const Aws::String &object_name,
                              Aws::Vector<unsigned char> &contentVector,
                              std::int64_t start_range, std::int64_t end_range,
                              const Aws::String &etag) {
  // Note: AWS byte ranges are inclusive
  auto request = MakeGetObjectRequest(bucket, object_name, etag,
                                      MakeByteRange(start_range, end_range));
  auto outcome = client->GetObject(request);
  if (!outcome.IsSuccess()) {
    GetLogger()->error(outcome.GetError().GetMessage());
    return -1;
  };

  Aws::IOStream &objectStream = outcome.GetResult().GetBody();
  std::string objectData((std::istreambuf_iterator<char>(objectStream)),
                         std::istreambuf_iterator<char>());

  // Convert string to vector<char>
  contentVector.assign(objectData.begin(), objectData.end());
  *size = static_cast<long long>(objectData.size());
  return 0;
}

int DownloadFileRangeToBuffer(long long *size, const Aws::String &bucket,
                              const Aws::String &object_name,
                              unsigned char *buffer, std::int64_t start_range,
                              std::int64_t end_range, const Aws::String &etag) {
  // Note: AWS byte ranges are inclusive
  auto request = MakeGetObjectRequest(bucket, object_name, etag,
                                      MakeByteRange(start_range, end_range));
  auto outcome = client->GetObject(request);
  if (!outcome.IsSuccess()) {
    GetLogger()->error(outcome.GetError().GetMessage());
    return -1;
  };

  // get ownership of the result and its underlying stream
  Aws::S3::Model::GetObjectResult result{outcome.GetResultWithOwnership()};
  auto &stream = result.GetBody();
  // remember comment above about inclusive byte ranges
  stream.read(reinterpret_cast<char *>(buffer), end_range - start_range + 1);

  if (stream.bad()) {
    GetLogger()->error("Failed to read stream content");
    return -1;
  }

  *size = stream.gcount();
  return 0;
}

int CheckEtagOnly(const MultiPartFile &mf, size_t idx) {
  if (idx >= mf.filenames_.size() || idx >= mf.etags_.size()) {
    GetLogger()->error("Invalid multipart index for ETag check.");
    return -1;
  }

  Aws::S3::Model::HeadObjectRequest req;
  req.SetBucket(mf.bucketname_);
  req.SetKey(mf.filenames_[idx]);
  req.SetIfMatch(mf.etags_[idx]);

  auto outcome = client->HeadObject(req);
  if (!outcome.IsSuccess()) {
    const auto &err = outcome.GetError();

    if (err.GetErrorType() == Aws::S3::S3Errors::INTERNAL_FAILURE) {
      GetLogger()->error("The file has been updated while reading it.");
      return -1;
    }

    GetLogger()->error(err.GetMessage().c_str());
    return -1;
  }

  return 0;
}

int ReadBytesInFile(long long *size, MultiPartFile &multifile,
                    unsigned char *buffer, tOffset to_read) {
  // Start at first usable file chunk
  // Advance through file chunks, advancing buffer pointer
  // Until last requested byte was read
  // Or error occured

  tOffset bytes_read{0LL};

  // Lookup item containing initial bytes at requested offset
  const auto &cumul_sizes = multifile.cumulative_sizes_;
  const tOffset common_header_length = multifile.common_header_length_;
  const Aws::String &bucket_name = multifile.bucketname_;
  const auto &filenames = multifile.filenames_;
  unsigned char *buffer_pos = buffer;
  tOffset &offset = multifile.offset_;
  // const tOffset offset_bak = offset; // in case of irrecoverable error, leave
  // the multifile in its starting state

  if (filenames.empty() || cumul_sizes.empty()) {
    GetLogger()->error("Cannot read from an empty multipart file.");
    return -1;
  }

  const tOffset total_size = cumul_sizes.back();

  if (offset >= total_size) {
    if (to_read == 0) {
      *size = 0LL;
      return 0;
    }

    GetLogger()->error("Cannot read after end of file.");
    return -1;
  }

  auto greater_than_offset_it =
      std::upper_bound(cumul_sizes.begin(), cumul_sizes.end(), offset);
  size_t idx = static_cast<size_t>(
      std::distance(cumul_sizes.begin(), greater_than_offset_it));

  // If offset is at/after the tracked end, route through the last file so that
  // generation consistency checks still run before returning EOF/out-of-range.
  if (idx == cumul_sizes.size()) {
    idx = cumul_sizes.size() - 1;
  }

  if (idx >= cumul_sizes.size() || idx >= filenames.size() ||
      idx >= multifile.etags_.size()) {
    GetLogger()->error("Cannot read after end of file.");
    return -1;
  }

  const tOffset range_end_for_log =
      (greater_than_offset_it == cumul_sizes.end()) ? cumul_sizes.back()
                                                    : *greater_than_offset_it;

  GetLogger()->debug("Use item {} to read @ {} (end = {})", idx, offset,
                     range_end_for_log);

  auto read_range_and_update = [&](long long *nread,
                                   const Aws::String &filename, tOffset start,
                                   tOffset end) -> int {
    const Aws::String &etag = multifile.etags_[idx];

    tOffset actual_read;
    if (DownloadFileRangeToBuffer(&actual_read, bucket_name, filename,
                                  buffer_pos, static_cast<int64_t>(start),
                                  static_cast<int64_t>(end), etag)) {
      GetLogger()->error("The file has been updated while reading it.");
      return -1;
    }

    GetLogger()->debug("read = {}", actual_read);

    bytes_read += actual_read;
    buffer_pos += actual_read;
    offset += actual_read;

    if (actual_read < (end - start + 1) /*expected read*/) {
      GetLogger()->debug("End of file encountered");
      to_read = 0;
    } else {
      to_read -= actual_read;
    }

    *nread = actual_read;
    return 0;
  };

  // first file read

  // AWS peculiarity: byte ranges are inclusive
  const tOffset file_start =
      (idx == 0) ? offset
                 : offset - cumul_sizes[idx - 1] + common_header_length;
  const tOffset read_end =
      std::min(file_start + to_read, file_start + cumul_sizes[idx] - offset) -
      1;
  if (read_end < file_start) {
    *size = 0LL;
    return 0;
  }

  long long nread;
  int code =
      read_range_and_update(&nread, filenames[idx], file_start, read_end);

  // continue with the next files
  while (!code && to_read) {
    // read the missing bytes in the next files as necessary
    if (idx + 1 >= cumul_sizes.size()) {
      to_read = 0;
      break;
    }
    idx++;
    const tOffset start = common_header_length;
    const tOffset end = std::min(start + to_read, start + cumul_sizes[idx] -
                                                      cumul_sizes[idx - 1]) -
                        1;
    if (end < start) {
      to_read = 0;
      break;
    }

    code = read_range_and_update(&nread, filenames[idx], start, end);
  }

  if (code) {
    return -1;
  }

  *size = bytes_read;
  return 0;
}

int ParseS3Uri(ParseUriResult *parsedUri, const Aws::String &s3_uri) {
  const Aws::String prefix = "s3://";
  const size_t prefix_size = prefix.size();
  if (s3_uri.compare(0, prefix_size, prefix) != 0) {
    GetLogger()->error("Invalid S3 URI: {}", s3_uri);
    return -1;
  }

  size_t pos = s3_uri.find('/', prefix_size);
  if (pos == std::string::npos) {
    GetLogger()->error("Invalid S3 URI, missing object name: {}", s3_uri);
    return -1;
  }

  Aws::String bucket_name = s3_uri.substr(prefix_size, pos - prefix_size);

  if (bucket_name.empty()) {
    if (globalBucketName.empty()) {
      GetLogger()->error(
          "No bucket specified, and GCS_BUCKET_NAME is not set!");
      return -1;
    }
    bucket_name = globalBucketName;
  }

  Aws::String object_name = s3_uri.substr(pos + 1);

  *parsedUri = ParseUriResult{std::move(bucket_name), std::move(object_name)};
  return 0;
}

bool IsMultifile(const Aws::String &pattern, size_t &first_special_char_idx) {
  GetLogger()->debug("Parse multifile pattern {}", pattern);

  constexpr auto special_chars = "*?![^";

  size_t from_offset = 0;
  size_t found_at = pattern.find_first_of(special_chars, from_offset);
  while (found_at != std::string::npos) {
    const char found = pattern[found_at];
    GetLogger()->debug("special char {} found at {}", found, found_at);

    if (found_at > 0 && pattern[found_at - 1] == '\\') {
      GetLogger()->debug("preceded by a \\, so not so special");
      from_offset = found_at + 1;
      found_at = pattern.find_first_of(special_chars, from_offset);
    } else {
      GetLogger()->debug("not preceded by a \\, so really a special char");
      first_special_char_idx = found_at;
      return true;
    }
  }
  return false;
}

Aws::S3::Model::ListObjectsV2Outcome ListObjects(const Aws::String &bucket,
                                                 const Aws::String &pattern) {
  Aws::S3::Model::ListObjectsV2Request request;
  request.WithBucket(bucket).WithPrefix(pattern).WithDelimiter("");
  return client->ListObjectsV2(request);
}

bool BlobDirectoryExists(const Aws::String &bucket, const Aws::String &object) {
  const auto head_object_outcome = HeadObject(bucket, object);
  if (head_object_outcome.IsSuccess()) {
    return true;
  }

  if (head_object_outcome.GetError().GetErrorType() !=
      Aws::S3::S3Errors::RESOURCE_NOT_FOUND) {
    GetLogger()->error("Failed retrieving directory info in dirExists: {}",
                       head_object_outcome.GetError().GetMessage());
    return false;
  }

  auto list_outcome = ListObjects(bucket, object);
  if (!list_outcome.IsSuccess()) {
    GetLogger()->error("Failed listing directory info in dirExists: {}",
                       list_outcome.GetError().GetMessage());
    return false;
  }

  return !list_outcome.GetResult().GetContents().empty();
}

bool CreateBlobDirectory(const Aws::String &bucket, const Aws::String &object) {
  Aws::S3::Model::PutObjectRequest request;
  request.WithBucket(bucket).WithKey(object);
  auto empty_body = Aws::MakeShared<Aws::StringStream>(KHIOPS_S3);
  request.SetBody(empty_body);

  const auto outcome = client->PutObject(request);
  if (!outcome.IsSuccess()) {
    const auto &err = outcome.GetError();
    GetLogger()->error("PutObject: {} {}", err.GetExceptionName(),
                       err.GetMessage());
  }

  return outcome.IsSuccess();
}

bool DeleteBlobDirectory(const Aws::String &bucket, const Aws::String &object) {
  Aws::S3::Model::ListObjectsV2Request request;
  request.WithBucket(bucket).WithPrefix(object).WithDelimiter("");

  Aws::String continuation_token;
  bool is_success = true;

  do {
    if (!continuation_token.empty()) {
      request.SetContinuationToken(continuation_token);
    }

    const auto outcome = client->ListObjectsV2(request);
    if (!outcome.IsSuccess()) {
      GetLogger()->error("Failed listing directory contents for rmdir: {}",
                         outcome.GetError().GetMessage());
      return false;
    }

    const auto &list_result = outcome.GetResult();
    const auto &objects = list_result.GetContents();

    Aws::S3::Model::DeleteObjectRequest delete_request;
    delete_request.WithBucket(bucket);
    for (const S3Object &object_to_delete : objects) {
      delete_request.WithKey(object_to_delete.GetKey());
      const auto delete_outcome = client->DeleteObject(delete_request);
      if (!delete_outcome.IsSuccess()) {
        is_success = false;
        const auto &err = delete_outcome.GetError();
        GetLogger()->error("DeleteObject: {} {}", err.GetExceptionName(),
                           err.GetMessage());
      }
    }

    continuation_token = list_result.GetContinuationToken();
  } while (!continuation_token.empty());

  return is_success;
}

// Get from a bucket a list of objects matching a name pattern.
// To get a limited list of objects to filter per request, the request includes
// a well defined prefix contained in the pattern
int FilterList(ObjectsVec *result, const Aws::String &bucket,
               const Aws::String &pattern, size_t pattern_1st_sp_char_pos) {
  GetLogger()->trace("FilterList(): bucket={}, pattern={}, pattern_1st_sp_char_pos={}", bucket, pattern, pattern_1st_sp_char_pos);
  ObjectsVec res;

  Aws::S3::Model::ListObjectsV2Request request;
  request.WithBucket(bucket).WithPrefix(
      pattern.substr(0, pattern_1st_sp_char_pos));
  Aws::String continuation_token;

  do {
    if (!continuation_token.empty()) {
      request.SetContinuationToken(continuation_token);
    }
    const Aws::S3::Model::ListObjectsV2Outcome outcome =
        client->ListObjectsV2(request);

    if (!outcome.IsSuccess()) {
      GetLogger()->error(outcome.GetError().GetMessage());
      return -1;
    };

    const auto &list_result = outcome.GetResult();
    const auto &objects = list_result.GetContents();
    std::copy_if(objects.begin(), objects.end(), std::back_inserter(res),
                 [&](const S3Object &obj) {
                   return GitignoreGlobMatch(
                       obj.GetKey(), pattern);
                 });
    continuation_token = list_result.GetContinuationToken();

  } while (!continuation_token.empty());

  *result = std::move(res);
  return 0;
}

bool WillSizeCountProductOverflow(size_t size, size_t count) {
  constexpr size_t max_prod_usable{
      static_cast<size_t>(std::numeric_limits<tOffset>::max())};
  return (max_prod_usable / size < count || max_prod_usable / count < size);
}

template <typename Request>
Request MakeBaseUploadRequest(const Writer &writer) {
  const auto &multipartupload_data = writer.writer_;

  return Request{}
      .WithBucket(multipartupload_data.GetBucket())
      .WithKey(multipartupload_data.GetKey())
      .WithUploadId(multipartupload_data.GetUploadId());
}

template <typename PartRequest>
PartRequest MakeBaseUploadPartRequest(const Writer &writer) {
  return MakeBaseUploadRequest<PartRequest>(writer).WithPartNumber(
      writer.part_tracker_);
}

Aws::S3::Model::UploadPartRequest
MakeUploadPartRequest(Writer &writer,
                      Aws::Utils::Stream::PreallocatedStreamBuf &pre_buf) {
  Aws::S3::Model::UploadPartRequest request =
      MakeBaseUploadPartRequest<Aws::S3::Model::UploadPartRequest>(writer);

  const auto body = Aws::MakeShared<Aws::IOStream>(KHIOPS_S3, &pre_buf);
  request.SetBody(body);
  return request;
}

Aws::S3::Model::UploadPartCopyRequest
MakeUploadPartCopyRequest(Writer &writer, const Aws::String &byte_range) {
  return MakeBaseUploadPartRequest<Aws::S3::Model::UploadPartCopyRequest>(
             writer)
      .WithCopySource(writer.append_target_)
      .WithCopySourceRange(byte_range);
}

Aws::S3::Model::CompleteMultipartUploadRequest
MakeCompleteMultipartUploadRequest(Writer &writer) {
  Aws::S3::Model::CompletedMultipartUpload request_body;
  request_body.SetParts(writer.parts_);

  return MakeBaseUploadRequest<Aws::S3::Model::CompleteMultipartUploadRequest>(
             writer)
      .WithMultipartUpload(std::move(request_body));
}

// Implementation of driver functions

const char *driver_getDriverName() { return "S3 driver"; }

const char *driver_getVersion() { return version; }

const char *driver_getScheme() { return "s3"; }

int driver_isReadOnly() { return kFalse; }

int driver_connect() {
  if (kTrue == bIsConnected) {
    GetLogger()->debug("Driver is already connected");
    return kOtherSuccess;
  }

  auto file_exists = [](const Aws::String &name) {
    Aws::IFStream ifile(name);
    return (ifile.is_open());
  };

  GetLogger()->debug("Connect");

  // Configuration: we honor both standard AWS config files and environment
  // variables If both configuration files and environment variables are set
  // precedence is given to environment variables
  Aws::String s3endpoint = "";
  Aws::String s3region = "us-east-1";

  // Note: this might be useless now since AWS SDK apparently allows setting
  // custom endpoints now...

  // Load AWS configuration from file
  Aws::Auth::AWSCredentials configCredentials;
  Aws::String userHome = GetEnvVarOrDefault("HOME", "");
  if (!userHome.empty()) {
    Aws::OStringStream defaultConfig_os;
    defaultConfig_os << userHome << "/.aws/config";
    const std::string defaultConfig = defaultConfig_os.str();

    const Aws::String configFile =
        GetEnvVarOrDefault("AWS_CONFIG_FILE", defaultConfig);
    GetLogger()->debug("Conf file = {}", configFile);

    if (file_exists(configFile)) {
      const Aws::String profile =
          GetEnvVarOrDefault("AWS_PROFILE", "default");

      GetLogger()->debug("Profile = {}", profile);

      const Aws::String profileSection =
          (profile != "default") ? "profile " + profile : profile;

      Aws::Auth::ProfileConfigFileAWSCredentialsProvider provider(
          profile.c_str());
      configCredentials = provider.GetAWSCredentials();

      mINI::INIFile file(configFile);
      mINI::INIStructure ini;
      file.read(ini);
      Aws::String confEndpoint = ini.get(profileSection).get("endpoint_url");
      if (!confEndpoint.empty()) {
        s3endpoint = std::move(confEndpoint);
      }
      GetLogger()->debug("Endpoint = {}", s3endpoint);

      Aws::String confRegion = ini.get(profileSection).get("region");
      if (!confRegion.empty()) {
        s3region = std::move(confRegion);
      }
      GetLogger()->debug("Region = {}", s3region);
    } else if (configFile != defaultConfig) {
      return kOtherFailure;
    }
  }

  // Initialize variables from environment
  // Both AWS_xxx standard variables and AutoML S3_xxx variables are supported
  // If both are present, AWS_xxx variables will be given precedence

  // Note: this behavior is normally the same as the one implemented by the SDK
  // except for the "S3_*" variables that are kept to support legacy
  // applications

  globalBucketName = GetEnvVarOrDefault("S3_BUCKET_NAME", "");
  s3endpoint = GetEnvVarOrDefault("S3_ENDPOINT", s3endpoint);
  s3endpoint = GetEnvVarOrDefault("AWS_ENDPOINT_URL", s3endpoint);
  s3region = GetEnvVarOrDefault("AWS_DEFAULT_REGION", s3region);
  Aws::String s3accessKey = GetEnvVarOrDefault("S3_ACCESS_KEY", "");
  s3accessKey = GetEnvVarOrDefault("AWS_ACCESS_KEY_ID", s3accessKey);
  Aws::String s3secretKey = GetEnvVarOrDefault("S3_SECRET_KEY", "");
  s3secretKey = GetEnvVarOrDefault("AWS_SECRET_ACCESS_KEY", s3secretKey);
  if ((s3accessKey != "" && s3secretKey == "") ||
      (s3accessKey == "" && s3secretKey != "")) {
    GetLogger()->error("Access key and secret configuration is only permitted "
                       "when both values are provided.");
    return kOtherFailure;
  }

  if (!GetEnvVarOrDefault("AWS_DEBUG_HTTP_LOGS", "").empty()) {
    options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Debug;
    options.loggingOptions.logger_create_fn = [] {
      return std::make_shared<ConsoleLogSystem>(LogLevel::Debug);
    };
  }

  // Initialisation du SDK AWS
  Aws::InitAPI(options);

  Aws::Client::ClientConfiguration clientConfig(true, "legacy", true);
  clientConfig.allowSystemProxy =
      !GetEnvVarOrDefault("http_proxy", "").empty() ||
      !GetEnvVarOrDefault("https_proxy", "").empty() ||
      !GetEnvVarOrDefault("HTTP_PROXY", "").empty() ||
      !GetEnvVarOrDefault("HTTPS_PROXY", "").empty() ||
      !GetEnvVarOrDefault("S3_ALLOW_SYSTEM_PROXY", "").empty();
  clientConfig.verifySSL = true;
  clientConfig.version = Aws::Http::Version::HTTP_VERSION_2TLS;
  if (s3endpoint != "") {
    clientConfig.endpointOverride = std::move(s3endpoint);
  }
  if (s3region != "") {
    clientConfig.region = s3region;
  }

  if (!s3accessKey.empty()) {
    configCredentials = Aws::Auth::AWSCredentials(s3accessKey, s3secretKey);
  }

#if defined(__linux__)
  if(FindCertificate(&clientConfig.caFile) != 0) return kOtherFailure;
#endif

  client = Aws::MakeUnique<Aws::S3::S3Client>(
      KHIOPS_S3, configCredentials,
      Aws::MakeShared<Aws::S3::S3EndpointProvider>(KHIOPS_S3), clientConfig);

  bIsConnected = true;
  return kOtherSuccess;
}

int driver_disconnect() {
  if (client) {
    // tie up loose ends
    Aws::Vector<Aws::S3::Model::AbortMultipartUploadOutcome> failures;
    for (auto h_it = active_writer_handles.begin();
         h_it != active_writer_handles.end();) {
      auto &writer = **h_it;
      auto outcome = client->AbortMultipartUpload(
          MakeBaseUploadRequest<Aws::S3::Model::AbortMultipartUploadRequest>(
              writer));

      if (outcome.IsSuccess()) {
        // delete the handle
        h_it = active_writer_handles.erase(h_it);
      } else {
        failures.push_back(std::move(outcome));
        h_it++;
      }
    }

    if (!failures.empty()) {
      Aws::OStringStream os;
      os << "Errors occured during disconnection:\n";
      for (const auto &outcome : failures) {
        os << outcome.GetError().GetMessage() << '\n';
      }
      GetLogger()->error(os.str());

      return kOtherFailure;
    }
  }

  active_writer_handles.clear();
  active_reader_handles.clear();

  client.reset();

  ShutdownAPI(options);

  bIsConnected = kFalse;

  return kOtherSuccess;
}

int driver_isConnected() { return bIsConnected; }

long long int driver_getSystemPreferredBufferSize() {
  constexpr long long buff_size = 4L * 1024L * 1024L;
  return buff_size; // 4 Mo
}

int driver_fileExists(const char *sFilePathName) {
  if (kFalse == bIsConnected) {
    GetLogger()->error(ERR_NOT_CONNECTED);
    return (kFalse);
  };

  if (!(sFilePathName)) {
    GetLogger()->error(ERR_NULL_ARG, __func__);
    return (kFalse);
  };

  GetLogger()->debug("fileExist {}", sFilePathName);

  ParseUriResult names;
  if (ParseS3Uri(&names, sFilePathName)) {
    GetLogger()->error(ERR_URL_PARSING);
    return kFalse;
  }

  size_t pattern_1st_sp_char_pos = 0;
  if (!IsMultifile(names.object_, pattern_1st_sp_char_pos)) {
    // go ahead with the simple request
    const auto head_object_outcome = HeadObject(names.bucket_, names.object_);
    if (head_object_outcome.GetError().GetErrorType() ==
        Aws::S3::S3Errors::RESOURCE_NOT_FOUND) {
      return kFalse;
    }
    if (!head_object_outcome.IsSuccess()) {
      GetLogger()->error("Failed retrieving file info in fileExists: {}",
                         head_object_outcome.GetError().GetMessage());
      return kFalse;
    }

    return kTrue;
  }

  // get a filtered list of the bucket files that match the pattern
  ObjectsVec filteredList;
  if (FilterList(&filteredList, names.bucket_, names.object_,
                 pattern_1st_sp_char_pos)) {
    GetLogger()->error("Error while filtering object list");
    return kFalse;
  }

  return filteredList.empty() ? kFalse : kTrue;
}

int driver_dirExists(const char *sFilePathName) {
  if (kFalse == bIsConnected) {
    GetLogger()->error(ERR_NOT_CONNECTED);
    return (kFalse);
  };

  if (!(sFilePathName)) {
    GetLogger()->error(ERR_NULL_ARG, __func__);
    return (kFalse);
  };

  GetLogger()->debug("dirExist {}", sFilePathName);

  ParseUriResult names;
  if (ParseS3Uri(&names, sFilePathName)) {
    GetLogger()->error(ERR_URL_PARSING);
    return kFalse;
  }

  return BlobDirectoryExists(names.bucket_, names.object_) ? kTrue : kFalse;

}

int GetOneFileSize(long long *size, const Aws::String &bucket,
                   const Aws::String &object) {
  const auto head_object_outcome = HeadObject(bucket, object);
  if (!head_object_outcome.IsSuccess()) {
    GetLogger()->error(head_object_outcome.GetError().GetMessage());
    return -1;
  };
  *size = head_object_outcome.GetResult().GetContentLength();
  return 0;
}

// Khiops allows header length to be max 8MB
constexpr int KHIOPS_MAX_HEADERLENGTH = 8 * 1024 * 1024;

int ReadHeader(Aws::String *header, const Aws::String &bucket,
               const S3Object &obj,
               int64_t max_length = KHIOPS_MAX_HEADERLENGTH) {
  auto request = MakeGetObjectRequest(bucket, obj.GetKey(), obj.GetETag(),
                                      MakeByteRange(0, max_length));
  auto outcome = client->GetObject(request);
  if (!outcome.IsSuccess()) {
    GetLogger()->error(outcome.GetError().GetMessage());
    return -1;
  };
  auto result = outcome.GetResultWithOwnership();
  Aws::IOStream &read_stream = result.GetBody();
  Aws::String line;
  std::getline(read_stream, line);
  if (read_stream.bad()) {
    GetLogger()->error("header read failed");
    return -1;
  }
  if (!read_stream.eof()) {
    line.push_back('\n');
  }
  if (line.empty()) {
    GetLogger()->error("Empty header");
    return -1;
  }
  *header = line;
  return 0;
}

// Sample a subset of objects for header detection (first, last, and some in
// middle) Deterministic: no randomness needed (no std::rand available)
std::set<std::string>
SelectObjectsSubset(std::vector<std::string> const &all_objects) {
  size_t total = all_objects.size();
  if (total == 0)
    return {};

  size_t first_count = std::min<size_t>(5, total);
  size_t last_count = total < 10 ? std::max<size_t>(0, total - first_count) : 5;

  size_t used = first_count + last_count;
  size_t random_count =
      10; // deterministic: pick evenly spaced samples from middle
  if (total < 20) {
    if (total > used) {
      random_count = total - used;
    } else {
      random_count = 0;
    }
  }

  std::set<std::string> result;

  // Add first elements
  for (size_t i = 0; i < first_count; ++i) {
    result.insert(all_objects[i]);
  }

  // Add last elements
  if (last_count > 0) {
    for (size_t i = total - last_count; i < total; ++i) {
      result.insert(all_objects[i]);
    }
  }

  // Deterministic sampling from the middle
  std::vector<std::string> middle;
  size_t middle_start = first_count;
  size_t middle_end = total - last_count;
  if (middle_start < middle_end) {
    middle.insert(middle.end(), all_objects.begin() + middle_start,
                  all_objects.begin() + middle_end);
  }

  if (!middle.empty() && random_count > 0) {
    // Deterministic even-spread sampling from the middle
    size_t middle_len = middle.size();
    size_t samples_to_take = std::min<size_t>(random_count, middle_len);
    for (size_t s = 0; s < samples_to_take; ++s) {
      // spread samples evenly across the middle
      size_t idx = (middle_len > 0) ? (s * middle_len) / samples_to_take : 0;
      result.insert(middle[idx]);
    }
  }

  GetLogger()->debug("Selected objects for header detection");
  for (auto const &name : result) {
    GetLogger()->debug(" {}", name);
    GetLogger()->info(" {}", name);
  }

  return result;
}

int getFileSize(long long *size, const Aws::String &bucket_name,
                const Aws::String &object_name) {
  // tweak the request for the object. if the object parameter is in fact a
  // pattern, the pattern could point to a list of objects that constitute a
  // whole file

  size_t pattern_1st_sp_char_pos = 0;
  if (!IsMultifile(object_name, pattern_1st_sp_char_pos)) {
    // go ahead with the simple request
    return GetOneFileSize(size, bucket_name, object_name);
  }

  ObjectsVec file_list;
  if (FilterList(&file_list, bucket_name, object_name,
                 pattern_1st_sp_char_pos)) {
    return -1;
  };

  if ((file_list).empty()) {
    GetLogger()->error("No match for the file pattern");
    return -1;
  };

  // build vector of filenames for sampling
  std::vector<std::string> filenames;
  filenames.reserve(file_list.size());
  for (const auto &obj : file_list) {
    filenames.push_back(obj.GetKey());
  }

  // get the size of the first file
  const S3Object &first_file = file_list.front();
  long long total_size = first_file.GetSize();

  // special case: one element
  if (file_list.size() == 1) {
    *size = total_size;
    return 0;
  }

  // sampling: pick representative files for header checks
  std::set<std::string> selected = SelectObjectsSubset(filenames);

  Aws::String header;
  if (ReadHeader(&header, bucket_name, first_file, KHIOPS_MAX_HEADERLENGTH)) {
    return -1;
  }

  const long long header_size = header.size();

  // scan the next files and adjust effective size if header is repeated
  int nb_headers_to_subtract = 0;
  bool same_header = true;

  for (size_t i = 1; i < file_list.size(); i++) {
    const Aws::S3::Model::Object &curr_file = file_list[i];
    const std::string &curr_key = curr_file.GetKey();
    if (same_header) {
      if (selected.find(curr_key) != selected.end()) {
        // Actually verify file contents for sampled file
        Aws::String curr_header;
        if (ReadHeader(&curr_header, bucket_name, curr_file, header_size)) {
          return -1;
        }

        same_header = (header == curr_header);
        if (same_header) {
          nb_headers_to_subtract++;
        }
      } else {
        // Only check filesize
        GetLogger()->debug("Skip header detect {} {} in pattern, expect min {}",
                           curr_key, file_list[i].GetSize(), header_size);
        same_header =
            (header_size <= static_cast<long long>(curr_file.GetSize()));
        if (same_header) {
          nb_headers_to_subtract++;
        }
      }
    }

    total_size += static_cast<long long>(curr_file.GetSize());
  }

  if (!same_header) {
    nb_headers_to_subtract = 0;
  }

  *size =
      total_size - static_cast<long long>(nb_headers_to_subtract) * header_size;
  return 0;
}

long long int driver_getFileSize(const char *filename) {
  if (kFalse == bIsConnected) {
    GetLogger()->error(ERR_NOT_CONNECTED);
    return (kFailure);
  };

  if (!(filename)) {
    GetLogger()->error(ERR_NULL_ARG, __func__);
    return (kFailure);
  };

  GetLogger()->debug("getFileSize {}", filename);

  ParseUriResult names;
  if (ParseS3Uri(&names, filename)) {
    GetLogger()->error(ERR_URL_PARSING);
    return kFailure;
  }
  long long size;
  if (getFileSize(&size, names.bucket_, names.object_)) {
    GetLogger()->error("Error getting file size");
    return kFailure;
  }
  return size;
}

int MakeReaderPtr(ReaderPtr *result, Aws::String bucketname,
                  Aws::String objectname) {
  size_t pattern_1st_sp_char_pos = 0;
  if (!IsMultifile(objectname, pattern_1st_sp_char_pos)) {
    auto head_outcome = HeadObject(bucketname, objectname);
    if (!head_outcome.IsSuccess()) {
      GetLogger()->error(head_outcome.GetError().GetMessage());
      return -1;
    };

    const auto &head = head_outcome.GetResult();
    long long size = head.GetContentLength();
    Aws::String etag = head.GetETag();

    Aws::Vector<Aws::String> objectnames(1, objectname);
    Aws::Vector<tOffset> sizes(1, size);
    Aws::Vector<Aws::String> etags(1, etag);

    *result = Aws::MakeUnique<Reader>(
        KHIOPS_S3, std::move(bucketname), std::move(objectname), 0, 0,
        std::move(objectnames), std::move(sizes), std::move(etags));
    return 0;
  }

  // this is a multifile. the reader object needs the list of filenames matching
  // the globbing pattern and their metadata, mainly their respective sizes.

  // Note: getting the metadata involves a tradeoff between memory size of the
  // data kept and the amount of data copied: storing only the relevant data in
  // the MultiPartFile struct requires another copy of each object name, since
  // the API does not allow moving from its own Object types. These copies could
  // be avoided by keeping the entire list of Objects, at the cost of the space
  // used by the other metadata. The implementation here will save that space.

  ObjectsVec file_list;
  if (FilterList(&file_list, bucketname, objectname, pattern_1st_sp_char_pos)) {
    return -1;
  };

  if (file_list.empty()) {
    GetLogger()->error("No match for the file pattern");
    return -1;
  };

  const size_t file_count = file_list.size();
  Aws::Vector<Aws::String> filenames(file_count);
  Aws::Vector<long long> cumulative_size(file_count);
  Aws::Vector<Aws::String> etags(file_count);

  // get metadata from the first file
  const auto &first_file = file_list.front();
  filenames.front() = first_file.GetKey();
  cumulative_size.front() = first_file.GetSize();
  etags.front() = first_file.GetETag();

  // sample and check headers
  long long common_header_length = 0;
  bool same_header = true;

  if (file_count > 1) {
    // read header of the first file
    Aws::String header;
    if (ReadHeader(&header, bucketname, first_file, KHIOPS_MAX_HEADERLENGTH)) {
      return -1;
    };
    tOffset header_length = static_cast<tOffset>(header.size());

    // Start building the rest of the lists
    for (size_t i = 1; i < file_count; i++) {
      const auto &curr_file = file_list[i];
      filenames[i] = curr_file.GetKey();
      cumulative_size[i] = cumulative_size[i - 1] + curr_file.GetSize();
      etags[i] = curr_file.GetETag();

      if (same_header) {
        // Read header only for sampled files
        if (SelectObjectsSubset(std::vector<std::string>{filenames[i]})
                .count(filenames[i])) {
          // Read header for this sampled file
          Aws::String curr_header;
          if (ReadHeader(&curr_header, bucketname, curr_file, header_length)) {
            return -1;
          };
          same_header = (curr_header == header);
        } else {
          // Not sampled: compare by size as a proxy
          same_header =
              (header_length <= static_cast<tOffset>(curr_file.GetSize()));
        }
      }
    }

    // if headers remained the same, adjust the cumulative sizes
    if (same_header) {
      common_header_length = header_length;
      for (size_t i = 1; i < file_count; i++) {
        cumulative_size[i] -= (i * common_header_length);
      }
    }
  }

  // construct the result
  *result = Aws::MakeUnique<Reader>(
      KHIOPS_S3, std::move(bucketname), std::move(objectname), 0,
      common_header_length, std::move(filenames), std::move(cumulative_size),
      std::move(etags));
  return 0;
}

int MakeWriterPtr(WriterPtr *result, Aws::String bucket, Aws::String object) {
  Aws::S3::Model::CreateMultipartUploadRequest request;
  request.SetBucket(std::move(bucket));
  request.SetKey(std::move(object));
  auto outcome = client->CreateMultipartUpload(request);
  if (!outcome.IsSuccess()) {
    return -1;
  };
  *result =
      Aws::MakeUnique<Writer>(KHIOPS_S3, outcome.GetResultWithOwnership());
  return 0;
}

Reader *PushBackReaderHandle(ReaderPtr &&stream_ptr) {
  active_reader_handles.push_back(std::move(stream_ptr));
  return active_reader_handles.back().get();
}

Writer *PushBackWriterHandle(WriterPtr &&stream_ptr) {
  active_writer_handles.push_back(std::move(stream_ptr));
  return active_writer_handles.back().get();
}

int RegisterReaderStream(Reader **result, Aws::String &&bucket,
                         Aws::String &&object) {
  Aws::UniquePtr<Reader> reader;
  if (MakeReaderPtr(&reader, std::move(bucket), std::move(object))) {
    return -1;
  }
  *result = PushBackReaderHandle(std::move(reader));
  return 0;
}

int RegisterWriterStream(Writer **result, Aws::String &&bucket,
                         Aws::String &&object) {
  Aws::UniquePtr<Writer> writer;
  if (MakeWriterPtr(&writer, std::move(bucket), std::move(object))) {
    return -1;
  }
  *result = PushBackWriterHandle(std::move(writer));
  return 0;
}

template <typename Result>
void UpdateUploadMetadata(Writer &writer, const Result &result) {
  Aws::S3::Model::CompletedPart part;
  part.SetETag(result.GetETag());
  part.SetPartNumber(writer.part_tracker_);
  writer.parts_.push_back(std::move(part));
  writer.part_tracker_++;
}

int UploadPart(Writer &writer) {
  auto &buffer = writer.buffer_;
  Aws::Utils::Stream::PreallocatedStreamBuf pre_buf(buffer.data(),
                                                    buffer.size());
  const auto request = MakeUploadPartRequest(writer, pre_buf);
  auto outcome = client->UploadPart(request);
  if (!((outcome)).IsSuccess()) {
    return -1;
  };

  UpdateUploadMetadata(writer, outcome.GetResult());

  return 0;
}

int UploadPartCopy(Writer &writer, const Aws::String &byte_range) {
  auto outcome =
      client->UploadPartCopy(MakeUploadPartCopyRequest(writer, byte_range));
  if (!((outcome)).IsSuccess()) {
    return -1;
  };
  UpdateUploadMetadata(writer, outcome.GetResult().GetCopyPartResult());
  return 0;
}

int InitiateAppend(Writer &writer, size_t source_bytes_to_copy) {
  // Make the requests to copy the source file.
  // If the source file is smaller than 5MB, the source needs to be
  // stored in an internal buffer and wait until more data arrives.
  //
  // Conversely, if the source file exceeds 5GB, the copy will be done
  // by parts. If the last part is smaller than 5MB, the last data range
  // will be copied into the internal buffer and wait there.

  const auto &multipartupload_data = writer.writer_;
  int64_t start_range = 0;
  while (source_bytes_to_copy > Writer::buff_min_) {
    const int64_t copy_count = static_cast<int64_t>(
        source_bytes_to_copy > Writer::buff_max_ ? Writer::buff_max_
                                                 : source_bytes_to_copy);

    // peculiarity of AWS: the range for the copy request has an inclusive end,
    // meaning that the bytes numbered start_range to end_range included are
    // copied
    const int64_t end_range = start_range + copy_count - 1;
    if (UploadPartCopy(writer, MakeByteRange(start_range, end_range))) {
      return -1;
    };

    source_bytes_to_copy -= static_cast<size_t>(copy_count);
    start_range += copy_count;
  }

  // copy in the internal buffer what remains from the source.
  if (source_bytes_to_copy > 0) {
    writer.buffer_.reserve(source_bytes_to_copy);
    auto head_outcome = HeadObject(multipartupload_data.GetBucket(),
                                   multipartupload_data.GetKey());
    if (!((head_outcome)).IsSuccess()) {
      GetLogger()->error(head_outcome.GetError().GetMessage());
      return -1;
    };

    Aws::String etag = head_outcome.GetResult().GetETag();
    // reminder: byte ranges are inclusive
    tOffset actual_read;
    if (DownloadFileRangeToVector(
            &actual_read, multipartupload_data.GetBucket(),
            multipartupload_data.GetKey(), writer.buffer_, start_range,
            start_range + static_cast<int64_t>(source_bytes_to_copy) - 1,
            etag)) {
      return -1;
    };

    GetLogger()->debug("copied = {}", actual_read);
  }

  return 0;
}

void *driver_fopen(const char *filename, char mode) {
  if (kFalse == bIsConnected) {
    GetLogger()->error(ERR_NOT_CONNECTED);
    return (nullptr);
  };

  if (!(filename)) {
    GetLogger()->error(ERR_NULL_ARG, __func__);
    return (nullptr);
  };

  GetLogger()->debug("fopen {} {}", filename, mode);

  ParseUriResult names;
  if (ParseS3Uri(&names, (filename))) {
    GetLogger()->error(ERR_URL_PARSING);
    return (((nullptr)));
  }

  switch (mode) {
  case 'r':
    Reader *readerPtr;
    if (RegisterReaderStream(&readerPtr, std::move(names.bucket_),
                             std::move(names.object_))) {
      GetLogger()->error("Error while opening reader stream");
      return (nullptr);
    }
    return readerPtr;
  case 'w':
    Writer *writerPtr;
    if (RegisterWriterStream(&writerPtr, std::move(names.bucket_),
                             std::move(names.object_))) {
      GetLogger()->error("Error while opening writer stream");
      return (nullptr);
    }
    return writerPtr;
  case 'a': {
    // identify the concrete target of the append
    Aws::String target;

    size_t pattern_1st_sp_char_pos = 0;
    if (IsMultifile(names.object_, pattern_1st_sp_char_pos)) {
      ObjectsVec file_list;
      if (FilterList(&file_list, names.bucket_, names.object_,
                     pattern_1st_sp_char_pos)) {
        GetLogger()->error("Error while looking for existing file");
        return (nullptr);
      }

      if (!file_list.empty()) {
        target = file_list.back().GetKey();
      } else {
        GetLogger()->debug("No match for the file pattern.");
      }
    } else {
      target = names.object_;
    }

    // if file does not already exist, fallback to simple write mode
    auto head_outcome = HeadObject(names.bucket_, target);
    if (!head_outcome.IsSuccess()) {
      auto &error = head_outcome.GetError();
      if (error.GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY ||
          error.GetErrorType() == Aws::S3::S3Errors::RESOURCE_NOT_FOUND) {
        // source file not found, fallback to simple write mode
        GetLogger()->debug(
            "No source file to append to, falling back to simple write.");
        Writer *writerPtr;
        if (RegisterWriterStream(&writerPtr, std::move(names.bucket_),
                                 std::move(target))) {
          GetLogger()->error("Error while opening writer stream");
          return (nullptr);
        }
        return writerPtr;
      } else {
        // genuine error
        GetLogger()->error("Error while opening append stream");
        return nullptr;
      }
    }

    // file exists, but is immutable. the strategy is to copy the content to a
    // new version of the file, add the new content with writes and complete,
    // deleting the previous version of the file at the end of the process for
    // the opening, gather the origin file metadata and issue the request to
    // copy its parts

    Writer *writerPtr;
    if (RegisterWriterStream(&writerPtr, std::move(names.bucket_),
                             std::move(target))) {
      GetLogger()->error("Error while opening append stream");
      return (nullptr);
    }

    writerPtr->append_target_ = head_outcome.GetResult().GetVersionId();

    // requests for copy

    if (InitiateAppend(
            *writerPtr,
            static_cast<size_t>(head_outcome.GetResult().GetContentLength()))) {
      GetLogger()->error("Error while initiating append stream");
      return (nullptr);
    }

    return writerPtr;
  }

  default:
    GetLogger()->error("Invalid open mode: {}", mode);
    return nullptr;
  }
}

int driver_fclose(void *stream) {
  if (kFalse == bIsConnected) {
    GetLogger()->error(ERR_NOT_CONNECTED);
    return (kFailure);
  };

  if (!(stream)) {
    GetLogger()->error(ERR_NULL_ARG, __func__);
    return (kFailure);
  };

  GetLogger()->debug("fclose {}", (void *)stream);

  auto reader_handle_it = FindHandle((active_reader_handles), (stream));
  if (reader_handle_it != (active_reader_handles).end()) {
    EraseRemove((active_reader_handles), reader_handle_it);
    return kSuccess;
  };

  auto writer_h_it = FindHandle(active_writer_handles, stream);
  if (writer_h_it != active_writer_handles.end()) {
    // end multipart upload
    // first, flush the pending data
    auto &writer = **writer_h_it;
    if (UploadPart(writer)) {
      GetLogger()->error("Error during upload");
      return (kFailure);
    }

    // close upload
    const auto complete_outcome = client->CompleteMultipartUpload(
        MakeCompleteMultipartUploadRequest(writer));

    // the request can fail and allow retries.
    // if the request fails, the parts are still present on server side!
    // to be able to delete the parts, the writer handle must remain in
    // the list of active handles.
    if (!((complete_outcome)).IsSuccess()) {
      GetLogger()->error("Error completing upload while closing stream");
      return (kFailure);
    }

    EraseRemove(active_writer_handles, writer_h_it);

    return kSuccess;
  }

  GetLogger()->error("Cannot identify stream");
  return kFailure;
}

int driver_fseek(void *stream, long long int offset, int whence) {
  if (kFalse == bIsConnected) {
    GetLogger()->error(ERR_NOT_CONNECTED);
    return (kFailure);
  };

  constexpr long long max_val = std::numeric_limits<long long>::max();

  if (!(stream)) {
    GetLogger()->error(ERR_NULL_ARG, __func__);
    return (kFailure);
  };

  // confirm stream's presence
  auto stream_it = FindHandle((active_reader_handles), (stream));
  if (stream_it == active_reader_handles.end()) {
    GetLogger()->error("Cannot identify stream");
    return (kFailure);
  }
  auto &h_ptr = *stream_it;
  auto &h = *h_ptr;

  GetLogger()->debug("fseek {} {} {}", stream, offset, whence);

  tOffset computed_offset{0};

  switch (whence) {
  case std::ios::beg:
    computed_offset = offset;
    break;
  case std::ios::cur:
    if (offset > max_val - h.offset_) {
      GetLogger()->error("Signed overflow prevented");
      return kFailure;
    }
    computed_offset = h.offset_ + offset;
    break;
  case std::ios::end:
    if (h.total_size_ > 0) {
      long long minus1 = h.total_size_ - 1;
      if (offset > max_val - minus1) {
        GetLogger()->error("Signed overflow prevented");
        return kFailure;
      }
    }
    if ((offset == std::numeric_limits<long long>::min()) &&
        (h.total_size_ == 0)) {
      GetLogger()->error("Signed overflow prevented");
      return kFailure;
    }

    computed_offset = (h.total_size_ == 0) ? offset : h.total_size_ + offset;
    break;
  default:
    GetLogger()->error("Invalid seek mode " + std::to_string(whence));
    return kFailure;
  }

  if (computed_offset < 0) {
    GetLogger()->error("Invalid seek offset " +
                       std::to_string(computed_offset));
    return kFailure;
  }
  h.offset_ = computed_offset;
  return kSuccess;
}

const char *driver_getlasterror() {
  GetLogger()->debug("getlasterror");
  static std::string last_error;
  last_error = GetLastError();
  return last_error.empty() ? nullptr : last_error.c_str();
}

long long int driver_fread(void *ptr, size_t size, size_t count, void *stream) {
  if (kFalse == bIsConnected) {
    GetLogger()->error(ERR_NOT_CONNECTED);
    return (kFailure);
  };

  if (0 == size || 0 == count) {
    return 0LL;
  }

  if (!(stream)) {
    GetLogger()->error(ERR_NULL_ARG, __func__);
    return (kFailure);
  };
  if (!(ptr)) {
    GetLogger()->error(ERR_NULL_ARG, __func__);
    return (kFailure);
  };

  GetLogger()->debug("fread {} {} {} {}", ptr, size, count, stream);

  // confirm stream's presence
  auto stream_it = FindHandle((active_reader_handles), (stream));
  if (stream_it == active_reader_handles.end()) {
    GetLogger()->error("Cannot identify stream");
    return (kFailure);
  }
  auto &h_ptr = *stream_it;
  auto &h = *h_ptr;

  const tOffset offset = h.offset_;

  // prevent overflow
  if (WillSizeCountProductOverflow(size, count)) {
    GetLogger()->error("product size * count is too large, would overflow");
    return kFailure;
  }

  tOffset to_read{static_cast<tOffset>(size * count)};
  if (offset > std::numeric_limits<long long>::max() - to_read) {
    GetLogger()->error("signed overflow prevented on reading attempt");
    return kFailure;
  }
  // end of overflow prevention

  GetLogger()->debug("offset = {} to_read = {}", offset, to_read);

  long long result;
  if (ReadBytesInFile(&result, h, reinterpret_cast<unsigned char *>(ptr),
                      to_read)) {
    GetLogger()->error("Error while reading from file");
    return (kFailure);
  }

  return static_cast<long long>(result / static_cast<long long>(size));
}

long long int driver_fwrite(const void *ptr, size_t size, size_t count,
                            void *stream) {
  if (kFalse == bIsConnected) {
    GetLogger()->error(ERR_NOT_CONNECTED);
    return (kFailure);
  };

  if (0 == size || 0 == count) {
    return 0LL;
  }

  if (!(stream)) {
    GetLogger()->error(ERR_NULL_ARG, __func__);
    return (kFailure);
  };
  if (!(ptr)) {
    GetLogger()->error(ERR_NULL_ARG, __func__);
    return (kFailure);
  };

  GetLogger()->debug("fwrite {} {} {} {}", ptr, size, count, stream);

  auto stream_it = FindHandle((active_writer_handles), (stream));
  if (stream_it == active_writer_handles.end()) {
    GetLogger()->error("Cannot identify stream");
    return (kFailure);
  }
  auto &h_ptr = *stream_it;

  // prevent integer overflow
  if (WillSizeCountProductOverflow(size, count)) {
    GetLogger()->error(
        "Error on write: product size * count is too large, would overflow");
    return kFailure;
  }

  const size_t to_write = size * count;

  // tune up the capacity of the internal buffer, the final buffer size must be
  // a multiple of the size argument
  auto &buffer = h_ptr->buffer_;
  const size_t curr_size = buffer.size();
  const size_t next_size = curr_size + to_write;
  if (next_size > buffer.capacity()) {
    // if next_size exceeds max capacity, reserve the closest capacity under
    // buff_max_ that is a multiple of size argument, else reserve next_size
    buffer.reserve(next_size > WriteFile::buff_max_
                       ? (WriteFile::buff_max_ / size) * size
                       : next_size);
  }

  // copy up to capacity or the whole data for now
  size_t remain = to_write;
  const size_t available = buffer.capacity() - buffer.size();
  size_t copy_count = std::min(available, remain);
  const unsigned char *ptr_cast_pos =
      reinterpret_cast<const unsigned char *>(ptr);

  auto copy_and_update = [](Aws::Vector<unsigned char> &dest,
                            const unsigned char **src_start, size_t count,
                            size_t &remain) {
    dest.insert(dest.end(), *src_start, (*src_start) + count);
    (*src_start) += count;
    remain -= count;
  };

  copy_and_update(buffer, &ptr_cast_pos, copy_count, remain);

  // upload the content of the buffer until the size of the remaining data is
  // smaller than the minimum upload size
  while (buffer.size() >= WriteFile::buff_min_) {
    if (UploadPart(*h_ptr)) {
      GetLogger()->error("Error during upload");
      return (kFailure);
    }

    // copy remaining data up to capacity
    buffer.clear();
    copy_count = std::min(remain, buffer.capacity());
    copy_and_update(buffer, &ptr_cast_pos, copy_count, remain);
  }

  // release unused memory
  buffer.shrink_to_fit();

  return static_cast<long long>(count);
}

int driver_fflush(void *) {
  if (kFalse == bIsConnected) {
    GetLogger()->error(ERR_NOT_CONNECTED);
    return (kFailure);
  };

  GetLogger()->debug("Flushing (does nothing...)");
  return kSuccess;
}

int driver_remove(const char *filename) {
  if (kFalse == bIsConnected) {
    GetLogger()->error(ERR_NOT_CONNECTED);
    return (kFalse);
  };

  if (!(filename)) {
    GetLogger()->error(ERR_NULL_ARG, __func__);
    return (kFalse);
  };

  GetLogger()->debug("remove {}", filename);

  std::string filename_as_string(filename);
  std::string prefix, suffix;
  if (filename_as_string.find('*') != std::string::npos) {
    if (!parse_globbing_pattern(std::string(filename_as_string), &prefix, &suffix)) {
      GetLogger()->error("Invalid globbing pattern");
      return kOtherFailure;
    }
    ParseUriResult names;
    if (ParseS3Uri(&names, (prefix.c_str()))) {
      GetLogger()->error(ERR_URL_PARSING);
      return (((kOtherFailure)));
    }
    size_t first_globchar_pos;
    IsMultifile(names.object_ + "*" + suffix, first_globchar_pos);
    ObjectsVec file_list;
    if (FilterList(&file_list, names.bucket_, names.object_ + "*" + suffix, first_globchar_pos)) {
      GetLogger()->error("Failed to list glob-matching objects.");
      return kOtherFailure;
    };
    Aws::S3::Model::DeleteObjectRequest request;
    request.WithBucket(names.bucket_);
    bool is_success = true;
    for (const S3Object &object_to_delete : file_list) {
      request.WithKey(object_to_delete.GetKey());
      Aws::S3::Model::DeleteObjectOutcome outcome = client->DeleteObject(request);
      if (!outcome.IsSuccess()) {
        is_success = false;
        auto err = outcome.GetError();
        GetLogger()->error("DeleteObject: {} {}", err.GetExceptionName(),
                          err.GetMessage());
      }
    }
    return is_success;
  } else {
    ParseUriResult names;
    if (ParseS3Uri(&names, (filename))) {
      GetLogger()->error(ERR_URL_PARSING);
      return (((kOtherFailure)));
    }

    Aws::S3::Model::DeleteObjectRequest request;

    request.WithBucket(names.bucket_).WithKey(names.object_);

    Aws::S3::Model::DeleteObjectOutcome outcome = client->DeleteObject(request);

    if (!outcome.IsSuccess()) {
      auto err = outcome.GetError();
      GetLogger()->error("DeleteObject: {} {}", err.GetExceptionName(),
                        err.GetMessage());
    }

    return outcome.IsSuccess();
  }
}

int driver_rmdir(const char *filename) {
  if (kFalse == bIsConnected) {
    GetLogger()->error(ERR_NOT_CONNECTED);
    return (kOtherFailure);
  };

  if (!(filename)) {
    GetLogger()->error(ERR_NULL_ARG, __func__);
    return (kOtherFailure);
  };
  GetLogger()->debug("rmdir {}", filename);

  ParseUriResult names;
  if (ParseS3Uri(&names, filename)) {
    GetLogger()->error(ERR_URL_PARSING);
    return kOtherFailure;
  }

  return DeleteBlobDirectory(names.bucket_, names.object_) ? kOtherSuccess
                                                           : kOtherFailure;
}

int driver_mkdir(const char *filename) {
  if (kFalse == bIsConnected) {
    GetLogger()->error(ERR_NOT_CONNECTED);
    return (kOtherFailure);
  };

  if (!(filename)) {
    GetLogger()->error(ERR_NULL_ARG, __func__);
    return (kOtherFailure);
  };
  GetLogger()->debug("mkdir {}", filename);

  ParseUriResult names;
  if (ParseS3Uri(&names, filename)) {
    GetLogger()->error(ERR_URL_PARSING);
    return kOtherFailure;
  }

  return CreateBlobDirectory(names.bucket_, names.object_) ? kOtherSuccess
                                                           : kOtherFailure;
}

long long int driver_diskFreeSpace(const char *filename) {
  GetLogger()->debug("diskFreeSpace {}", filename);

  return (long long int)5 * 1024 * 1024 * 1024 * 1024;
}

int driver_copyToLocal(const char *sSourceFilePathName,
                       const char *sDestFilePathName) {
  if (kFalse == bIsConnected) {
    GetLogger()->error(ERR_NOT_CONNECTED);
    return (kOtherFailure);
  };
  if (!(sSourceFilePathName)) {
    GetLogger()->error(ERR_NULL_ARG, __func__);
    return (kOtherFailure);
  };
  if (!(sDestFilePathName)) {
    GetLogger()->error(ERR_NULL_ARG, __func__);
    return (kOtherFailure);
  };

  GetLogger()->debug("copyToLocal {} {}", sSourceFilePathName,
                     sDestFilePathName);

  // try opening the online source file
  ParseUriResult names;
  if (ParseS3Uri(&names, (sSourceFilePathName))) {
    GetLogger()->error(ERR_URL_PARSING);
    return (((kOtherFailure)));
  }
  ReaderPtr readerPtr;
  if (MakeReaderPtr(&readerPtr, names.bucket_, names.object_)) {
    GetLogger()->error("Error while opening remote file");
    return (kOtherFailure);
  }

  // open local file
  std::ofstream file_stream(sDestFilePathName, std::ios::binary);
  if (!file_stream.is_open()) {
    std::ostringstream oss;
    oss << "Failed to open local file for writing: " << sDestFilePathName;
    GetLogger()->error(oss.str());
    return kOtherFailure;
  }

  auto read_and_write = [](const Reader &from, size_t part,
                           std::ofstream &to_file) -> bool {
    // file metadata
    const long long header_size = from.common_header_length_;

    // limit download to a few MBs at a time.
    constexpr long long dl_limit{10 * 1024 * 1024};

    const long long file_size =
        part == 0 ? from.cumulative_sizes_[0]
                  : header_size + from.cumulative_sizes_[part] -
                        from.cumulative_sizes_[part - 1];

    // download range limits
    const long long end_limit = file_size - 1;
    long long start = 0 == part ? 0 : header_size;
    long long end = std::min(start + dl_limit - 1, end_limit);

    const Aws::String etag =
        (part < from.etags_.size()) ? from.etags_[part] : Aws::String{};

    // download by pieces
    while (to_file && start < end_limit) {
      const auto request =
          MakeGetObjectRequest(from.bucketname_, from.filenames_[part], etag,
                               MakeByteRange(start, end));
      auto get_outcome = client->GetObject(request);
      if (!((get_outcome)).IsSuccess()) {
        GetLogger()->error("Error while downloading file content");
        return (false);
      }

      // get ownership of the result and its underlying stream
      const Aws::S3::Model::GetObjectResult result{
          get_outcome.GetResultWithOwnership()};
      to_file << result.GetBody().rdbuf();

      start += result.GetContentLength(); // a bit of security for now: could
                                          // the downloading be incomplete?
      end = std::min(start + dl_limit - 1, end_limit);
    }
    // what made the process stop?
    if (!to_file) {
      // something went wrong on write side, abort
      GetLogger()->error("Error while writing data to local file");
      return false;
    }

    return true;
  };

  const Reader &reader = *readerPtr;
  const size_t parts_count{reader.filenames_.size()};

  bool op_res = true;
  for (size_t part = 0; part < parts_count && op_res; part++) {
    op_res = read_and_write(reader, part, file_stream);
  }

  file_stream.close();

  if (!op_res || !file_stream) {
    GetLogger()->error("Error copying remote file to local storage.");
    GetLogger()->debug("Attempting to remove local file.");
    if (0 != std::remove(sDestFilePathName)) {
      GetLogger()->error("Error attempting to remove local file.");
    }
    GetLogger()->debug("Successful file removal.");

    return kOtherFailure;
  }

  GetLogger()->debug("Successful local copy of remote file.");

  return kOtherSuccess;
}

int driver_copyFromLocal(const char *sSourceFilePathName,
                         const char *sDestFilePathName) {
  if (kFalse == bIsConnected) {
    GetLogger()->error(ERR_NOT_CONNECTED);
    return (kFailure);
  };

  if (!(sSourceFilePathName)) {
    GetLogger()->error(ERR_NULL_ARG, __func__);
    return (kOtherFailure);
  };
  if (!(sDestFilePathName)) {
    GetLogger()->error(ERR_NULL_ARG, __func__);
    return (kOtherFailure);
  };

  GetLogger()->debug("copyFromLocal {} {}", sSourceFilePathName,
                     sDestFilePathName);

  ParseUriResult names;
  if (ParseS3Uri(&names, (sDestFilePathName))) {
    GetLogger()->error(ERR_URL_PARSING);
    return (((kOtherFailure)));
  }

  // Configuration de la requête pour envoyer l'objet
  Aws::S3::Model::PutObjectRequest object_request;
  object_request.WithBucket(names.bucket_).WithKey(names.object_);

  // Chargement du fichier dans un flux d'entrée
  std::shared_ptr<Aws::IOStream> input_data =
      Aws::MakeShared<Aws::FStream>("PutObjectInputStream", sSourceFilePathName,
                                    std::ios_base::in | std::ios_base::binary);

  object_request.SetBody(input_data);

  // Exécution de la requête
  auto put_object_outcome = client->PutObject(object_request);

  if (!put_object_outcome.IsSuccess()) {
    GetLogger()->error("Error during file upload: {}",
                       put_object_outcome.GetError().GetMessage());
    return kOtherFailure;
  }

  return kOtherSuccess;
}

/**
 * @brief Concatenate S3 objects into a destination object using multipart
 * upload.
 *
 * This implementation optimizes for server-side operations:
 *  - UploadPartCopy is used whenever possible (ranges >= 5 MiB, <= 5 GiB).
 *  - Small sources (< 5 MiB) and small tails are aggregated locally into a
 * buffer and uploaded as a part once the buffer reaches >= 5 MiB.
 *  - The final part may be < 5 MiB.
 *
 * Explicit handling of the 10,000-part limit:
 *  - We estimate an upper bound on parts for a set of sources and split the
 * work into multi-stage server-side concatenations if needed.
 *  - Each stage produces an intermediate object (except the final stage).
 *  - Intermediate objects are concatenated again until the final object can be
 * built within the 10,000-part limit.
 *
 * On success, all source objects are deleted (destination is never deleted).
 */
int driver_concat(const char *destfilename, const char **sourcefilenames,
                  size_t sourcefilecount) {
  if (kFalse == bIsConnected) {
    GetLogger()->error(ERR_NOT_CONNECTED);
    return (kOtherFailure);
  };
  if (!(destfilename)) {
    GetLogger()->error(ERR_NULL_ARG, __func__);
    return (kOtherFailure);
  };
  if (!(sourcefilenames)) {
    GetLogger()->error(ERR_NULL_ARG, __func__);
    return (kOtherFailure);
  };

  if (sourcefilecount == 0) {
    GetLogger()->error("driver_concat: no source files");
    return kOtherFailure;
  }
  ParseUriResult names;
  if (ParseS3Uri(&names, (destfilename))) {
    GetLogger()->error(ERR_URL_PARSING);
    return (((kOtherFailure)));
  }

  size_t sp = 0;
  if (IsMultifile(names.object_, sp)) {
    GetLogger()->error("driver_concat: destination must be a single object");
    return kOtherFailure;
  }

  constexpr long long MIN_PART =
      static_cast<long long>(Writer::buff_min_); // 5 MiB
  constexpr long long MAX_PART =
      static_cast<long long>(Writer::buff_max_); // 5 GiB
  constexpr long long MAX_PARTS = 10000;

  struct Src {
    Aws::String bucket;
    Aws::String key;
    long long size;
    Aws::String etag;
  };

  Aws::Vector<Src> srcs;
  srcs.reserve(sourcefilecount);

  long long total_size = 0;

  GetLogger()->info("driver_concat: dest={}, source count={}", destfilename,
                    sourcefilecount);

  // Track unique source keys to delete on success
  std::set<Aws::String> sources_to_delete;

  for (size_t i = 0; i < sourcefilecount; ++i) {
    if (!(sourcefilenames[i])) {
      GetLogger()->error(ERR_NULL_ARG, __func__);
      return (kOtherFailure);
    };
    ParseUriResult s;
    if (ParseS3Uri(&s, sourcefilenames[i])) {
      GetLogger()->error("Error parsing source URI");
      return (kOtherFailure);
    }

    if (s.bucket_ != names.bucket_) {
      GetLogger()->error(
          "driver_concat: sources must be in same bucket as destination");
      return kOtherFailure;
    }

    auto head_outcome = HeadObject(s.bucket_, s.object_);
    if (!((head_outcome)).IsSuccess()) {
      GetLogger()->error("Error getting source metadata");
      return (kOtherFailure);
    }

    const auto &head = head_outcome.GetResult();
    long long size = head.GetContentLength();
    Aws::String etag = head.GetETag();

    if (size > 0 && total_size > std::numeric_limits<long long>::max() - size) {
      GetLogger()->error("driver_concat: total size overflow");
      return kOtherFailure;
    }
    total_size += size;

    GetLogger()->debug("driver_concat: source {} -> size={}", s.object_, size);

    srcs.push_back({s.bucket_, s.object_, size, etag});

    if (!(s.bucket_ == names.bucket_ && s.object_ == names.object_)) {
      sources_to_delete.insert(s.object_);
    } else {
      GetLogger()->warn(
          "driver_concat: source equals destination ({}), will not delete it",
          s.object_);
    }
  }

  // If everything is empty, create an empty destination object.
  if (total_size == 0) {
    GetLogger()->info(
        "driver_concat: all sources empty, creating empty destination object");
    Aws::S3::Model::PutObjectRequest req;
    req.WithBucket(names.bucket_).WithKey(names.object_);
    auto empty_body = Aws::MakeShared<Aws::StringStream>(KHIOPS_S3);
    req.SetBody(empty_body);

    auto put_outcome = client->PutObject(req);
    if (!put_outcome.IsSuccess()) {
      GetLogger()->error("Error creating empty object");
      return kOtherFailure;
    }

    // Delete sources even if empty
    bool delete_ok = true;
    for (const auto &k : sources_to_delete) {
      Aws::S3::Model::DeleteObjectRequest del;
      del.WithBucket(names.bucket_).WithKey(k);
      auto del_outcome = client->DeleteObject(del);
      if (!del_outcome.IsSuccess()) {
        delete_ok = false;
        GetLogger()->error("driver_concat: failed to delete source {}: {}", k,
                           del_outcome.GetError().GetMessage());
      } else {
        GetLogger()->debug("driver_concat: deleted source {}", k);
      }
    }

    return delete_ok ? kOtherSuccess : kOtherFailure;
  }

  auto estimate_parts = [&](const Src &s) -> long long {
    if (s.size == 0)
      return 0;
    if (s.size <= MAX_PART)
      return 1;
    return (s.size + MAX_PART - 1) / MAX_PART;
  };

  auto make_temp_key = [&](int level, int group) -> Aws::String {
    Aws::StringStream ss;
    ss << names.object_ << ".concat_tmp_L" << level << "_G" << group << "_"
       << std::rand();
    return ss.str().c_str();
  };

  auto cleanup_temps = [&](const Aws::Vector<Aws::String> &keys) {
    for (const auto &k : keys) {
      Aws::S3::Model::DeleteObjectRequest req;
      req.WithBucket(names.bucket_).WithKey(k);
      auto del_outcome = client->DeleteObject(req);
      if (!del_outcome.IsSuccess()) {
        GetLogger()->warn("driver_concat: failed to delete temp object {}: {}",
                          k, del_outcome.GetError().GetMessage());
      } else {
        GetLogger()->debug("driver_concat: deleted temp object {}", k);
      }
    }
  };

  auto concat_stage = [&](const Aws::Vector<Src> &inputs,
                          const Aws::String &dest_key, int level,
                          int group_idx) -> bool {
    GetLogger()->info("concat_stage: level={}, group={}, dest={}, sources={}",
                      level, group_idx, dest_key, inputs.size());

    WriterPtr writerPtr;
    if (MakeWriterPtr(&writerPtr, names.bucket_, dest_key)) {
      GetLogger()->error("Error creating multipart upload");
      return false;
    }
    auto writer = writerPtr.get();

    auto abort_upload = [&]() {
      client->AbortMultipartUpload(
          MakeBaseUploadRequest<Aws::S3::Model::AbortMultipartUploadRequest>(
              *writer));
    };

    size_t part_count = 0;
    long long bytes_written = 0;

    Aws::Vector<bool> has_data_after(inputs.size(), false);
    bool seen = false;
    for (size_t i = inputs.size(); i-- > 0;) {
      has_data_after[i] = seen;
      if (inputs[i].size > 0)
        seen = true;
    }

    auto append_range_to_buffer = [&](const Src &src, long long start,
                                      long long len) -> bool {
      if (len <= 0)
        return true;

      GetLogger()->debug(
          "concat_stage: download range {}:{} (len={}) into buffer", src.key,
          start, len);

      Aws::Vector<unsigned char> tmp;
      tmp.reserve(static_cast<size_t>(len));

      long long dlsize;
      if (DownloadFileRangeToVector(
              &dlsize, src.bucket, src.key, tmp, static_cast<int64_t>(start),
              static_cast<int64_t>(start + len - 1), src.etag)) {

        GetLogger()->error("concat_stage: download failed");
        return false;
      }

      if (dlsize != len) {
        GetLogger()->error(
            "concat_stage: short read while downloading source range");
        return false;
      }

      writer->buffer_.reserve(writer->buffer_.size() + tmp.size());
      writer->buffer_.insert(writer->buffer_.end(), tmp.begin(), tmp.end());

      GetLogger()->debug("concat_stage: buffer size now {}",
                         writer->buffer_.size());
      return true;
    };

    auto flush_buffer = [&](bool is_last_part) -> bool {
      if (writer->buffer_.empty())
        return true;

      const size_t sz = writer->buffer_.size();
      if (!is_last_part && sz < static_cast<size_t>(MIN_PART)) {
        GetLogger()->error("concat_stage: internal error, part < 5MiB");
        return false;
      }
      if (sz > static_cast<size_t>(MAX_PART)) {
        GetLogger()->error("concat_stage: internal error, part > 5GiB");
        return false;
      }
      if (part_count >= static_cast<size_t>(MAX_PARTS)) {
        GetLogger()->error("concat_stage: exceeded 10,000 parts");
        return false;
      }

      GetLogger()->debug("concat_stage: UploadPart (part #{}) size={}",
                         writer->part_tracker_, sz);

      if (UploadPart(*writer)) {
        GetLogger()->error("Error during UploadPart");
        return false;
      }

      part_count++;
      bytes_written += static_cast<long long>(sz);
      writer->buffer_.clear();

      return true;
    };

    auto upload_copy_range = [&](const Src &src, long long start,
                                 long long len) -> bool {
      if (len <= 0)
        return true;
      if (part_count >= static_cast<size_t>(MAX_PARTS)) {
        GetLogger()->error("concat_stage: exceeded 10,000 parts");
        return false;
      }

      writer->append_target_ = src.bucket + "/" + src.key;

      GetLogger()->debug(
          "concat_stage: UploadPartCopy (part #{}) {} [{}..{}] len={}",
          writer->part_tracker_, src.key, start, start + len - 1, len);

      if (UploadPartCopy(*writer, MakeByteRange(start, start + len - 1))) {
        GetLogger()->error("Error during UploadPartCopy");
        return false;
      }

      part_count++;
      bytes_written += len;
      return true;
    };

    writer->buffer_.clear();

    for (size_t i = 0; i < inputs.size(); ++i) {
      const auto &src = inputs[i];
      if (src.size == 0)
        continue;

      GetLogger()->debug("concat_stage: processing source {} size={}", src.key,
                         src.size);

      long long offset = 0;
      long long rem = src.size;

      // If a partial buffer exists, fill it minimally and flush.
      if (!writer->buffer_.empty()) {
        if (writer->buffer_.size() >= static_cast<size_t>(MIN_PART)) {
          if (!flush_buffer(false)) {
            abort_upload();
            return false;
          }
        }

        if (writer->buffer_.size() < static_cast<size_t>(MIN_PART)) {
          const long long need =
              MIN_PART - static_cast<long long>(writer->buffer_.size());
          const long long to_take = std::min<long long>(need, rem);

          if (to_take > 0) {
            if (!append_range_to_buffer(src, offset, to_take)) {
              abort_upload();
              return false;
            }
            offset += to_take;
            rem -= to_take;
          }

          if (writer->buffer_.size() >= static_cast<size_t>(MIN_PART)) {
            if (!flush_buffer(false)) {
              abort_upload();
              return false;
            }
          }

          if (rem == 0) {
            continue;
          }
        }
      }

      // Server-side copy the remaining bytes, except small tail (<5 MiB) when
      // more data follows.
      while (rem > 0) {
        if (rem > MAX_PART) {
          if (!upload_copy_range(src, offset, MAX_PART)) {
            abort_upload();
            return false;
          }
          offset += MAX_PART;
          rem -= MAX_PART;
          continue;
        }

        if (rem < MIN_PART && has_data_after[i]) {
          GetLogger()->debug(
              "concat_stage: tail <5MiB buffered (source {}, len={})", src.key,
              rem);
          if (!append_range_to_buffer(src, offset, rem)) {
            abort_upload();
            return false;
          }
          offset += rem;
          rem = 0;
          break;
        } else {
          if (!upload_copy_range(src, offset, rem)) {
            abort_upload();
            return false;
          }
          offset += rem;
          rem = 0;
          break;
        }
      }
    }

    // Flush remaining buffer as last part (may be < 5 MiB).
    if (!writer->buffer_.empty()) {
      if (!flush_buffer(true)) {
        abort_upload();
        return false;
      }
    }

    if (part_count == 0) {
      GetLogger()->error("concat_stage: no parts uploaded (internal error)");
      abort_upload();
      return false;
    }

    auto complete = client->CompleteMultipartUpload(
        MakeCompleteMultipartUploadRequest(*writer));
    if (!complete.IsSuccess()) {
      GetLogger()->error("Error completing concat stage");
      abort_upload();
      return false;
    }

    GetLogger()->info("concat_stage: completed dest={} parts={} bytes={}",
                      dest_key, part_count, bytes_written);

    return true;
  };

  Aws::Vector<Aws::String> temp_keys;
  Aws::Vector<Src> current = srcs;

  int level = 0;

  while (true) {
    long long total_est = 0;
    for (const auto &s : current) {
      long long est = estimate_parts(s);
      if (est > MAX_PARTS) {
        GetLogger()->error(
            "driver_concat: single source exceeds 10,000-part limit");
        cleanup_temps(temp_keys);
        return kOtherFailure;
      }
      total_est += est;
    }

    GetLogger()->info("driver_concat: level {} sources={} estimated_parts={}",
                      level, current.size(), total_est);

    if (total_est <= MAX_PARTS) {
      // Final stage to destination
      if (!concat_stage(current, names.object_, level, 0)) {
        cleanup_temps(temp_keys);
        return kOtherFailure;
      }
      break;
    }

    // Build groups for this level
    Aws::Vector<Src> next;
    size_t idx = 0;
    int group = 0;

    while (idx < current.size()) {
      long long group_est = 0;
      Aws::Vector<Src> group_sources;

      while (idx < current.size()) {
        long long est = estimate_parts(current[idx]);
        if (!group_sources.empty() && group_est + est > MAX_PARTS) {
          break;
        }
        group_sources.push_back(current[idx]);
        group_est += est;
        idx++;
      }

      if (group_sources.empty()) {
        GetLogger()->error("driver_concat: grouping failed due to part limit");
        cleanup_temps(temp_keys);
        return kOtherFailure;
      }

      Aws::String tmp_key = make_temp_key(level, group);

      GetLogger()->info("driver_concat: level {} group {} -> temp {} "
                        "(sources={}, est_parts={})",
                        level, group, tmp_key, group_sources.size(), group_est);

      if (!concat_stage(group_sources, tmp_key, level, group)) {
        cleanup_temps(temp_keys);
        return kOtherFailure;
      }

      auto head_outcome = HeadObject(names.bucket_, tmp_key);
      if (!head_outcome.IsSuccess()) {
        GetLogger()->error("Error getting temp object metadata");
        cleanup_temps(temp_keys);
        return kOtherFailure;
      }

      const auto &head = head_outcome.GetResult();
      next.push_back(
          {names.bucket_, tmp_key, head.GetContentLength(), head.GetETag()});
      temp_keys.push_back(tmp_key);

      group++;
    }

    current = std::move(next);
    level++;
  }

  // Cleanup intermediate objects
  cleanup_temps(temp_keys);

  // Delete all source objects (except destination)
  bool delete_ok = true;
  for (const auto &k : sources_to_delete) {
    Aws::S3::Model::DeleteObjectRequest del;
    del.WithBucket(names.bucket_).WithKey(k);

    auto del_outcome = client->DeleteObject(del);
    if (!del_outcome.IsSuccess()) {
      delete_ok = false;
      GetLogger()->error("driver_concat: failed to delete source {}: {}", k,
                         del_outcome.GetError().GetMessage());
    } else {
      GetLogger()->debug("driver_concat: deleted source {}", k);
    }
  }

  return delete_ok ? kOtherSuccess : kOtherFailure;
}

int driver_composeMultifile(const char *sDestFilePathName,
                            const char **sSourceFilePathNames,
                            size_t nSourceFileCount) {
  if (kFalse == bIsConnected) {
    GetLogger()->error(ERR_NOT_CONNECTED);
    return kOtherFailure;
  }

  if (!sDestFilePathName || !sSourceFilePathNames) {
    GetLogger()->error(
        "Error passing null pointers as arguments to driver_composeMultifile");
    return kOtherFailure;
  }

  if (nSourceFileCount < 1) {
    GetLogger()->error(
        "Error passing invalid number of files to driver_composeMultifile");
    return kOtherFailure;
  }

  GetLogger()->debug("driver_composeMultifile {} with {} sources:",
                     sDestFilePathName, nSourceFileCount);

  // ---- Local helpers (autonomous implementation) --------------------------

  // Validate "relative path": no URI scheme, no leading slash
  auto is_relative_path = [](const char *p) -> bool {
    if (p == NULL) {
      return false;
    }
    const std::string s(p);
    if (s.empty()) {
      return false;
    }
    if (s.find("://") != std::string::npos) {
      return false;
    }
    if (!s.empty() && s[0] == '/') {
      return false;
    }
    return true;
  };

  // 12-digit zero-padded sequence number
  auto generate_sequence_number = [](size_t i) -> std::string {
    std::ostringstream os;
    os << std::setfill('0') << std::setw(12) << i;
    return os.str();
  };

  // -------------------------------------------------------------------------

  // Parse and validate destination pattern
  std::string prefix;
  std::string suffix;
  if (!parse_globbing_pattern(std::string(sDestFilePathName), &prefix, &suffix)) {
    GetLogger()->error("Invalid globbing pattern");
    return kOtherFailure;
  }

  // Destination prefix must be a valid S3 URI
  ParseUriResult parsed_dest;
  if (ParseS3Uri(&parsed_dest, prefix.c_str())) {
    GetLogger()->error("Error parsing destination pattern");
    return kOtherFailure;
  }

  const Aws::String &dest_bucket = parsed_dest.bucket_;
  const Aws::String &base_object = parsed_dest.object_;

  // Validate source paths and log them
  for (size_t i = 0; i < nSourceFileCount; ++i) {
    if (!is_relative_path(sSourceFilePathNames[i])) {
      std::ostringstream os;
      os << "Source file path must be relative (no s3:// allowed): "
         << (sSourceFilePathNames[i] ? sSourceFilePathNames[i] : "<null>");
      GetLogger()->error(os.str());
      return kOtherFailure;
    }
    GetLogger()->debug("- {}", sSourceFilePathNames[i]);
  }

  bool failure_detected = false;

  // Copy + delete for each source
  for (size_t i = 0; i < nSourceFileCount; ++i) {
    const Aws::String source_object = sSourceFilePathNames[i];

    const std::string seq = generate_sequence_number(i);

    std::ostringstream name_os;
    name_os << base_object.c_str() << seq << suffix;
    const Aws::String new_object_name = name_os.str().c_str();

    GetLogger()->debug("Renaming {} to {}", source_object.c_str(),
                       new_object_name.c_str());

    // S3 CopyObject source format: "bucket/key"
    Aws::String copy_source = dest_bucket;
    copy_source += "/";
    copy_source += source_object;

    Aws::S3::Model::CopyObjectRequest copy_req;
    copy_req.SetBucket(dest_bucket);
    copy_req.SetKey(new_object_name);
    copy_req.SetCopySource(copy_source);

    Aws::S3::Model::CopyObjectOutcome copy_outcome = client->CopyObject(copy_req);
    if (!copy_outcome.IsSuccess()) {
      GetLogger()->error("Error renaming '{}' to '{}': {}",
                         source_object.c_str(), new_object_name.c_str(),
                         copy_outcome.GetError().GetMessage());
      failure_detected = true;
      continue;
    }

    Aws::S3::Model::DeleteObjectRequest del_req;
    del_req.SetBucket(dest_bucket);
    del_req.SetKey(source_object);

    Aws::S3::Model::DeleteObjectOutcome del_outcome = client->DeleteObject(del_req);
    if (!del_outcome.IsSuccess()) {
      GetLogger()->error("Error deleting original file '{}': {}",
                         source_object.c_str(),
                         del_outcome.GetError().GetMessage());
      failure_detected = true;
    }
  }

  return failure_detected ? kOtherFailure : kOtherSuccess;
}

bool test_compareFiles(const char *local_file_path_str,
                       const char *s3_uri_str) {
  std::string local_file_path(local_file_path_str);
  std::string s3_uri(s3_uri_str);

  // Lire le fichier local
  std::ifstream local_file(local_file_path, std::ios::binary);
  if (!local_file) {
    std::cerr << "Failure reading local file" << std::endl;
    return false;
  }
  std::string local_content((std::istreambuf_iterator<char>(local_file)),
                            std::istreambuf_iterator<char>());

  // Télécharger l'objet S3
  char const *prefix = "s3://";
  const size_t prefix_size{std::strlen(prefix)};
  const size_t pos = s3_uri.find('/', prefix_size);
  std::string bucket_name = s3_uri.substr(prefix_size, pos - prefix_size);
  std::string object_name = s3_uri.substr(pos + 1);

  // Télécharger l'objet S3
  Aws::S3::Model::GetObjectRequest object_request;
  object_request.SetBucket(bucket_name.c_str());
  object_request.SetKey(object_name.c_str());
  auto get_object_outcome = client->GetObject(object_request);
  if (!get_object_outcome.IsSuccess()) {
    std::cerr << "Failure retrieving object from S3" << std::endl;
    return false;
  }

  // Lire le contenu de l'objet S3
  std::stringstream s3_content;
  s3_content << get_object_outcome.GetResult().GetBody().rdbuf();

  // Comparer les contenus
  auto result =
      local_content == s3_content.str() ? kOtherSuccess : kOtherFailure;

  return static_cast<bool>(result);
}
