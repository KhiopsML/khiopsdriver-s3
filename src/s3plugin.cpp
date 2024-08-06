#ifdef __CYGWIN__
#define _CRT_SECURE_NO_WARNINGS
#endif

#include "s3plugin.h"
#include "spdlog/spdlog.h"
#include <assert.h>
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>

#include "ini.h"
// #include <filesystem>

constexpr int kSuccess{1};
constexpr int kFailure{0};

constexpr int kFalse{0};
constexpr int kTrue{1};

constexpr long long kBadSize{-1};

int bIsConnected = false;

constexpr const char *S3EndpointProvider = "KHIOPS_ML_S3";

Aws::SDKOptions options;
// Aws::S3::S3Client *client = NULL;
std::unique_ptr<Aws::S3::S3Client> client;

// Global bucket name
std::string globalBucketName = "";

typedef long long int tOffset;

struct MultiPartFile {
  std::string bucketname;
  std::string filename;
  tOffset offset;
  // Added for multifile support
  tOffset commonHeaderLength;
  std::vector<std::string> filenames;
  std::vector<long long int> cumulativeSize;
};

std::string last_error;

constexpr const char *nullptr_msg_stub = "Error passing null pointer to ";

#define ERROR_ON_NULL_ARG(arg, err_val)                                        \
  if (!(arg)) {                                                                \
    std::ostringstream os;                                                     \
    os << nullptr_msg_stub << __func__;                                        \
    LogError(os.str());                                                        \
    return (err_val);                                                          \
  }

#define RETURN_ON_ERROR(outcome, msg, err_val)                                 \
  {                                                                            \
    if (!(outcome).IsSuccess()) {                                              \
      LogBadOutcome((outcome), (msg));                                         \
      return (err_val);                                                        \
    }                                                                          \
  }

#define ERROR_ON_NAMES(status_or_names, err_val)                               \
  RETURN_ON_ERROR((status_or_names), "Error parsing URL", (err_val))

#define NAMES_OR_ERROR(arg, err_val)                                           \
  auto maybe_parsed_names = ParseS3Uri((arg));                                 \
  ERROR_ON_NAMES(maybe_parsed_names, (err_val));                               \
  auto &names = maybe_parsed_names.GetResult();

void LogError(const std::string &msg) {
  spdlog::error(msg);
  last_error = std::move(msg);
}

template <typename R, typename E>
void LogBadOutcome(const Aws::Utils::Outcome<R, E> &outcome,
                   const std::string &msg) {
  outcome.GetError().GetMessage() std::ostringstream os;
  os << msg << ": " << outcome.GetError().GetMessage();
  LogError(os.str());
}

template <typename RequestType>
RequestType MakeBaseRequest(const std::string &bucket,
                            const std::string &object,
                            std::string &&range = "") {
  RequestType request;
  request.WithBucket(bucket).WithKey(object);
  return request
}

Aws::S3::Model::HeadObjectRequest
MakeHeadObjectRequest(const std::string &bucket, const std::string &object) {
  return MakeBaseRequest<Aws::S3::Model::HeadObjectRequest>(bucket, object);
}

Aws::S3::Model::GetObjectRequest
MakeGetObjectRequest(const std::string &bucket, const std::string &object,
                     std::string &&range = "") {
  auto request =
      MakeBaseRequest<Aws::S3::Model::GetObjectRequest>(bucket, object);
  if (!range.empty()) {
    request.SetRange(std::move(range));
  }
  return request;
}

Aws::S3::Model::GetObjectOutcome GetObject(const std::string &bucket,
                                           const std::string &object,
                                           std::string &&range = "") {
  return client->GetObject(
      MakeGetObjectRequest(bucket, object, std::move(range)));
}

Aws::S3::Model::HeadObjectOutcome HeadObject(const std::string &bucket,
                                             const std::string &object) {
  return client->HeadObject(MakeHeadObjectRequest(bucket, object));
}

// Definition of helper functions
long long int DownloadFileRangeToBuffer(const std::string &bucket_name,
                                        const std::string &object_name,
                                        char *buffer, std::size_t buffer_length,
                                        std::int64_t start_range,
                                        std::int64_t end_range) {

  // Définition de la plage de bytes à télécharger
  Aws::StringStream range;
  range << "bytes=" << start_range << "-" << end_range;

  // Exécution de la requête
  auto get_object_outcome = GetObject(bucket_name, object_name, range.str());
  // // Configuration de la requête pour obtenir un objet
  // auto object_request = MakeHeadObjectRequest(bucket_name, object_name,
  // range.str());

  long long int num_read = -1;

  if (get_object_outcome.IsSuccess()) {
    // Téléchargement réussi, lecture des données dans le buffer
    auto &retrieved_file =
        get_object_outcome.GetResultWithOwnership().GetBody();

    retrieved_file.read(buffer, buffer_length);
    num_read = retrieved_file.gcount();
    spdlog::debug("read = {}", num_read);
  } else {
    // Gestion des erreurs
    std::cerr << "Failed to download object: "
              << get_object_outcome.GetError().GetMessage() << std::endl;

    num_read = -1;
  }
  return num_read;
}

bool UploadBuffer(const std::string &bucket_name,
                  const std::string &object_name, const char *buffer,
                  std::size_t buffer_size) {

  Aws::S3::Model::PutObjectRequest request;
  request.SetBucket(bucket_name);
  request.SetKey(object_name);

  const std::shared_ptr<Aws::IOStream> inputData =
      Aws::MakeShared<Aws::StringStream>("");
  std::string data;
  data.assign(buffer, buffer_size);
  *inputData << data.c_str();

  request.SetBody(inputData);

  Aws::S3::Model::PutObjectOutcome outcome = client->PutObject(request);

  if (!outcome.IsSuccess()) {
    spdlog::error("PutObjectBuffer: {}", outcome.GetError().GetMessage());
  }

  return outcome.IsSuccess();
}

struct SimpleErrorModel {
  std::string err_msg_;

  const std::string &GetMessage() const { return err_msg_; }
};

struct SimpleError {
  int code_;
  std::string err_msg_;

  const std::string &GetMessage() const {
    return std::to_string(code_) + err_msg_;
  }

  SimpleErrorModel GetModeledError() const { return {GetMessage()}; }
};

SimpleError MakeSimpleError(const Aws::S3::S3Error &from) {
  return {static_cast<int>(from.GetErrorType()), from.GetMessage()};
}

struct ParseUriResult {
  std::string bucket_;
  std::string object_;
};

template <typename R> using SimpleOutcome = Aws::Utils::Outcome<R, SimpleError>;

using ParseURIOutcome = SimpleOutcome<ParseUriResult>;
using SizeOutcome = SimpleOutcome<long long>;

ParseURIOutcome ParseS3Uri(
    const std::string
        &s3_uri) { //, std::string &bucket_name, std::string &object_name) {
  const std::string prefix = "s3://";
  const size_t prefix_size = prefix.size();
  if (s3_uri.compare(0, prefix_size, prefix) != 0) {
    return SimpleError{
        static_cast<int>(Aws::S3::S3Errors::INVALID_PARAMETER_VALUE),
        "Invalid S3 URI: " + s3_uri}; //{"", "", "Invalid S3 URI: " + s3_uri};
    // spdlog::error("Invalid S3 URI: {}", s3_uri);
    // return false;
  }

  size_t pos = s3_uri.find('/', prefix_size);
  if (pos == std::string::npos) {
    return SimpleError{
        static_cast<int>(Aws::S3::S3Errors::INVALID_PARAMETER_VALUE),
        "Invalid S3 URI, missing object name: " +
            s3_uri}; //{"", "", "Invalid S3 URI, missing object name: " +
                     // s3_uri};
    // spdlog::error("Invalid S3 URI, missing object name: {}", s3_uri);
    // return false;
  }

  std::string bucket_name = s3_uri.substr(prefix_size, pos - prefix_size);

  if (bucket_name.empty()) {
    if (globalBucketName.empty()) {
      return SimpleError{
          static_cast<int>(Aws::S3::S3Errors::MISSING_PARAMETER),
          "No bucket specified, and GCS_BUCKET_NAME is not set!"};
    }
    bucket_name = globalBucketName;
  }

  std::string object_name = s3_uri.substr(pos + 1);

  return ParseUriResult{std::move(bucket_name), std::move(object_name)};
}

// void FallbackToDefaultBucket(std::string &bucket_name) {
//   if (!bucket_name.empty())
//     return;
//   if (!globalBucketName.empty()) {
//     bucket_name = globalBucketName;
//     return;
//   }
//   spdlog::critical("No bucket specified, and GCS_BUCKET_NAME is not set!");
// }

std::string GetEnvironmentVariableOrDefault(const std::string &variable_name,
                                            const std::string &default_value) {
  const char *value = getenv(variable_name.c_str());
  return value ? value : default_value;
}

// Implementation of driver functions

const char *driver_getDriverName() { return "S3 driver"; }

const char *driver_getVersion() { return "0.1.0"; }

const char *driver_getScheme() { return "s3"; }

int driver_isReadOnly() { return 0; }

int driver_connect() {

  auto file_exists = [](const std::string &name) {
    std::ifstream ifile(name);
    return (ifile.is_open());
  };

  const auto loglevel =
      GetEnvironmentVariableOrDefault("S3_DRIVER_LOGLEVEL", "info");
  if (loglevel == "debug")
    spdlog::set_level(spdlog::level::debug);
  else if (loglevel == "trace")
    spdlog::set_level(spdlog::level::trace);
  else
    spdlog::set_level(spdlog::level::info);

  spdlog::debug("Connect {}", loglevel);

  // Configuration: we honor both standard AWS config files and environment
  // variables If both configuration files and environment variables are set
  // precedence is given to environment variables
  std::string s3endpoint = "";
  std::string s3region = "us-east-1";

  // Load AWS configuration from file
  Aws::Auth::AWSCredentials configCredentials;
  std::string userHome = GetEnvironmentVariableOrDefault("HOME", "");
  if (!userHome.empty()) {
    std::ostringstream defaultConfig_os;
    defaultConfig_os << userHome << "/.aws/config";
    const std::string defaultConfig = defaultConfig_os.str();

    // std::string defaultConfig = std::filesystem::path(userHome)
    //                                 .append(".aws")
    //                                 .append("config")
    //                                 .string();

    const std::string configFile =
        GetEnvironmentVariableOrDefault("AWS_CONFIG_FILE", defaultConfig);
    spdlog::debug("Conf file = {}", configFile);

    if (file_exists(configFile)) {
      // if (std::filesystem::exists(std::filesystem::path(configFile))) {
      const std::string profile =
          GetEnvironmentVariableOrDefault("AWS_PROFILE", "default");

      spdlog::debug("Profile = {}", profile);

      const std::string profileSection =
          (profile != "default") ? "profile " + profile : profile;

      Aws::Auth::ProfileConfigFileAWSCredentialsProvider provider(
          profile.c_str());
      configCredentials = provider.GetAWSCredentials();

      mINI::INIFile file(configFile);
      mINI::INIStructure ini;
      file.read(ini);
      std::string confEndpoint = ini.get(profileSection).get("endpoint_url");
      if (!confEndpoint.empty()) {
        s3endpoint = std::move(confEndpoint);
      }
      spdlog::debug("Endpoint = {}", s3endpoint);

      std::string confRegion = ini.get(profileSection).get("region");
      if (!confRegion.empty()) {
        s3region = std::move(confRegion);
      }
      spdlog::debug("Region = {}", s3region);
    } else if (configFile != defaultConfig) {
      return kFailure;
    }
  }

  // Initialize variables from environment
  // Both AWS_xxx standard variables and AutoML S3_xxx variables are supported
  // If both are present, AWS_xxx variables will be given precedence
  globalBucketName = GetEnvironmentVariableOrDefault("S3_BUCKET_NAME", "");
  s3endpoint = GetEnvironmentVariableOrDefault("S3_ENDPOINT", s3endpoint);
  s3endpoint = GetEnvironmentVariableOrDefault("AWS_ENDPOINT_URL", s3endpoint);
  s3region = GetEnvironmentVariableOrDefault("AWS_DEFAULT_REGION", s3region);
  std::string s3accessKey =
      GetEnvironmentVariableOrDefault("S3_ACCESS_KEY", "");
  s3accessKey =
      GetEnvironmentVariableOrDefault("AWS_ACCESS_KEY_ID", s3accessKey);
  std::string s3secretKey =
      GetEnvironmentVariableOrDefault("S3_SECRET_KEY", "");
  s3secretKey =
      GetEnvironmentVariableOrDefault("AWS_SECRET_ACCESS_KEY", s3secretKey);
  if ((s3accessKey != "" && s3secretKey == "") ||
      (s3accessKey == "" && s3secretKey != "")) {
    LogError("Access key and secret configuration is only permitted "
             "when both values are provided.");
    return false;
  }

  // Initialisation du SDK AWS
  Aws::InitAPI(options);

  Aws::Client::ClientConfiguration clientConfig;
  clientConfig.allowSystemProxy = true;
  clientConfig.verifySSL = false;
  if (s3endpoint != "") {
    clientConfig.endpointOverride = s3endpoint;
  }
  if (s3region != "") {
    clientConfig.region = s3region;
  }

  if (!s3accessKey.empty()) {
    configCredentials = Aws::Auth::AWSCredentials(s3accessKey, s3secretKey);
  }
  client.reset(new Aws::S3::S3Client(
      configCredentials,
      Aws::MakeShared<Aws::S3::S3EndpointProvider>(S3EndpointProvider),
      clientConfig));

  bIsConnected = true;
  return kSuccess;
}

int driver_disconnect() {
  if (client) {
    ShutdownAPI(options);
  }
  bIsConnected = false;
  return kSuccess;
}

int driver_isConnected() { return bIsConnected; }

long long int driver_getSystemPreferredBufferSize() {
  constexpr long long buff_size = 4L * 1024L * 1024L;
  return buff_size; // 4 Mo
}

int driver_exist(const char *filename) {
  ERROR_ON_NULL_ARG(filename, kFalse);

  const size_t size = std::strlen(filename);
  if (0 == size) {
    LogError("Error passing an empty name to driver_exist");
    return kFalse;
  }

  spdlog::debug("exist {}", filename);

  // const std::string file_uri = filename;
  // spdlog::debug("exist file_uri {}", file_uri);
  const char last_char = filename[std::strlen(filename) - 1];
  spdlog::debug("exist last char {}", last_char);

  if (last_char == '/') {
    return driver_dirExists(filename);
  } else {
    return driver_fileExists(filename);
  }
}

int driver_fileExists(const char *sFilePathName) {
  ERROR_ON_NULL_ARG(sFilePathName, kFalse);

  spdlog::debug("fileExist {}", sFilePathName);

  // auto maybe_parsed_names = ParseS3Uri(sFilePathName);
  // ERROR_ON_NAMES(maybe_parsed_names, kFalse);
  // auto& parsed_names = maybe_parsed_names.GetResult();

  NAMES_OR_ERROR(sFilePathName, kFalse);

  // Exécution de la requête HEAD
  const auto head_object_outcome = HeadObject(names.bucket_, names.object_);
  if (!head_object_outcome.IsSuccess()) {
    auto &error = head_object_outcome.GetError();
    spdlog::debug("Failed retrieving file info: {} {}",
                  static_cast<int>(error.GetErrorType()), error.GetMessage());

    return kFailure;
  }

  // Retourne true si l'objet existe, false sinon
  return kSuccess;
}

int driver_dirExists(const char *sFilePathName) {
  ERROR_ON_NULL_ARG(sFilePathName, kFalse);

  spdlog::debug("dirExist {}", sFilePathName);

  return kTrue;
}

SizeOutcome getFileSize(const std::string &bucket_name,
                        const std::string &object_name) {
  // Exécution de la requête HEAD
  const auto head_object_outcome = HeadObject(bucket_name, object_name);

  if (!head_object_outcome.IsSuccess()) {
    return MakeSimpleError(head_object_outcome.GetError());
  }

  // Retourne la taille de l'objet s'il existe
  return head_object_outcome.GetResult().GetContentLength();
}

long long int driver_getFileSize(const char *filename) {
  ERROR_ON_NULL_ARG(filename, kBadSize);

  spdlog::debug("getFileSize {}", filename);

  // auto maybe_parsed_names = ParseS3Uri(filename);
  // ERROR_ON_NAMES(maybe_parsed_names, kBadSize);
  // auto& parsed_names = maybe_parsed_names.GetResult();

  NAMES_OR_ERROR(filename, kBadSize);

  auto maybe_file_size = getFileSize(names.bucket_, names.object_);

  RETURN_ON_ERROR(maybe_file_size, "Error getting file size", kBadSize);

  return maybe_file_size.GetResult();
}

void *driver_fopen(const char *filename, char mode) {
  assert(driver_isConnected());

  ERROR_ON_NULL_ARG(filename, nullptr);

  spdlog::debug("fopen {} {}", filename, mode);

  NAMES_OR_ERROR(filename, nullptr);

  // auto maybe_parsed_names = ParseS3Uri(filename);
  // RETURN_ON_ERROR(maybe_parsed_names, "Error parsing names in fopen",
  // nullptr); auto& names = maybe_parsed_names.GetResult();

  auto h = new MultiPartFile;
  h->bucketname = names.bucket_;
  h->filename = names.object_;
  h->offset = 0;

  switch (mode) {
  case 'r': {
    if (false) /* isMultifile(object_name)*/
    {
      // TODO
      // identify files part of multifile glob pattern
      // initialize cumulative size array
      // determine if header is repeated
    } else {
      h->filenames.push_back(names.object_);
      auto maybe_filesize = getFileSize(names.bucket_, names.object_);
      RETURN_ON_ERROR(maybe_filesize, "Error while opening file in read mode",
                      nullptr);
      // long long int fileSize = getFileSize(bucket_name, object_name);
      h->cumulativeSize.push_back(maybe_filesize.GetResult());
    }
    return h;
  }
  case 'w':
    return h;

  case 'a':

  default:
    spdlog::error("Invalid open mode {}", mode);
    return 0;
  }
}

int driver_fclose(void *stream) {
  assert(driver_isConnected());

  ERROR_ON_NULL_ARG(stream, kFailure);

  spdlog::debug("fclose {}", (void *)stream);

  MultiPartFile *multiFile;

  multiFile = (MultiPartFile *)stream;
  delete ((MultiPartFile *)stream);
  return 0;
}

long long int totalSize(MultiPartFile *h) {
  return h->cumulativeSize[h->cumulativeSize.size() - 1];
}

int driver_fseek(void *stream, long long int offset, int whence) {
  ERROR_ON_NULL_ARG(stream, kBadSize);

  spdlog::debug("fseek {} {} {}", stream, offset, whence);

  MultiPartFile *h = (MultiPartFile *)stream;

  switch (whence) {
  case std::ios::beg:
    h->offset = offset;
    return 0;
  case std::ios::cur: {
    auto computedOffset = h->offset + offset;
    if (computedOffset < 0) {
      spdlog::critical("Invalid seek offset {}", computedOffset);
      return -1;
    }
    h->offset = computedOffset;
    return 0;
  }
  case std::ios::end: {
    auto computedOffset = totalSize(h) + offset;
    if (computedOffset < 0) {
      spdlog::critical("Invalid seek offset {}", computedOffset);
      return -1;
    }
    h->offset = computedOffset;
    return 0;
  }
  default:
    spdlog::critical("Invalid seek mode {}", whence);
    return -1;
  }
}

const char *driver_getlasterror() {
  spdlog::debug("getlasterror");

  return NULL;
}

long long int driver_fread(void *ptr, size_t size, size_t count, void *stream) {
  spdlog::debug("fread {} {} {} {}", ptr, size, count, stream);

  assert(stream != NULL);
  MultiPartFile *h = (MultiPartFile *)stream;

  // TODO: handle multifile case
  auto toRead = size * count;
  spdlog::debug("offset = {} toRead = {}", h->offset, toRead);

  auto num_read =
      DownloadFileRangeToBuffer(h->bucketname, h->filename, (char *)ptr, toRead,
                                h->offset, h->offset + toRead);

  if (num_read != -1)
    h->offset += num_read;
  return num_read;
}

long long int driver_fwrite(const void *ptr, size_t size, size_t count,
                            void *stream) {
  spdlog::debug("fwrite {} {} {} {}", ptr, size, count, stream);

  assert(stream != NULL);
  MultiPartFile *h = (MultiPartFile *)stream;

  UploadBuffer(h->bucketname, h->filename, (char *)ptr, size * count);

  // TODO proper error handling...
  return size * count;
}

int driver_fflush(void *stream) {
  spdlog::debug("Flushing (does nothing...)");
  return 0;
}

int driver_remove(const char *filename) {
  assert(driver_isConnected());

  ERROR_ON_NULL_ARG(filename, kFalse);

  spdlog::debug("remove {}", filename);

  NAMES_OR_ERROR(filename, kFalse);

  // auto maybe_parsed_names = ParseS3Uri(filename);
  // ERROR_ON_NAMES(maybe_parsed_names, kFalse);
  // auto& names = maybe_parsed_names.GetResult();
  // std::string bucket_name, object_name;
  // ParseS3Uri(filename, bucket_name, object_name);
  // FallbackToDefaultBucket(bucket_name);

  Aws::S3::Model::DeleteObjectRequest request;

  request.WithBucket(names.bucket_).WithKey(names.object_);

  Aws::S3::Model::DeleteObjectOutcome outcome = client->DeleteObject(request);

  if (!outcome.IsSuccess()) {
    auto err = outcome.GetError();
    spdlog::error("DeleteObject: {} {}", err.GetExceptionName(),
                  err.GetMessage());
  }

  return outcome.IsSuccess();
}

int driver_rmdir(const char *filename) {
  assert(driver_isConnected());

  ERROR_ON_NULL_ARG(filename, kFailure);
  spdlog::debug("rmdir {}", filename);

  spdlog::debug("Remove dir (does nothing...)");
  return kSuccess;
}

int driver_mkdir(const char *filename) {
  assert(driver_isConnected());

  ERROR_ON_NULL_ARG(filename, kFailure);
  spdlog::debug("mkdir {}", filename);

  return 1;
}

long long int driver_diskFreeSpace(const char *filename) {
  spdlog::debug("diskFreeSpace {}", filename);

  assert(driver_isConnected());
  return (long long int)5 * 1024 * 1024 * 1024 * 1024;
}

int driver_copyToLocal(const char *sSourceFilePathName,
                       const char *sDestFilePathName) {
  spdlog::debug("copyToLocal {} {}", sSourceFilePathName, sDestFilePathName);

  assert(driver_isConnected());

  ERROR_ON_NULL_ARG(sSourceFilePathName, kBadSize);
  ERROR_ON_NULL_ARG(sDestFilePathName, kBadSize);
  NAMES_OR_ERROR(sSourceFilePathName, kBadSize);
  // std::string bucket_name, object_name;
  // ParseS3Uri(sSourceFilePathName, bucket_name, object_name);
  // FallbackToDefaultBucket(bucket_name);

  // Configuration de la requête pour obtenir un objet
  Aws::S3::Model::GetObjectRequest object_request;
  object_request.WithBucket(names.bucket_).WithKey(names.object_);

  // Exécution de la requête
  auto get_object_outcome = client->GetObject(object_request);
  long long int num_read = -1;

  if (!get_object_outcome.IsSuccess()) {
    spdlog::error("Error initializing download stream: {}",
                  get_object_outcome.GetError().GetMessage());
    return false;
  }

  // Open the local file
  std::ofstream file_stream(sDestFilePathName, std::ios::binary);
  if (!file_stream.is_open()) {
    spdlog::error("Failed to open local file for writing: {}",
                  sDestFilePathName);
    return false;
  }

  // Téléchargement réussi, copie des données vers le fichier local
  file_stream << get_object_outcome.GetResult().GetBody().rdbuf();
  file_stream.close();

  spdlog::debug("Close output");
  file_stream.close();

  spdlog::debug("Done copying");

  return 1;
}

int driver_copyFromLocal(const char *sSourceFilePathName,
                         const char *sDestFilePathName) {
  assert(driver_isConnected());

  ERROR_ON_NULL_ARG(sSourceFilePathName, kFailure);
  ERROR_ON_NULL_ARG(sDestFilePathName, kFailure);

  spdlog::debug("copyFromLocal {} {}", sSourceFilePathName, sDestFilePathName);

  NAMES_OR_ERROR(sDestFilePathName, kFailure);
  // std::string bucket_name, object_name;
  // ParseS3Uri(sDestFilePathName, bucket_name, object_name);
  // FallbackToDefaultBucket(bucket_name);

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
    spdlog::error("Error during file upload: {}",
                  put_object_outcome.GetError().GetMessage());
    return false;
  }

  return true;
}
