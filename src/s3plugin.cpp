#ifdef __CYGWIN__
#define _CRT_SECURE_NO_WARNINGS
#endif

#include "s3plugin.h"
#include "s3plugin_internal.h"
#include "contrib/matching.h"

#include "spdlog/spdlog.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>

#include "ini.h"

#include <algorithm>
#include <assert.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>

using namespace s3plugin;

using S3Object = Aws::S3::Model::Object;

int bIsConnected = false;

constexpr const char* S3EndpointProvider = "KHIOPS_ML_S3";

Aws::SDKOptions options;
std::unique_ptr<Aws::S3::S3Client> client;

// Global bucket name
std::string globalBucketName = "";

HandleContainer active_handles;

std::string last_error;

constexpr const char* nullptr_msg_stub = "Error passing null pointer to ";

// test utilities

void test_setClient(Aws::S3::S3Client* mock_client)
{
	client.reset(mock_client);
	bIsConnected = kTrue;
}

void test_unsetClient()
{
	client.reset();
	bIsConnected = kFalse;
}

void test_clearHandles()
{
	active_handles.clear();
}

void test_cleanupClient()
{
	test_clearHandles();
	test_unsetClient();
}

void* test_getActiveHandles()
{
	return &active_handles;
}

#define ERROR_ON_NULL_ARG(arg, err_val)                                                                                \
	if (!(arg))                                                                                                    \
	{                                                                                                              \
		std::ostringstream os;                                                                                 \
		os << nullptr_msg_stub << __func__;                                                                    \
		LogError(os.str());                                                                                    \
		return (err_val);                                                                                      \
	}

#define IF_ERROR(outcome) if (!(outcome).IsSuccess())

#define RETURN_OUTCOME_ON_ERROR(outcome)                                                                               \
	IF_ERROR((outcome))                                                                                            \
	{                                                                                                              \
		return {MakeSimpleError((outcome).GetError())};                                                        \
	}

#define PASS_OUTCOME_ON_ERROR(outcome)                                                                                 \
	IF_ERROR((outcome))                                                                                            \
	{                                                                                                              \
		return (outcome).GetError();                                                                           \
	}

#define RETURN_ON_ERROR(outcome, msg, err_val)                                                                         \
	{                                                                                                              \
		IF_ERROR((outcome))                                                                                    \
		{                                                                                                      \
			LogBadOutcome((outcome), (msg));                                                               \
			return (err_val);                                                                              \
		}                                                                                                      \
	}

#define ERROR_ON_NAMES(status_or_names, err_val) RETURN_ON_ERROR((status_or_names), "Error parsing URL", (err_val))

#define NAMES_OR_ERROR(arg, err_val)                                                                                   \
	auto maybe_parsed_names = ParseS3Uri((arg));                                                                   \
	ERROR_ON_NAMES(maybe_parsed_names, (err_val));                                                                 \
	auto& names = maybe_parsed_names.GetResult();

void LogError(const std::string& msg)
{
	spdlog::error(msg);
	last_error = std::move(msg);
}

template <typename R, typename E> void LogBadOutcome(const Aws::Utils::Outcome<R, E>& outcome, const std::string& msg)
{
	std::ostringstream os;
	os << msg << ": " << outcome.GetError().GetMessage();
	LogError(os.str());
}

template <typename RequestType> RequestType MakeBaseRequest(const std::string& bucket, const std::string& object)
{
	RequestType request;
	request.WithBucket(bucket).WithKey(object);
	return request;
}

Aws::S3::Model::HeadObjectRequest MakeHeadObjectRequest(const std::string& bucket, const std::string& object)
{
	return MakeBaseRequest<Aws::S3::Model::HeadObjectRequest>(bucket, object);
}

Aws::S3::Model::GetObjectRequest MakeGetObjectRequest(const std::string& bucket, const std::string& object,
						      std::string&& range = "")
{
	auto request = MakeBaseRequest<Aws::S3::Model::GetObjectRequest>(bucket, object);
	if (!range.empty())
	{
		request.SetRange(std::move(range));
	}
	return request;
}

Aws::S3::Model::GetObjectOutcome GetObject(const std::string& bucket, const std::string& object,
					   std::string&& range = "")
{
	return client->GetObject(MakeGetObjectRequest(bucket, object, std::move(range)));
}

Aws::S3::Model::HeadObjectOutcome HeadObject(const std::string& bucket, const std::string& object)
{
	return client->HeadObject(MakeHeadObjectRequest(bucket, object));
}

// Definition of helper functions
long long int DownloadFileRangeToBuffer(const std::string& bucket_name, const std::string& object_name, char* buffer,
					std::size_t buffer_length, std::int64_t start_range, std::int64_t end_range)
{

	// Définition de la plage de bytes à télécharger
	Aws::StringStream range;
	range << "bytes=" << start_range << "-" << end_range;

	// Exécution de la requête
	auto get_object_outcome = GetObject(bucket_name, object_name, range.str());
	// // Configuration de la requête pour obtenir un objet
	// auto object_request = MakeHeadObjectRequest(bucket_name, object_name,
	// range.str());

	long long int num_read = -1;

	if (get_object_outcome.IsSuccess())
	{
		// Téléchargement réussi, lecture des données dans le buffer
		auto& retrieved_file = get_object_outcome.GetResultWithOwnership().GetBody();

		retrieved_file.read(buffer, buffer_length);
		num_read = retrieved_file.gcount();
		spdlog::debug("read = {}", num_read);
	}
	else
	{
		// Gestion des erreurs
		std::cerr << "Failed to download object: " << get_object_outcome.GetError().GetMessage() << std::endl;

		num_read = -1;
	}
	return num_read;
}

bool UploadBuffer(const std::string& bucket_name, const std::string& object_name, const char* buffer,
		  std::size_t buffer_size)
{

	Aws::S3::Model::PutObjectRequest request;
	request.SetBucket(bucket_name);
	request.SetKey(object_name);

	const std::shared_ptr<Aws::IOStream> inputData = Aws::MakeShared<Aws::StringStream>("");
	std::string data;
	data.assign(buffer, buffer_size);
	*inputData << data.c_str();

	request.SetBody(inputData);

	Aws::S3::Model::PutObjectOutcome outcome = client->PutObject(request);

	if (!outcome.IsSuccess())
	{
		spdlog::error("PutObjectBuffer: {}", outcome.GetError().GetMessage());
	}

	return outcome.IsSuccess();
}

struct SimpleErrorModel
{
	std::string err_msg_;

	const std::string& GetMessage() const
	{
		return err_msg_;
	}
};

struct SimpleError
{
	int code_;
	std::string err_msg_;

	std::string GetMessage() const
	{
		return std::to_string(code_) + err_msg_;
	}

	SimpleErrorModel GetModeledError() const
	{
		return {GetMessage()};
	}
};

SimpleError MakeSimpleError(Aws::S3::S3Errors err_code, std::string&& err_msg)
{
	return {static_cast<int>(err_code), std::move(err_msg)};
}

SimpleError MakeSimpleError(Aws::S3::S3Errors err_code, const char* err_msg)
{
	return {static_cast<int>(err_code), err_msg};
}

SimpleError MakeSimpleError(const Aws::S3::S3Error& from)
{
	return {static_cast<int>(from.GetErrorType()), from.GetMessage()};
}

struct ParseUriResult
{
	std::string bucket_;
	std::string object_;
};

using ObjectsVec = std::vector<S3Object>;

template <typename R> using SimpleOutcome = Aws::Utils::Outcome<R, SimpleError>;

using ParseURIOutcome = SimpleOutcome<ParseUriResult>;
using SizeOutcome = SimpleOutcome<long long>;
using FilterOutcome = SimpleOutcome<ObjectsVec>;

ParseURIOutcome ParseS3Uri(const std::string& s3_uri)
{ //, std::string &bucket_name, std::string &object_name) {
	const std::string prefix = "s3://";
	const size_t prefix_size = prefix.size();
	if (s3_uri.compare(0, prefix_size, prefix) != 0)
	{
		return SimpleError{static_cast<int>(Aws::S3::S3Errors::INVALID_PARAMETER_VALUE),
				   "Invalid S3 URI: " + s3_uri};
		//{"", "", "Invalid S3 URI: " + s3_uri};
		// spdlog::error("Invalid S3 URI: {}", s3_uri);
		// return false;
	}

	size_t pos = s3_uri.find('/', prefix_size);
	if (pos == std::string::npos)
	{
		return SimpleError{static_cast<int>(Aws::S3::S3Errors::INVALID_PARAMETER_VALUE),
				   "Invalid S3 URI, missing object name: " + s3_uri};
		//{"", "", "Invalid S3 URI, missing object name: " +
		// s3_uri};
		// spdlog::error("Invalid S3 URI, missing object name: {}", s3_uri);
		// return false;
	}

	std::string bucket_name = s3_uri.substr(prefix_size, pos - prefix_size);

	if (bucket_name.empty())
	{
		if (globalBucketName.empty())
		{
			return SimpleError{static_cast<int>(Aws::S3::S3Errors::MISSING_PARAMETER),
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

std::string GetEnvironmentVariableOrDefault(const std::string& variable_name, const std::string& default_value)
{
	const char* value = getenv(variable_name.c_str());
	return value ? value : default_value;
}

bool IsMultifile(const std::string& pattern, size_t& first_special_char_idx)
{
	spdlog::debug("Parse multifile pattern {}", pattern);

	constexpr auto special_chars = "*?![^";

	size_t from_offset = 0;
	size_t found_at = pattern.find_first_of(special_chars, from_offset);
	while (found_at != std::string::npos)
	{
		const char found = pattern[found_at];
		spdlog::debug("special char {} found at {}", found, found_at);

		if (found_at > 0 && pattern[found_at - 1] == '\\')
		{
			spdlog::debug("preceded by a \\, so not so special");
			from_offset = found_at + 1;
			found_at = pattern.find_first_of(special_chars, from_offset);
		}
		else
		{
			spdlog::debug("not preceded by a \\, so really a special char");
			first_special_char_idx = found_at;
			return true;
		}
	}
	return false;
}

// func isMultifile(p string) int {
// 	globalIdx := 0
// 	for {
// 		log.Debugln("Parse multifile pattern", p)
// 		idxChar := strings.IndexAny(p, "*?![^")
// 		if idxChar >= 0 {
// 			log.Debugln("  special char", string([]rune(p)[idxChar]), "found at", globalIdx+idxChar)
// 			globalIdx += idxChar
// 			if idxChar > 0 && string([]rune(p)[idxChar-1]) == "\\" {
// 				log.Debugln("  preceded by a \\, so not so special...")
// 				p = trimLeftChars(p, idxChar+1)
// 			} else {
// 				log.Debugln("  not preceded by a \\, so really a special char...")
// 				return globalIdx
// 			}
// 		}
// 		if idxChar == -1 {
// 			break
// 		}
// 	}
// 	return -1
// }

Aws::S3::Model::ListObjectsV2Outcome ListObjects(const std::string& bucket, const std::string& pattern)
{
	Aws::S3::Model::ListObjectsV2Request request;
	request.WithBucket(bucket).WithPrefix(pattern).WithDelimiter("");
	return client->ListObjectsV2(request);
}

// Get from a bucket a list of objects matching a name pattern.
// To get a limited list of objects to filter per request, the request includes a well defined
// prefix contained in the pattern
FilterOutcome FilterList(const std::string& bucket, const std::string& pattern, size_t pattern_1st_sp_char_pos)
{
	ObjectsVec res;

	Aws::S3::Model::ListObjectsV2Request request;
	request.WithBucket(bucket).WithPrefix(pattern.substr(0, pattern_1st_sp_char_pos)); //.WithDelimiter("");
	Aws::String continuation_token;

	do
	{
		if (!continuation_token.empty())
		{
			request.SetContinuationToken(continuation_token);
		}
		const Aws::S3::Model::ListObjectsV2Outcome outcome = client->ListObjectsV2(request);

		RETURN_OUTCOME_ON_ERROR(outcome);

		const auto& list_result = outcome.GetResult();
		const auto& objects = list_result.GetContents();
		std::copy_if(objects.begin(), objects.end(), std::back_inserter(res),
			     [&](const S3Object& obj) { return utils::gitignore_glob_match(obj.GetKey(), pattern); });
		continuation_token = list_result.GetContinuationToken();

	} while (!continuation_token.empty());

	return res;
}

#define KH_S3_FILTER_LIST(var, bucket, pattern, pattern_1st_sp_char_pos)                                               \
	const auto var##_outcome = FilterList(bucket, pattern, pattern_1st_sp_char_pos);                               \
	PASS_OUTCOME_ON_ERROR(var##_outcome);                                                                          \
	const ObjectsVec& var = var##_outcome.GetResult();

// Implementation of driver functions

const char* driver_getDriverName()
{
	return "S3 driver";
}

const char* driver_getVersion()
{
	return "0.1.0";
}

const char* driver_getScheme()
{
	return "s3";
}

int driver_isReadOnly()
{
	return 0;
}

int driver_connect()
{

	auto file_exists = [](const std::string& name)
	{
		std::ifstream ifile(name);
		return (ifile.is_open());
	};

	const auto loglevel = GetEnvironmentVariableOrDefault("S3_DRIVER_LOGLEVEL", "info");
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
	if (!userHome.empty())
	{
		std::ostringstream defaultConfig_os;
		defaultConfig_os << userHome << "/.aws/config";
		const std::string defaultConfig = defaultConfig_os.str();

		// std::string defaultConfig = std::filesystem::path(userHome)
		//                                 .append(".aws")
		//                                 .append("config")
		//                                 .string();

		const std::string configFile = GetEnvironmentVariableOrDefault("AWS_CONFIG_FILE", defaultConfig);
		spdlog::debug("Conf file = {}", configFile);

		if (file_exists(configFile))
		{
			// if (std::filesystem::exists(std::filesystem::path(configFile))) {
			const std::string profile = GetEnvironmentVariableOrDefault("AWS_PROFILE", "default");

			spdlog::debug("Profile = {}", profile);

			const std::string profileSection = (profile != "default") ? "profile " + profile : profile;

			Aws::Auth::ProfileConfigFileAWSCredentialsProvider provider(profile.c_str());
			configCredentials = provider.GetAWSCredentials();

			mINI::INIFile file(configFile);
			mINI::INIStructure ini;
			file.read(ini);
			std::string confEndpoint = ini.get(profileSection).get("endpoint_url");
			if (!confEndpoint.empty())
			{
				s3endpoint = std::move(confEndpoint);
			}
			spdlog::debug("Endpoint = {}", s3endpoint);

			std::string confRegion = ini.get(profileSection).get("region");
			if (!confRegion.empty())
			{
				s3region = std::move(confRegion);
			}
			spdlog::debug("Region = {}", s3region);
		}
		else if (configFile != defaultConfig)
		{
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
	std::string s3accessKey = GetEnvironmentVariableOrDefault("S3_ACCESS_KEY", "");
	s3accessKey = GetEnvironmentVariableOrDefault("AWS_ACCESS_KEY_ID", s3accessKey);
	std::string s3secretKey = GetEnvironmentVariableOrDefault("S3_SECRET_KEY", "");
	s3secretKey = GetEnvironmentVariableOrDefault("AWS_SECRET_ACCESS_KEY", s3secretKey);
	if ((s3accessKey != "" && s3secretKey == "") || (s3accessKey == "" && s3secretKey != ""))
	{
		LogError("Access key and secret configuration is only permitted "
			 "when both values are provided.");
		return false;
	}

	// Initialisation du SDK AWS
	Aws::InitAPI(options);

	Aws::Client::ClientConfiguration clientConfig;
	clientConfig.allowSystemProxy = true;
	clientConfig.verifySSL = false;
	if (s3endpoint != "")
	{
		clientConfig.endpointOverride = s3endpoint;
	}
	if (s3region != "")
	{
		clientConfig.region = s3region;
	}

	if (!s3accessKey.empty())
	{
		configCredentials = Aws::Auth::AWSCredentials(s3accessKey, s3secretKey);
	}
	client.reset(new Aws::S3::S3Client(
	    configCredentials, Aws::MakeShared<Aws::S3::S3EndpointProvider>(S3EndpointProvider), clientConfig));

	bIsConnected = true;
	return kSuccess;
}

int driver_disconnect()
{
	if (client)
	{
		ShutdownAPI(options);
	}
	bIsConnected = false;
	return kSuccess;
}

int driver_isConnected()
{
	return bIsConnected;
}

long long int driver_getSystemPreferredBufferSize()
{
	constexpr long long buff_size = 4L * 1024L * 1024L;
	return buff_size; // 4 Mo
}

int driver_exist(const char* filename)
{
	ERROR_ON_NULL_ARG(filename, kFalse);

	const size_t size = std::strlen(filename);
	if (0 == size)
	{
		LogError("Error passing an empty name to driver_exist");
		return kFalse;
	}

	spdlog::debug("exist {}", filename);

	// const std::string file_uri = filename;
	// spdlog::debug("exist file_uri {}", file_uri);
	const char last_char = filename[std::strlen(filename) - 1];
	spdlog::debug("exist last char {}", last_char);

	if (last_char == '/')
	{
		return driver_dirExists(filename);
	}
	else
	{
		return driver_fileExists(filename);
	}
}

int driver_fileExists(const char* sFilePathName)
{
	ERROR_ON_NULL_ARG(sFilePathName, kFalse);

	spdlog::debug("fileExist {}", sFilePathName);

	NAMES_OR_ERROR(sFilePathName, kFalse);

	size_t pattern_1st_sp_char_pos = 0;
	if (!IsMultifile(names.object_, pattern_1st_sp_char_pos))
	{
		//go ahead with the simple request
		const auto head_object_outcome = HeadObject(names.bucket_, names.object_);
		RETURN_ON_ERROR(head_object_outcome, "Failed retrieving file info in fileExists", kFalse);

		return kTrue;
	}

	// get a filtered list of the bucket files that match the pattern
	auto filter_list_outcome = FilterList(names.bucket_, names.object_, pattern_1st_sp_char_pos);
	RETURN_ON_ERROR(filter_list_outcome, "Error while filtering object list", kFalse);

	return filter_list_outcome.GetResult().empty() ? kFalse : kTrue;
}

int driver_dirExists(const char* sFilePathName)
{
	ERROR_ON_NULL_ARG(sFilePathName, kFalse);

	spdlog::debug("dirExist {}", sFilePathName);

	return kTrue;
}

SizeOutcome GetOneFileSize(const std::string& bucket, const std::string& object)
{
	const auto head_object_outcome = HeadObject(bucket, object);
	RETURN_OUTCOME_ON_ERROR(head_object_outcome);
	return head_object_outcome.GetResult().GetContentLength();
}

SimpleOutcome<std::string> ReadHeader(const std::string& bucket, const S3Object& obj)
{
	auto request = MakeGetObjectRequest(bucket, obj.GetKey());
	auto outcome = client->GetObject(request);
	IF_ERROR(outcome) {}
	// Aws::IOStream& read_stream = outcome.GetResultWithOwnership().GetBody();
	Aws::IOStream& read_stream = outcome.GetResult().GetBody();
	std::string line;
	std::getline(read_stream, line);
	if (read_stream.bad())
	{
		return MakeSimpleError(Aws::S3::S3Errors::INTERNAL_FAILURE, "header read failed");
	}
	if (!read_stream.eof())
	{
		line.push_back('\n');
	}
	if (line.empty())
	{
		return MakeSimpleError(Aws::S3::S3Errors::INTERNAL_FAILURE, "Empty header");
	}
	return line;
}

#define KH_S3_READ_HEADER(var, bucket, obj)                                                                            \
	const auto var##_outcome = ReadHeader((bucket), (obj));                                                        \
	PASS_OUTCOME_ON_ERROR(var##_outcome);                                                                          \
	const std::string& var = var##_outcome.GetResult();

SizeOutcome getFileSize(const std::string& bucket_name, const std::string& object_name)
{
	// tweak the request for the object. if the object parameter is in fact a pattern,
	// the pattern could point to a list of objects that constitute a whole file

	size_t pattern_1st_sp_char_pos = 0;
	if (!IsMultifile(object_name, pattern_1st_sp_char_pos))
	{
		//go ahead with the simple request
		return GetOneFileSize(bucket_name, object_name);
	}

	KH_S3_FILTER_LIST(file_list, bucket_name, object_name,
			  pattern_1st_sp_char_pos); // !! puts file_list and file_list_outcome into scope

	// auto file_list_outcome = FilterList(bucket_name, object_name, prefix_idx);
	// PASS_OUTCOME_ON_ERROR(file_list_outcome);
	// ObjectsVec& file_list = file_list_outcome.GetResult();

	// get the size of the first file
	const S3Object& first_file = file_list.front();
	long long total_size = first_file.GetSize();

	// read the size of the header

	// auto header_outcome = ReadHeader(bucket_name, file);
	// PASS_OUTCOME_ON_ERROR(header_outcome);
	// const std::string& header = header_outcome.GetResult();

	KH_S3_READ_HEADER(header, bucket_name, first_file); // !! puts header and outcome_header into scope

	const size_t header_size = header.size();

	// scan the next files and adjust effective size if header is repeated
	int nb_headers_to_subtract = 0;
	bool same_header = true;

	for (size_t i = 1; i < file_list.size(); i++)
	{
		const S3Object& curr_file = file_list[i];
		if (same_header)
		{
			KH_S3_READ_HEADER(curr_header, bucket_name,
					  curr_file); // !! puts curr_header_outcome and curr_header into scope

			same_header = (header == curr_header);
			if (same_header)
			{
				nb_headers_to_subtract++;
			}
		}
		total_size += curr_file.GetSize();
	}

	if (!same_header)
	{
		nb_headers_to_subtract = 0;
	}
	return total_size - nb_headers_to_subtract * header_size;
}

long long int driver_getFileSize(const char* filename)
{
	ERROR_ON_NULL_ARG(filename, kBadSize);

	spdlog::debug("getFileSize {}", filename);

	NAMES_OR_ERROR(filename, kBadSize);
	const auto maybe_file_size = getFileSize(names.bucket_, names.object_);
	RETURN_ON_ERROR(maybe_file_size, "Error getting file size", kBadSize);

	return maybe_file_size.GetResult();
}

SimpleOutcome<ReaderPtr> MakeReaderPtr(std::string bucketname, std::string objectname)
{
	size_t pattern_1st_sp_char_pos = 0;
	if (!IsMultifile(objectname, pattern_1st_sp_char_pos))
	{
		// create a Multifile with a single file
		const auto size_outcome = GetOneFileSize(bucketname, objectname);
		PASS_OUTCOME_ON_ERROR(size_outcome);
		const long long size = size_outcome.GetResult();

		return ReaderPtr(
		    new MultiPartFile{std::move(bucketname), std::move(objectname), 0, 0, {objectname}, {size}, size});
	}

	// this is a multifile. the reader object needs the list of filenames matching the globbing pattern and their
	// metadata, mainly their respective sizes.

	// Note: getting the metadata involves a tradeoff between memory size of the data kept and the amount of data copied:
	// storing only the relevant data in the MultiPartFile struct requires another copy of each object name, since the API
	// does not allow moving from its own Object types. These copies could be avoided by keeping the entire list of Objects,
	// at the cost of the space used by the other metadata. The implementation here will save that space.

	KH_S3_FILTER_LIST(file_list, bucketname, objectname,
			  pattern_1st_sp_char_pos); // !! file_list and file_list_outcome now in scope

	const size_t file_count = file_list.size();
	std::vector<std::string> filenames(file_count);
	std::vector<long long> cumulative_size(file_count);

	// get metadata from the first file
	const auto& first_file = file_list.front();
	filenames.front() = first_file.GetKey();
	cumulative_size.front() = first_file.GetSize();
	tOffset common_header_length = 0;

	if (1 == file_count)
	{
		// unique file, make the result
		goto make_struct;
	}

	{ // block scope needed by goto jump above, otherwise the compiler complains about the local variables
		bool same_header = true;

		// more than one file, the headers need to be checked
		KH_S3_READ_HEADER(header, bucketname, first_file); // !! puts header and header_outcome into scope
		tOffset header_length = static_cast<tOffset>(header.size());

		for (size_t i = 1; i < file_count; i++)
		{
			const auto& curr_file = file_list[i];
			filenames[i] = curr_file.GetKey();
			cumulative_size[i] = cumulative_size[i - 1] + curr_file.GetSize();

			if (same_header)
			{
				// continue checking the header of the file:
				KH_S3_READ_HEADER(curr_header, bucketname,
						  curr_file); // !! puts curr_header and curr_header_outcome into scope
				same_header = (curr_header == header);
			}
		}

		// if headers remained the same, adjust the cumulative sizes
		if (same_header)
		{
			common_header_length = header_length;
			for (size_t i = 1; i < file_count; i++)
			{
				cumulative_size[i] -= (i * common_header_length);
			}
		}
	}

// construct the result
make_struct:
	const tOffset total_size = cumulative_size.back(); // keep it now, cumulative_size will be moved from
	return ReaderPtr(new MultiPartFile{std::move(bucketname), std::move(objectname), 0, common_header_length,
					   std::move(filenames), std::move(cumulative_size), total_size});
}

void* driver_fopen(const char* filename, char mode)
{
	assert(driver_isConnected());

	ERROR_ON_NULL_ARG(filename, nullptr);

	spdlog::debug("fopen {} {}", filename, mode);

	NAMES_OR_ERROR(filename, nullptr);

	switch (mode)
	{
	case 'r':
	{
		auto outcome = MakeReaderPtr(std::move(names.bucket_), std::move(names.object_));
		RETURN_ON_ERROR(outcome, "Error while opening reader stream", nullptr);
		active_handles.push_back(outcome.GetResultWithOwnership());
		return active_handles.back().get();
	}
	case 'w':
	case 'a':
	default:
		LogError("Invalid open mode " + mode);
		return nullptr;
	}
}

int driver_fclose(void* stream)
{
	assert(driver_isConnected());

	ERROR_ON_NULL_ARG(stream, kFailure);

	spdlog::debug("fclose {}", (void*)stream);

	MultiPartFile* multiFile;

	multiFile = (MultiPartFile*)stream;
	delete ((MultiPartFile*)stream);
	return 0;
}

long long int totalSize(MultiPartFile* h)
{
	return h->cumulative_sizes_[h->cumulative_sizes_.size() - 1];
}

int driver_fseek(void* stream, long long int offset, int whence)
{
	ERROR_ON_NULL_ARG(stream, kBadSize);

	spdlog::debug("fseek {} {} {}", stream, offset, whence);

	MultiPartFile* h = (MultiPartFile*)stream;

	switch (whence)
	{
	case std::ios::beg:
		h->offset_ = offset;
		return 0;
	case std::ios::cur:
	{
		auto computedOffset = h->offset_ + offset;
		if (computedOffset < 0)
		{
			spdlog::critical("Invalid seek offset {}", computedOffset);
			return -1;
		}
		h->offset_ = computedOffset;
		return 0;
	}
	case std::ios::end:
	{
		auto computedOffset = totalSize(h) + offset;
		if (computedOffset < 0)
		{
			spdlog::critical("Invalid seek offset {}", computedOffset);
			return -1;
		}
		h->offset_ = computedOffset;
		return 0;
	}
	default:
		spdlog::critical("Invalid seek mode {}", whence);
		return -1;
	}
}

const char* driver_getlasterror()
{
	spdlog::debug("getlasterror");

	return NULL;
}

long long int driver_fread(void* ptr, size_t size, size_t count, void* stream)
{
	spdlog::debug("fread {} {} {} {}", ptr, size, count, stream);

	assert(stream != NULL);
	MultiPartFile* h = (MultiPartFile*)stream;

	// TODO: handle multifile case
	auto toRead = size * count;
	spdlog::debug("offset = {} toRead = {}", h->offset_, toRead);

	auto num_read = DownloadFileRangeToBuffer(h->bucketname_, h->filename_, (char*)ptr, toRead, h->offset_,
						  h->offset_ + toRead);

	if (num_read != -1)
		h->offset_ += num_read;
	return num_read;
}

long long int driver_fwrite(const void* ptr, size_t size, size_t count, void* stream)
{
	spdlog::debug("fwrite {} {} {} {}", ptr, size, count, stream);

	assert(stream != NULL);
	MultiPartFile* h = (MultiPartFile*)stream;

	UploadBuffer(h->bucketname_, h->filename_, (char*)ptr, size * count);

	// TODO proper error handling...
	return size * count;
}

int driver_fflush(void* stream)
{
	spdlog::debug("Flushing (does nothing...)");
	return 0;
}

int driver_remove(const char* filename)
{
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

	if (!outcome.IsSuccess())
	{
		auto err = outcome.GetError();
		spdlog::error("DeleteObject: {} {}", err.GetExceptionName(), err.GetMessage());
	}

	return outcome.IsSuccess();
}

int driver_rmdir(const char* filename)
{
	assert(driver_isConnected());

	ERROR_ON_NULL_ARG(filename, kFailure);
	spdlog::debug("rmdir {}", filename);

	spdlog::debug("Remove dir (does nothing...)");
	return kSuccess;
}

int driver_mkdir(const char* filename)
{
	assert(driver_isConnected());

	ERROR_ON_NULL_ARG(filename, kFailure);
	spdlog::debug("mkdir {}", filename);

	return 1;
}

long long int driver_diskFreeSpace(const char* filename)
{
	spdlog::debug("diskFreeSpace {}", filename);

	assert(driver_isConnected());
	return (long long int)5 * 1024 * 1024 * 1024 * 1024;
}

int driver_copyToLocal(const char* sSourceFilePathName, const char* sDestFilePathName)
{
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

	if (!get_object_outcome.IsSuccess())
	{
		spdlog::error("Error initializing download stream: {}", get_object_outcome.GetError().GetMessage());
		return false;
	}

	// Open the local file
	std::ofstream file_stream(sDestFilePathName, std::ios::binary);
	if (!file_stream.is_open())
	{
		spdlog::error("Failed to open local file for writing: {}", sDestFilePathName);
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

int driver_copyFromLocal(const char* sSourceFilePathName, const char* sDestFilePathName)
{
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
	std::shared_ptr<Aws::IOStream> input_data = Aws::MakeShared<Aws::FStream>(
	    "PutObjectInputStream", sSourceFilePathName, std::ios_base::in | std::ios_base::binary);

	object_request.SetBody(input_data);

	// Exécution de la requête
	auto put_object_outcome = client->PutObject(object_request);

	if (!put_object_outcome.IsSuccess())
	{
		spdlog::error("Error during file upload: {}", put_object_outcome.GetError().GetMessage());
		return false;
	}

	return true;
}
