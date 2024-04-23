#ifdef __CYGWIN__
#define _CRT_SECURE_NO_WARNINGS
#endif

#include "s3plugin.h"
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include "spdlog/spdlog.h"
#include <assert.h>
#include <fstream>
#include <iostream>

int bIsConnected = false;
Aws::SDKOptions options;
Aws::S3::S3Client *client = NULL;

// Global bucket name
std::string globalBucketName = "";

typedef long long int tOffset;

struct MultiPartFile
{
    std::string bucketname;
	std::string filename;
	tOffset offset;
    // Added for multifile support
    tOffset commonHeaderLength;
    std::vector<std::string> filenames;
    std::vector<long long int> cumulativeSize;
};

// Definition of helper functions
long long int DownloadFileRangeToBuffer(const std::string& bucket_name,
                               const std::string& object_name,
                               char* buffer,
                               std::size_t buffer_length,
                               std::int64_t start_range,
                               std::int64_t end_range) {

    // Configuration de la requête pour obtenir un objet
    Aws::S3::Model::GetObjectRequest object_request;
    object_request.WithBucket(bucket_name).WithKey(object_name);

    // Définition de la plage de bytes à télécharger
    Aws::StringStream range;
    range << "bytes=" << start_range << "-" << end_range;
    object_request.SetRange(range.str());

    // Exécution de la requête
    auto get_object_outcome = client->GetObject(object_request);
    long long int num_read = -1;

    if (get_object_outcome.IsSuccess()) {
        // Téléchargement réussi, lecture des données dans le buffer
        auto& retrieved_file = get_object_outcome.GetResultWithOwnership().GetBody();

        retrieved_file.read(buffer, buffer_length);
        num_read = retrieved_file.gcount();
        spdlog::debug("read = {}", num_read);

    } else {
        // Gestion des erreurs
        std::cerr << "Failed to download object: " <<
            get_object_outcome.GetError().GetMessage() << std::endl;

        num_read = -1;
    }
    return num_read;
}

bool UploadBuffer(const std::string& bucket_name,
                       const std::string& object_name,
                       const char* buffer,
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

bool ParseS3Uri(const std::string& s3_uri, std::string& bucket_name, std::string& object_name) {
    const std::string prefix = "s3://";
    if (s3_uri.compare(0, prefix.size(), prefix) != 0) {
        spdlog::error("Invalid S3 URI: {}", s3_uri);
        return false;
    }

    std::size_t pos = s3_uri.find('/', prefix.size());
    if (pos == std::string::npos) {
        spdlog::error("Invalid S3 URI, missing object name: {}", s3_uri);
        return false;
    }

    bucket_name = s3_uri.substr(prefix.size(), pos - prefix.size());
    object_name = s3_uri.substr(pos + 1);

    return true;
}

void FallbackToDefaultBucket(std::string& bucket_name) {
    if (!bucket_name.empty())
        return;
    if (!globalBucketName.empty()) {
        bucket_name = globalBucketName;
        return;
    }
    spdlog::critical("No bucket specified, and GCS_BUCKET_NAME is not set!");
}

std::string GetEnvironmentVariableOrDefault(const std::string& variable_name, 
                                            const std::string& default_value)
{
    const char* value = getenv(variable_name.c_str());
    return value ? value : default_value;
}

// Implementation of driver functions

const char *driver_getDriverName()
{
	return "S3 driver";
}

const char *driver_getVersion()
{
	return "0.1.0";
}

const char *driver_getScheme()
{
	return "s3";
}

int driver_isReadOnly()
{
	return 0;
}

int driver_connect()
{
    auto loglevel = GetEnvironmentVariableOrDefault("S3_DRIVER_LOGLEVEL", "info");
    if (loglevel == "debug")
        spdlog::set_level(spdlog::level::debug);
    else if (loglevel == "trace")
        spdlog::set_level(spdlog::level::trace);
    else
        spdlog::set_level(spdlog::level::info);

    spdlog::debug("Connect {}", loglevel);

	// Initialize variables from environment
    // Both AWS_xxx standard variables and AutoML S3_xxx variables are supported
	// If both are present, AWS_xxx variables will be given precedence
	globalBucketName = GetEnvironmentVariableOrDefault("S3_BUCKET_NAME", "");
	std::string s3endpoint = GetEnvironmentVariableOrDefault("S3_ENDPOINT", "");
	s3endpoint = GetEnvironmentVariableOrDefault("AWS_ENDPOINT_URL", s3endpoint);
	std::string s3region = GetEnvironmentVariableOrDefault("AWS_DEFAULT_REGION", "us-east-1");
	std::string s3accessKey = GetEnvironmentVariableOrDefault("S3_ACCESS_KEY", "");
	s3accessKey = GetEnvironmentVariableOrDefault("AWS_ACCESS_KEY_ID", s3accessKey);
	std::string s3secretKey = GetEnvironmentVariableOrDefault("S3_SECRET_KEY", "");
	s3secretKey = GetEnvironmentVariableOrDefault("AWS_SECRET_ACCESS_KEY", s3secretKey);
	if ((s3accessKey != "" && s3secretKey == "") || (s3accessKey == "" && s3secretKey != "")) {
		spdlog::critical("Access key and secret configuration is only permitted when both values are provided.");
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
    if (s3accessKey != "") {
        Aws::Auth::AWSCredentials credentials(s3accessKey, s3secretKey);
        client = new Aws::S3::S3Client(credentials, Aws::MakeShared<Aws::S3::S3EndpointProvider>(Aws::S3::S3Client::ALLOCATION_TAG), clientConfig);
    } else {
        client = new Aws::S3::S3Client(clientConfig);
    }

    bIsConnected = true;
	return true;
}

int driver_disconnect()
{
	int nRet = 0;
    if (client != NULL) {
        delete(client);
        ShutdownAPI(options);
    }
	return nRet == 0;
}

int driver_isConnected()
{
	return bIsConnected;
}

long long int driver_getSystemPreferredBufferSize()
{
	return 4 * 1024 * 1024; // 4 Mo
}

int driver_exist(const char *filename)
{
    spdlog::debug("exist {}", filename);

    std::string file_uri = filename;
    spdlog::debug("exist file_uri {}", file_uri);
    spdlog::debug("exist last char {}", file_uri.back());

    if (file_uri.back() == '/') {
        return driver_dirExists(filename);
    } else {
        return driver_fileExists(filename);
    }
}

int driver_fileExists(const char *sFilePathName)
{
    spdlog::debug("fileExist {}", sFilePathName);

    std::string bucket_name, object_name;
    ParseS3Uri(sFilePathName, bucket_name, object_name);
    FallbackToDefaultBucket(bucket_name);

    // Configuration de la requête HEAD pour l'objet
    Aws::S3::Model::HeadObjectRequest head_object_request;
    head_object_request.WithBucket(bucket_name).WithKey(object_name);

    // Exécution de la requête HEAD
    auto head_object_outcome = client->HeadObject(head_object_request);

    if (!head_object_outcome.IsSuccess()) {
        spdlog::debug("Failed retrieving file info: {} {}", int(head_object_outcome.GetError().GetErrorType()), head_object_outcome.GetError().GetMessage());
    }

    // Retourne true si l'objet existe, false sinon
    return head_object_outcome.IsSuccess();
}

int driver_dirExists(const char *sFilePathName)
{
    spdlog::debug("dirExist {}", sFilePathName);

    return 1;
}

long long int getFileSize(std::string bucket_name, std::string object_name) {
    // Configuration de la requête HEAD pour l'objet
    Aws::S3::Model::HeadObjectRequest head_object_request;
    head_object_request.WithBucket(bucket_name).WithKey(object_name);

    // Exécution de la requête HEAD
    auto head_object_outcome = client->HeadObject(head_object_request);

    if (!head_object_outcome.IsSuccess()) {
        spdlog::error("Failed retrieving file info: {}", head_object_outcome.GetError().GetMessage());
        return -1;
    }

    // Retourne la taille de l'objet s'il existe
    return head_object_outcome.GetResult().GetContentLength();
}

long long int driver_getFileSize(const char *filename)
{
    spdlog::debug("getFileSize {}", filename);

    std::string bucket_name, object_name;
    ParseS3Uri(filename, bucket_name, object_name);
    FallbackToDefaultBucket(bucket_name);

    return getFileSize(bucket_name, object_name);
}

void *driver_fopen(const char *filename, char mode)
{
    spdlog::debug("fopen {} {}", filename, mode);

    std::string bucket_name, object_name;
    ParseS3Uri(filename, bucket_name, object_name);
    FallbackToDefaultBucket(bucket_name);
	assert(driver_isConnected());

    auto h = new MultiPartFile;
    h->bucketname = bucket_name;
    h->filename = object_name;
    h->offset = 0;

	switch (mode) {
        case 'r':
        {
            if (false) /* isMultifile(object_name)*/ {
                // TODO
                // identify files part of multifile glob pattern
                // initialize cumulative size array
                // determine if header is repeated
            } else {
                h->filenames.push_back(object_name);
                long long int fileSize = getFileSize(bucket_name, object_name);
                h->cumulativeSize.push_back(fileSize);
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

int driver_fclose(void *stream)
{
    spdlog::debug("fclose {}", (void*)stream);

    MultiPartFile *multiFile;

	assert(driver_isConnected());
	assert(stream != NULL);
	multiFile = (MultiPartFile *)stream;
    delete ((MultiPartFile *)stream);
    return 0;
}

long long int totalSize(MultiPartFile *h) {
    return h->cumulativeSize[h->cumulativeSize.size()-1];
}

int driver_fseek(void *stream, long long int offset, int whence)
{
    spdlog::debug("fseek {} {} {}", stream, offset, whence);

    assert(stream != NULL);
	MultiPartFile *h = (MultiPartFile *)stream;

    switch (whence) {
        case std::ios::beg:
            h->offset = offset;
            return 0;
        case std::ios::cur:
        {
            auto computedOffset = h->offset + offset;
            if (computedOffset < 0) {
                spdlog::critical("Invalid seek offset {}", computedOffset);
                return -1;
            }
            h->offset = computedOffset;
            return 0;
        }
        case std::ios::end:
        {
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

const char *driver_getlasterror()
{
    spdlog::debug("getlasterror");

    return NULL;
}

long long int driver_fread(void *ptr, size_t size, size_t count, void *stream)
{
    spdlog::debug("fread {} {} {} {}", ptr, size, count, stream);

    assert(stream != NULL);
	MultiPartFile *h = (MultiPartFile *)stream;

    // TODO: handle multifile case
    auto toRead = size * count;
    spdlog::debug("offset = {} toRead = {}", h->offset, toRead);

    auto num_read = DownloadFileRangeToBuffer(h->bucketname, h->filename, (char*)ptr, toRead, h->offset,
        h->offset + toRead);

    if (num_read != -1)
        h->offset += num_read;
    return num_read;
}

long long int driver_fwrite(const void *ptr, size_t size, size_t count, void *stream)
{
    spdlog::debug("fwrite {} {} {} {}", ptr, size, count, stream);

    assert(stream != NULL);
	MultiPartFile *h = (MultiPartFile *)stream;

    UploadBuffer(h->bucketname, h->filename, (char*)ptr, size*count);
    
    // TODO proper error handling...
    return size*count;
}

int driver_fflush(void *stream)
{
    spdlog::debug("Flushing (does nothing...)");
    return 0;
}

int driver_remove(const char *filename)
{
    spdlog::debug("remove {}", filename);

	assert(driver_isConnected());
    std::string bucket_name, object_name;
    ParseS3Uri(filename, bucket_name, object_name);
    FallbackToDefaultBucket(bucket_name);

    Aws::S3::Model::DeleteObjectRequest request;

    request.WithBucket(bucket_name).WithKey(object_name);

    Aws::S3::Model::DeleteObjectOutcome outcome =
            client->DeleteObject(request);

    if (!outcome.IsSuccess()) {
        auto err = outcome.GetError();
        spdlog::error("DeleteObject: {} {}", err.GetExceptionName(), err.GetMessage());
    }

    return outcome.IsSuccess();
}

int driver_rmdir(const char *filename)
{
    spdlog::debug("rmdir {}", filename);

	assert(driver_isConnected());
    spdlog::debug("Remove dir (does nothing...)");
    return 1;
}

int driver_mkdir(const char *filename)
{
    spdlog::debug("mkdir {}", filename);

	assert(driver_isConnected());
    return 1;
}

long long int driver_diskFreeSpace(const char *filename)
{
    spdlog::debug("diskFreeSpace {}", filename);

	assert(driver_isConnected());
    return (long long int)5 * 1024 * 1024 * 1024 * 1024;
}

int driver_copyToLocal(const char *sSourceFilePathName, const char *sDestFilePathName)
{
    spdlog::debug("copyToLocal {} {}", sSourceFilePathName, sDestFilePathName);

	assert(driver_isConnected());

    std::string bucket_name, object_name;
    ParseS3Uri(sSourceFilePathName, bucket_name, object_name);
    FallbackToDefaultBucket(bucket_name);

    // Configuration de la requête pour obtenir un objet
    Aws::S3::Model::GetObjectRequest object_request;
    object_request.WithBucket(bucket_name).WithKey(object_name);

    // Exécution de la requête
    auto get_object_outcome = client->GetObject(object_request);
    long long int num_read = -1;

    if (!get_object_outcome.IsSuccess()) {
        spdlog::error("Error initializing download stream: {}", get_object_outcome.GetError().GetMessage());
        return false;
    }

    // Open the local file
    std::ofstream file_stream(sDestFilePathName, std::ios::binary);
    if (!file_stream.is_open()) {
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

int driver_copyFromLocal(const char *sSourceFilePathName, const char *sDestFilePathName)
{
    spdlog::debug("copyFromLocal {} {}", sSourceFilePathName, sDestFilePathName);

	assert(driver_isConnected());

    std::string bucket_name, object_name;
    ParseS3Uri(sDestFilePathName, bucket_name, object_name);
    FallbackToDefaultBucket(bucket_name);

    // Configuration de la requête pour envoyer l'objet
    Aws::S3::Model::PutObjectRequest object_request;
    object_request.WithBucket(bucket_name).WithKey(object_name);

    // Chargement du fichier dans un flux d'entrée
    std::shared_ptr<Aws::IOStream> input_data = 
        Aws::MakeShared<Aws::FStream>("PutObjectInputStream",
                                        sSourceFilePathName,
                                        std::ios_base::in | std::ios_base::binary);

    object_request.SetBody(input_data);

    // Exécution de la requête
    auto put_object_outcome = client->PutObject(object_request);

    if (!put_object_outcome.IsSuccess())  {
        spdlog::error("Error during file upload: {}", put_object_outcome.GetError().GetMessage());
        return false;
    }

    return true;
}


