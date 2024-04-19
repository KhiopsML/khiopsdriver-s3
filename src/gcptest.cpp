#include "google/cloud/storage/client.h"
#include <iostream>

bool ParseGcsUri(const std::string& gcs_uri, std::string& bucket_name, std::string& object_name) {
    const std::string prefix = "gs://";
    if (gcs_uri.compare(0, prefix.size(), prefix) != 0) {
        std::cerr << "Invalid GCS URI: " << gcs_uri << "\n";
        return false;
    }

    std::size_t pos = gcs_uri.find('/', prefix.size());
    if (pos == std::string::npos) {
        std::cerr << "Invalid GCS URI, missing object name: " << gcs_uri << "\n";
        return false;
    }

    bucket_name = gcs_uri.substr(prefix.size(), pos - prefix.size());
    object_name = gcs_uri.substr(pos + 1);

    return true;
}


int main(int argc, char* argv[]) {
	if (argc != 2) {
		std::cerr << "Missing URI.\n";
		std::cerr << "Usage: GCPTest URI\n";
		return 1;
	}
	std::string const uri = argv[1];
	std::string bucket_name, object_name;
	ParseGcsUri(uri, bucket_name, object_name);

	// Create aliases to make the code easier to read.
	namespace gcs = ::google::cloud::storage;

	// Create a client to communicate with Google Cloud Storage. This client
	// uses the default configuration for authentication and project id.
	auto client = gcs::Client();

	auto object_metadata = client.GetObjectMetadata(bucket_name, object_name);
	if (object_metadata) {
		std::cout << "Object size: " << object_metadata->size() << "\n";
	} else if (object_metadata.status().code() == google::cloud::StatusCode::kNotFound) {
		std::cout << "Object not found!\n";

	} else {
		std::cerr << "Error checking object: " << object_metadata.status().message() << "\n";
	}

	return 0;
}
