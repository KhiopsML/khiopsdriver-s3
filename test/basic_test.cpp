#include <gtest/gtest.h>

// Use mocking examples from
// https://github.com/aws/aws-sdk-cpp/blob/main/tests/aws-cpp-sdk-s3-unit-tests/S3UnitTests.cpp
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/GetObjectResult.h>
#include <fstream>
#include <gmock/gmock.h>
#include <iostream>
#include <sstream>

#include <boost/process/environment.hpp>

#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.

#include "../src/s3plugin.h"
#include "contrib/matching.h"

constexpr int kSuccess{1};
constexpr int kFailure{0};

constexpr int kFalse{0};
constexpr int kTrue{1};

void TestPatternMatching(const std::vector<std::string> &must_match,
                         const std::vector<std::string> &no_match,
                         const std::string &pattern) {
  for (auto &s : must_match) {
    ASSERT_TRUE(utils::gitignore_glob_match(s, pattern));
  }

  for (auto &s : no_match) {
    ASSERT_FALSE(utils::gitignore_glob_match(s, pattern));
  }
}

#define DO_PATTERN_MATCHING_TEST                                               \
  TestPatternMatching(must_match, no_match, pattern)

// tests for glob matching utility
TEST(S3DriverMatchingUtilityTest, NoWildCard) {
  const std::string pattern = "s3://this/pattern/to/match/exactly/A000.txt";

  const std::vector<std::string> must_match = {pattern};

  const std::vector<std::string> no_match = {
      "", " ", "off_topic",
      "s3://this/pattern/to/match/exactly/a000.txt", // case sensitivity
      "s3://this/patern/to/match/exactly/A000.txt",  // error on the path
      "s3://this/pattern/to/match/exactly/A000.tx",  // missing character at the
                                                     // end
      "3://this/pattern/to/match/exactly/A000.txt",  // missing character at the
                                                     // beginning
      "d3://this/pattern/to/match/exactly/A000.txt", // wrong character at the
                                                     // begininng
      "s3://this/pattern/to/match/exactly/A000.txx"  // wrong character at the
                                                     // end
  };

  DO_PATTERN_MATCHING_TEST;
}

TEST(S3DriverMatchingUtilityTest, SimpleWildCard) {
  const std::string pattern = "s3://path/to/dir/A00?.txt";

  const std::vector<std::string> must_match = {
      "s3://path/to/dir/A000.txt", "s3://path/to/dir/A00a.txt",
      "s3://path/to/dir/A00-.txt", "s3://path/to/dir/A00?.txt"};

  const std::vector<std::string> no_match = {"",
                                             " ",
                                             "off_topic",
                                             "s3://path/to/dir/A00.txt",
                                             "s3://path/to/dir/A00/a.txt",
                                             "s3://path/to/dir/A0000.txt"};

  DO_PATTERN_MATCHING_TEST;
}

TEST(S3DriverMatchingUtilityTest, MultiCharWildCard) {
  const std::string pattern = "s3://path/to/dir/*.txt";

  const std::vector<std::string> must_match = {"s3://path/to/dir/a.txt",
                                               "s3://path/to/dir/aa.txt",
                                               "s3://path/to/dir/1.txt"
                                               "s3://path/to/dir/00.txt"};

  const std::vector<std::string> no_match = {
      "", " ", "off_topic",
      "s3://path/to/a.txt",      // path does not match
      "s3://path/to/dir/a/a.txt" // path does not match
  };

  DO_PATTERN_MATCHING_TEST;
}

TEST(S3DriverMatchingUtilityTest, NumericRangeWildCard) {
  const std::string pattern = "s3://path/to/dir/[0-9].txt";

  const std::vector<std::string> must_match = {
      "s3://path/to/dir/0.txt",
      "s3://path/to/dir/9.txt",
  };

  const std::vector<std::string> no_match = {
      "", " ", "off_topic",
      "s3://path/to/dir/a.txt", // not in the range
      "s3://path/to/dir/00.txt" // too many characters
  };

  DO_PATTERN_MATCHING_TEST;
}

TEST(S3DriverMatchingUtilityTest, DoubleStarWildCard) {
  const std::string pattern = "s3://path/**/a.txt";

  const std::vector<std::string> must_match = {
      "s3://path/to/dir/a.txt", "s3://path/to/a.txt", "s3://path/to/../a.txt"};

  const std::vector<std::string> no_match = {
      "", " ", "off_topic",
      "s3://to/dir/a.txt",     // prefix before ** does not match
      "s3://path/to/dir/b.txt" // file name after ** does not match
  };

  DO_PATTERN_MATCHING_TEST;
}

TEST(S3DriverTest, GetDriverName) {
  ASSERT_STREQ(driver_getDriverName(), "S3 driver");
}

TEST(S3DriverTest, GetVersion) { ASSERT_STREQ(driver_getVersion(), "0.1.0"); }

TEST(S3DriverTest, GetScheme) { ASSERT_STREQ(driver_getScheme(), "s3"); }

TEST(S3DriverTest, IsReadOnly) { ASSERT_EQ(driver_isReadOnly(), kFalse); }

TEST(S3DriverTest, Connect) {
  // check connection state before call to connect
  ASSERT_EQ(driver_isConnected(), kFalse);

  // call connect and check connection
  ASSERT_EQ(driver_connect(), kSuccess);
  ASSERT_EQ(driver_isConnected(), kTrue);

  // call disconnect and check connection
  ASSERT_EQ(driver_disconnect(), kSuccess);
  ASSERT_EQ(driver_isConnected(), kFalse);
}

TEST(S3DriverTest, Disconnect) {
  ASSERT_EQ(driver_connect(), kSuccess);
  ASSERT_EQ(driver_disconnect(), kSuccess);
  ASSERT_EQ(driver_isConnected(), kFalse);
}

TEST(S3DriverTest, GetFileSize) {
  ASSERT_EQ(driver_connect(), kSuccess);
  ASSERT_EQ(
      driver_getFileSize(
          "s3://diod-data-di-jupyterhub/khiops_data/samples/Adult/Adult.txt"),
      5585568);
  ASSERT_EQ(driver_disconnect(), kSuccess);
}
/*
// TODO
TEST(S3DriverTest, GetMultipartFileSize)
{
        ASSERT_EQ(driver_connect(), kSuccess);
        ASSERT_EQ(driver_getFileSize("s3://diod-data-di-jupyterhub/bq_export/Adult/Adult-split-00000000000*.txt"),
5585568); ASSERT_EQ(driver_disconnect(), kSuccess);
}

TEST(S3DriverTest, GetFileSizeNonexistentFailure)
{
        ASSERT_EQ(driver_connect(), kSuccess);
        ASSERT_EQ(driver_getFileSize("s3://diod-data-di-jupyterhub/khiops_data/samples/non_existent_file.txt"),
-1); ASSERT_STRNE(driver_getlasterror(), NULL); ASSERT_EQ(driver_disconnect(),
kSuccess);
}
*/

TEST(S3DriverTest, FileExists) {
  ASSERT_EQ(driver_connect(), kSuccess);
  ASSERT_EQ(
      driver_exist(
          "s3://diod-data-di-jupyterhub/khiops_data/samples/Adult/Adult.txt"),
      kSuccess);
  ASSERT_EQ(driver_disconnect(), kSuccess);
}

TEST(S3DriverTest, DirExists) {
  ASSERT_EQ(driver_connect(), kSuccess);
  ASSERT_EQ(
      driver_exist("s3://diod-data-di-jupyterhub/khiops_data/samples/Adult/"),
      kSuccess);
  ASSERT_EQ(driver_disconnect(), kSuccess);
}

#ifndef _WIN32
// Setting of environment variables does not work on Windows
TEST(S3DriverTest, DriverConnectMissingCredentialsFailure) {
  auto env = boost::this_process::environment();
  env["AWS_CONFIG_FILE"] = "/tmp/noconfig";
  ASSERT_EQ(driver_connect(), kFailure);
  env.erase("AWS_CONFIG_FILE");
}

void setup_bad_credentials() {
  std::stringstream tempCredsFile;
  tempCredsFile << "/tmp/config-" << boost::uuids::random_generator()();
  std::ofstream outfile(tempCredsFile.str());
  outfile << "{}" << std::endl;
  outfile.close();
  auto env = boost::this_process::environment();
  env["AWS_CONFIG_FILE"] = tempCredsFile.str();
}

void cleanup_bad_credentials() {
  auto env = boost::this_process::environment();
  env.erase("AWS_CONFIG_FILE");
}
// TODO
/*
TEST(S3DriverTest, GetFileSizeInvalidCredentialsFailure)
{
    setup_bad_credentials();
        ASSERT_EQ(driver_connect(), kSuccess);
        ASSERT_EQ(driver_getFileSize("s3://diod-data-di-jupyterhub/khiops_data/samples/Adult/Adult.txt"),
-1); ASSERT_STRNE(driver_getlasterror(), NULL); ASSERT_EQ(driver_disconnect(),
kSuccess); cleanup_bad_credentials();
}
*/
#endif

TEST(S3DriverTest, RmDir) {
  ASSERT_EQ(driver_connect(), kSuccess);
  ASSERT_EQ(driver_rmdir("dummy"), kSuccess);
  ASSERT_EQ(driver_disconnect(), kSuccess);
}

TEST(S3DriverTest, mkDir) {
  ASSERT_EQ(driver_connect(), kSuccess);
  ASSERT_EQ(driver_mkdir("dummy"), kSuccess);
  ASSERT_EQ(driver_disconnect(), kSuccess);
}

TEST(S3DriverTest, GetSystemPreferredBufferSize) {
  ASSERT_EQ(driver_getSystemPreferredBufferSize(), 4 * 1024 * 1024);
}

class MockS3Client : public Aws::S3::S3Client {
public:
  MOCK_METHOD1(GetObject, Aws::S3::Model::GetObjectOutcome(
                              const Aws::S3::Model::GetObjectRequest &));
};

TEST(S3DriverTest, GetObjectTest) {
  // Setup AWS API
  Aws::SDKOptions options;
  Aws::InitAPI(options);

  MockS3Client mockClient;
  Aws::S3::Model::GetObjectRequest request;
  Aws::S3::Model::GetObjectResult expected;

  const char *ALLOCATION_TAG = "S3DriverTest";

  std::shared_ptr<Aws::IOStream> body =
      Aws::MakeShared<Aws::StringStream>(ALLOCATION_TAG);
  *body << "What country shall I say is calling From across the world?";
  body->flush();
  expected.ReplaceBody(body.get());

  // Définir le comportement attendu de la méthode GetObject
  EXPECT_CALL(mockClient, GetObject(testing::_))
      .WillOnce(testing::Return(
          Aws::S3::Model::GetObjectOutcome(std::move(expected))));

  // Appeler la méthode GetObject et vérifier le résultat
  auto outcome = mockClient.GetObject(request);
  ASSERT_EQ(outcome.IsSuccess(), true);
  Aws::StringStream ss;
  Aws::S3::Model::GetObjectResult result = outcome.GetResultWithOwnership();
  ss << result.GetBody().rdbuf();
  ASSERT_STREQ("What country shall I say is calling From across the world?",
               ss.str().c_str());
  // Delete shared ptr, otherwise a mismatched free will be called
  body.reset();

  // Cleanup AWS API
  Aws::ShutdownAPI(options);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  // check that the arguments are effectively passed from ctest
  for (int i = 0; i < argc; i++) {
    std::cout << argv[i] << '\n';
  }

  int result = RUN_ALL_TESTS();

  return result;
}
