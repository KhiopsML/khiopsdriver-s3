#include "s3plugin.h"
#include "s3plugin_internal.h"
#include "contrib/matching.h"

// Use mocking examples from
// https://github.com/aws/aws-sdk-cpp/blob/main/tests/aws-cpp-sdk-s3-unit-tests/S3UnitTests.cpp
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/GetObjectResult.h>

#include <boost/process/environment.hpp>

#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <fstream>
#include <iostream>
#include <sstream>

using namespace s3plugin;

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

  const std::vector<std::string> must_match = {
      "s3://path/to/dir/a.txt", "s3://path/to/dir/aa.txt",
      "s3://path/to/dir/1.txt", "s3://path/to/dir/00.txt"};

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

using namespace Aws::S3;
using namespace Aws::S3::Model;

using ::testing::Return;

class MockS3Client : public S3Client {
public:
  MOCK_METHOD1(GetObject, GetObjectOutcome(const GetObjectRequest &));

  MOCK_METHOD(HeadObjectOutcome, HeadObject, (const HeadObjectRequest &request),
              (const));

  MOCK_METHOD(ListObjectsV2Outcome, ListObjectsV2,
              (const ListObjectsV2Request &request), (const));
};

HeadObjectOutcome MakeHeadObjectOutcome(bool failure = false) {
  if (failure) {
    return S3Error{};
  }
  return HeadObjectResult{};
}

ListObjectsV2Outcome MakeListObjectOutcomeFailure() { return S3Error{}; }

ListObjectsV2Outcome MakeListObjectOutcome(Aws::Vector<Object> &&v,
                                           std::string &&token) {
  ListObjectsV2Result res;
  res.SetContents(std::move(v));
  res.SetContinuationToken(std::move(token));
  return res;
}

// Note: the strings inside keys will be moved from
Aws::Vector<Object> MakeObjectVector(std::vector<std::string> &&keys) {
  const size_t key_count = keys.size();
  Aws::Vector<Object> res(key_count);
  for (size_t i = 0; i < key_count; i++) {
    res[i].SetKey(std::move(keys[i]));
  }
  return res;
}

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

class S3DriverTestFixture : public ::testing::Test {
protected:
  void SetUp() override {
    // Setup AWS API
    Aws::InitAPI(options_);

    mock_client_alias_ = new MockS3Client{};
    mock_client_ = dynamic_cast<MockS3Client *>(mock_client_alias_);
    test_setClient(mock_client_alias_);
  }

  void TearDown() override {
    test_cleanupClient();

    // Cleanup AWS API
    Aws::ShutdownAPI(options_);
  }

public:
  // Note: a bit of "rodeo" here: the driver state contains a
  // unique_ptr<S3Client> that will be set by the test SetUp and unset by the
  // test TearDown. We can't just pass a pointer owned by a shared_ptr, there
  // would be conflicts when freeing the associated memory. So, for each test,
  // the fixture passes a raw pointer to the driver state for it to own. All is
  // well as long as gtest runs the tests sequentially, otherwise the whole
  // thing blows up.
  S3Client *mock_client_alias_ = nullptr;
  MockS3Client *mock_client_ = nullptr; // avoid repeated casts by casting once
                                        // in setup. not owning, do not free!

  Aws::SDKOptions options_;

  template <typename Func, typename ReturnType>
  void CheckInvalidURIs(Func f, ReturnType expect) {
    // null pointer
    ASSERT_EQ(f(nullptr), expect);

    // name without "gs://" prefix
    ASSERT_EQ(f("noprefix"), expect);

    // name with correct prefix, but no clear bucket and object names
    ASSERT_EQ(f("s3://not_valid"), expect);

    // name with only bucket name
    ASSERT_EQ(f("s3://only_bucket_name/"), expect);

    // valid URI, but only object name and assuming global bucket name is not
    // set
    ASSERT_EQ(f("s3:///no_bucket"), expect);
  }

  // template<typename Func, typename Arg, typename ReturnType>
  //   void CheckInvalidURIs(Func f, Arg arg, ReturnType expect)
  //   {
  //       // null pointer
  //       ASSERT_EQ(f(nullptr, arg), expect);

  //       // name without "gs://" prefix
  //       ASSERT_EQ(f("noprefix", arg), expect);

  //       // name with correct prefix, but no clear bucket and object names
  //       ASSERT_EQ(f("s3://not_valid", arg), expect);

  //       // name with only bucket name
  //       ASSERT_EQ(f("s3://only_bucket_name/", arg), expect);

  //       // valid URI, but only object name and assuming global bucket name is
  //       not set ASSERT_EQ(f("s3://no_bucket", arg), expect);
  //   }

  // HandleContainer* GetHandles() { return
  // reinterpret_cast<HandleContainer*>(test_getActiveHandles()); }

  void SetListCall(Aws::Vector<Object> &&content, std::string &&token) {
    EXPECT_CALL(*mock_client_, ListObjectsV2)
        .WillOnce(Return(
            MakeListObjectOutcome(std::move(content), std::move(token))));
  }
};

#define LIST_CALL_ACTION(content, token)                                       \
  .WillOnce(                                                                   \
      Return(MakeListObjectOutcome(std::move((content)), std::move((token)))))

TEST_F(S3DriverTestFixture, FileExists_InvalidURIs) {
  CheckInvalidURIs(driver_fileExists, kFalse);
}

TEST_F(S3DriverTestFixture, FileExists_NoGlobbing) {
  EXPECT_CALL(*mock_client_, HeadObject)
      .WillOnce(Return(MakeHeadObjectOutcome())) // file exists
      .WillOnce(
          Return(MakeHeadObjectOutcome(true))); // no file found or server error

  ASSERT_EQ(driver_fileExists("s3://mock_bucket/mock_name"), kTrue);
  ASSERT_EQ(driver_fileExists("s3://mock_bucket/no_match_or_error"), kFalse);
}

TEST_F(S3DriverTestFixture, FileExists_Globbing_ListObjectError) {
  EXPECT_CALL(*mock_client_, ListObjectsV2)
      .WillOnce(Return(MakeListObjectOutcomeFailure()));

  ASSERT_EQ(driver_fileExists("s3://mock_bucket/**/pattern"), kFalse);
}

TEST_F(S3DriverTestFixture, FileExists_Globbing_EmptyList) {
  Aws::Vector<Object> content;
  std::string token;

  EXPECT_CALL(*mock_client_, ListObjectsV2)
  LIST_CALL_ACTION(content, token);

  ASSERT_EQ(driver_fileExists("s3://mock_bucket/**/pattern"), kFalse);
}

TEST_F(S3DriverTestFixture, FileExists_Globbing_SomeContent_NoMatch) {
  auto content = MakeObjectVector({"nomatch0", "nomatch1"});
  std::string token;

  EXPECT_CALL(*mock_client_, ListObjectsV2)
  LIST_CALL_ACTION(content, token);

  ASSERT_EQ(driver_fileExists("s3://mock_bucket/**/pattern"), kFalse);
}

TEST_F(S3DriverTestFixture, FileExists_Globbing_SomeContent_Match) {
  auto content = MakeObjectVector({"s3://mock_bucket/i_match/pattern",
                                   "s3://mock_bucket/i_match_too/pattern"});
  std::string token;

  EXPECT_CALL(*mock_client_, ListObjectsV2)
  LIST_CALL_ACTION(content, token);

  ASSERT_EQ(driver_fileExists("s3://mock_bucket/**/pattern"), kTrue);
}

TEST_F(S3DriverTestFixture, FileExists_Globbing_ContinuationToken) {
  auto content0 = MakeObjectVector({"a"});
  auto content1 = MakeObjectVector({"b"});

  std::string not_empty = "not_empty_token";
  std::string empty;

  EXPECT_CALL(*mock_client_, ListObjectsV2)
  LIST_CALL_ACTION(content0, not_empty)
  LIST_CALL_ACTION(content1, empty);

  ASSERT_EQ(driver_fileExists("s3://mock_bucket/**/pattern"), kFalse);
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
