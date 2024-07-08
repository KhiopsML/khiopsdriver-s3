#include <gtest/gtest.h>

// Use mocking examples from https://github.com/aws/aws-sdk-cpp/blob/main/tests/aws-cpp-sdk-s3-unit-tests/S3UnitTests.cpp
#include <gmock/gmock.h>
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/GetObjectResult.h>
#include <iostream>

#include "../src/s3plugin.h"

constexpr int kSuccess{ 1 };
constexpr int kFailure{ 0 };

constexpr int kFalse{ 0 };
constexpr int kTrue{ 1 };

TEST(S3DriverTest, GetDriverName)
{
	ASSERT_STREQ(driver_getDriverName(), "S3 driver");
}

TEST(S3DriverTest, GetVersion)
{
	ASSERT_STREQ(driver_getVersion(), "0.1.0");
}

TEST(S3DriverTest, GetScheme)
{
	ASSERT_STREQ(driver_getScheme(), "s3");
}

TEST(S3DriverTest, IsReadOnly)
{
    ASSERT_EQ(driver_isReadOnly(), kFalse);
}

TEST(S3DriverTest, Connect)
{
    //check connection state before call to connect
    ASSERT_EQ(driver_isConnected(), kFalse);

    //call connect and check connection
    ASSERT_EQ(driver_connect(), kSuccess);
    ASSERT_EQ(driver_isConnected(), kTrue);

    //call disconnect and check connection
    ASSERT_EQ(driver_disconnect(), kSuccess);
    ASSERT_EQ(driver_isConnected(), kFalse);
}

TEST(S3DriverTest, Disconnect)
{
    ASSERT_EQ(driver_connect(), kSuccess);
    ASSERT_EQ(driver_disconnect(), kSuccess);
    ASSERT_EQ(driver_isConnected(), kFalse);
}
/*
TEST(S3DriverTest, GetFileSize)
{
	ASSERT_EQ(driver_connect(), kSuccess);
	ASSERT_EQ(driver_getFileSize("s3://bucket/khiops_data/samples/Adult/Adult.txt"), 5585568);
	ASSERT_EQ(driver_disconnect(), kSuccess);
}

TEST(S3DriverTest, GetMultipartFileSize)
{
	ASSERT_EQ(driver_connect(), kSuccess);
	ASSERT_EQ(driver_getFileSize("s3://bucket/khiops_data/bq_export/Adult/Adult-split-00000000000*.txt"), 5585568);
	ASSERT_EQ(driver_disconnect(), kSuccess);
}
*/
TEST(S3DriverTest, GetSystemPreferredBufferSize)
{
	ASSERT_EQ(driver_getSystemPreferredBufferSize(), 4 * 1024 * 1024);
}

class MockS3Client : public Aws::S3::S3Client {
public:
    MOCK_METHOD1(GetObject, Aws::S3::Model::GetObjectOutcome(const Aws::S3::Model::GetObjectRequest&));
};

TEST(S3DriverTest,  GetObjectTest) {
	Aws::SDKOptions options;
    Aws::InitAPI(options);

    MockS3Client mockClient;
    Aws::S3::Model::GetObjectRequest request;
    Aws::S3::Model::GetObjectResult expected;

	const char* ALLOCATION_TAG = "S3DriverTest";

	std::shared_ptr<Aws::IOStream> body = Aws::MakeShared<Aws::StringStream>(ALLOCATION_TAG);
	*body << "What country shall I say is calling From across the world?";
	body->flush();
	expected.ReplaceBody(body.get());

    // Définir le comportement attendu de la méthode GetObject
    EXPECT_CALL(mockClient, GetObject(testing::_))
        .WillOnce(testing::Return(Aws::S3::Model::GetObjectOutcome(std::move(expected))));

    // Appeler la méthode GetObject et vérifier le résultat
    auto outcome = mockClient.GetObject(request);
    ASSERT_EQ(outcome.IsSuccess(), true);
	Aws::StringStream ss;
	Aws::S3::Model::GetObjectResult result = outcome.GetResultWithOwnership();
	ss << result.GetBody().rdbuf();
    ASSERT_STREQ("What country shall I say is calling From across the world?", ss.str().c_str());
	// Delete shared ptr, otherwise a mismatched free will be called 
	body.reset();


	Aws::ShutdownAPI(options);
}

int main(int argc, char** argv)
{
	::testing::InitGoogleTest(&argc, argv);

	//check that the arguments are effectively passed from ctest
	for (int i = 0; i < argc; i++)
	{
		std::cout << argv[i] << '\n';
	}

	int result = RUN_ALL_TESTS();

	return result;
}
