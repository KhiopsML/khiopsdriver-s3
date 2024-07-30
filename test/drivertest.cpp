#include <gtest/gtest.h>

#include "s3plugin.h"

#include <sstream>

#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.

/* functions prototype */
int test(const char *file_name_input, const char *file_name_output,
         const char *file_name_local, int nBufferSize);
int launch_test(const char *inputFilename, int nBufferSize);
int copyFile(const char *file_name_input, const char *file_name_output,
             int nBufferSize);
int copyFileWithFseek(const char *file_name_input, const char *file_name_output,
                      int nBufferSize);
int removeFile(const char *filename);
int compareSize(const char *file_name_output, long long int filesize);

constexpr int kSuccess{1};
constexpr int kFailure{0};

TEST(GCSDriverTest, End2EndTest_SingleFile_512KB_OK) {
  const char *inputFilename = "s3://diod-data-di-jupyterhub/khiops_data/"
                              "bq_export/Adult/Adult-split-000000000001.txt";

  /* default size of buffer passed to driver */
  int nBufferSize = 512 * 1024;

  /* error indicator in case of error */
  int test_status = launch_test(inputFilename, nBufferSize);
  ASSERT_EQ(test_status, kSuccess);
}

TEST(GCSDriverTest, End2EndTest_SingleFile_2MB_OK) {
  const char *inputFilename = "s3://diod-data-di-jupyterhub/khiops_data/"
                              "bq_export/Adult/Adult-split-000000000001.txt";

  /* default size of buffer passed to driver */
  int nBufferSize = 2 * 1024 * 1024;

  /* error indicator in case of error */
  int test_status = launch_test(inputFilename, nBufferSize);
  ASSERT_EQ(test_status, kSuccess);
}

TEST(GCSDriverTest, End2EndTest_SingleFile_512B_OK) {
  /* use this particular file because it is short and buffer size triggers lots
   * of read operations */
  const char *inputFilename = "s3://diod-data-di-jupyterhub/khiops_data/"
                              "bq_export/Adult/Adult-split-000000000002.txt";

  /* default size of buffer passed to driver */
  int nBufferSize = 512;

  /* error indicator in case of error */
  int test_status = launch_test(inputFilename, nBufferSize);
  ASSERT_EQ(test_status, kSuccess);
}

#if 0
TEST(GCSDriverTest, End2EndTest_MultipartBQFile_512KB_OK)
{
	const char* inputFilename = "s3://diod-data-di-jupyterhub/khiops_data/bq_export/Adult/Adult-split-00000000000*.txt";

	/* default size of buffer passed to driver */
	int nBufferSize = 512 * 1024;

	/* error indicator in case of error */
	int test_status = launch_test(inputFilename, nBufferSize);
    ASSERT_EQ(test_status, kSuccess);
}

TEST(GCSDriverTest, End2EndTest_MultipartBQEmptyFile_512KB_OK)
{
	const char* inputFilename = "s3://diod-data-di-jupyterhub/khiops_data/bq_export/Adult_empty/Adult-split-00000000000*.txt";

	/* default size of buffer passed to driver */
	int nBufferSize = 512 * 1024;

	/* error indicator in case of error */
	int test_status = launch_test(inputFilename, nBufferSize);
    ASSERT_EQ(test_status, kSuccess);
}

TEST(GCSDriverTest, End2EndTest_MultipartSplitFile_512KB_OK)
{
	const char* inputFilename = "s3://diod-data-di-jupyterhub/khiops_data/split/Adult/Adult-split-0*.txt";

	/* default size of buffer passed to driver */
	int nBufferSize = 512 * 1024;

	/* error indicator in case of error */
	int test_status = launch_test(inputFilename, nBufferSize);
    ASSERT_EQ(test_status, kSuccess);
}

TEST(GCSDriverTest, End2EndTest_MultipartSubsplitFile_512KB_OK)
{
	const char* inputFilename = "s3://diod-data-di-jupyterhub/khiops_data/split/Adult_subsplit/**/Adult-split-0*.txt";

	/* default size of buffer passed to driver */
	int nBufferSize = 512 * 1024;

	/* error indicator in case of error */
	int test_status = launch_test(inputFilename, nBufferSize);
    ASSERT_EQ(test_status, kSuccess);
}
#endif

int launch_test(const char *inputFilename, int nBufferSize) {
  int test_status = kSuccess;

  std::stringstream outputFilename;
  outputFilename << "s3://diod-data-di-jupyterhub/khiops_data/output/"
                 << boost::uuids::random_generator()() << "/output.txt";
  std::stringstream localOutput;
#ifdef _WIN32
  localOutput << std::getenv("TEMP") << "\\out-"
              << boost::uuids::random_generator()() << ".txt";
#else
  localOutput << "/tmp/out-" << boost::uuids::random_generator()() << ".txt";
#endif

  // Connection to the file system
  bool bIsconnected = driver_connect();
  if (bIsconnected) {
    if (!driver_isConnected()) {
      test_status = kFailure;
      fprintf(stderr,
              "ERROR : connection is done but driver is not connected\n");
    }
    if (!driver_fileExists(inputFilename)) {
      fprintf(stderr, "ERROR : %s is missing\n", inputFilename);
      test_status = kFailure;
    }
    // The real test begins here
    if (test_status == kSuccess) {
      test_status = test(inputFilename, outputFilename.str().c_str(),
                         localOutput.str().c_str(), nBufferSize);
    }
    driver_disconnect();
  } else {
    test_status = kFailure;
    fprintf(stderr, "ERROR : unable to connect to the file system\n");
  }

  if (test_status == kFailure) {
    printf("Test has failed\n");
  }

  return test_status;
}

/* functions definitions */

int test(const char *file_name_input, const char *file_name_output,
         const char *file_name_local, int nBufferSize) {
  // Basic information of the scheme
  printf("scheme: %s\n", driver_getScheme());
  printf("is read-only: %d\n", driver_isReadOnly());

  // Checks size of input file
  long long int filesize = driver_getFileSize(file_name_input);
  printf("size of %s is %lld\n", file_name_input, filesize);

  if (driver_fileExists(file_name_input) == 1)
    printf("%s exists\n", file_name_input);
  else {
    printf("%s is missing, abort\n", file_name_input);
    return kFailure;
  }

  int copy_status = kSuccess;

  // Test copying files
  printf("Copy %s to %s\n", file_name_input, file_name_output);
  copy_status = copyFile(file_name_input, file_name_output, nBufferSize);
  if (copy_status == kSuccess) {
    compareSize(file_name_output, filesize);
    // removeFile(file_name_output);
  }

  // Test copying files with fseek
  printf("Copy with fseek %s to %s ...\n", file_name_input, file_name_output);
  copy_status =
      copyFileWithFseek(file_name_input, file_name_output, nBufferSize);
  if (copy_status == kSuccess) {
    compareSize(file_name_output, filesize);
    removeFile(file_name_output);
  }

  // Copy to local
  if (copy_status == kSuccess) {
    printf("Copy to local %s to %s ...\n", file_name_input, file_name_local);
    copy_status = driver_copyToLocal(file_name_input, file_name_local);
    if (copy_status != kSuccess)
      printf("Error while copying : %s\n", driver_getlasterror());
    else
      printf("copy %s to local is done\n", file_name_input);
  }

  // Copy from local
  if (copy_status == kSuccess) {
    printf("Copy from local %s to %s ...\n", file_name_local, file_name_output);
    copy_status = driver_copyFromLocal(file_name_local, file_name_output);
    if (copy_status != kSuccess)
      printf("Error while copying : %s\n", driver_getlasterror());
    else
      printf("copy %s from local is done\n", file_name_local);
    if (!driver_fileExists(file_name_output)) {
      printf("%s is missing !\n", file_name_output);
      copy_status = kFailure;
    }
  }

  return copy_status;
}

// Copy file_name_input to file_name_output by steps of 1Kb
int copyFile(const char *file_name_input, const char *file_name_output,
             int nBufferSize) {
  // Opens for read
  void *fileinput = driver_fopen(file_name_input, 'r');
  if (fileinput == NULL) {
    printf("error : %s : %s\n", file_name_input, driver_getlasterror());
    return kFailure;
  }

  int copy_status = kSuccess;
  void *fileoutput = driver_fopen(file_name_output, 'w');
  if (fileoutput == NULL) {
    printf("error : %s : %s\n", file_name_input, driver_getlasterror());
    copy_status = kFailure;
  }

  if (copy_status == kSuccess) {
    // Reads the file by steps of nBufferSize and writes to the output file at
    // each step
    char *buffer = new char[nBufferSize + 1]();
    long long int sizeRead = nBufferSize;
    long long int sizeWrite;
    driver_fseek(fileinput, 0, SEEK_SET);
    while (sizeRead == nBufferSize && copy_status == kSuccess) {
      sizeRead = driver_fread(buffer, sizeof(char), nBufferSize, fileinput);
      if (sizeRead == -1) {
        copy_status = kFailure;
        printf("error while reading %s : %s\n", file_name_input,
               driver_getlasterror());
      } else {
        sizeWrite =
            driver_fwrite(buffer, sizeof(char), (size_t)sizeRead, fileoutput);
        if (sizeWrite == -1) {
          copy_status = kFailure;
          printf("error while writing %s : %s\n", file_name_output,
                 driver_getlasterror());
        }
      }
    }
    driver_fclose(fileoutput);
    delete[] (buffer);
  }
  driver_fclose(fileinput);
  return copy_status;
}

// Copy file_name_input to file_name_output by steps of 1Kb by using fseek
// before each read
int copyFileWithFseek(const char *file_name_input, const char *file_name_output,
                      int nBufferSize) {
  // Opens for read
  void *fileinput = driver_fopen(file_name_input, 'r');
  if (fileinput == NULL) {
    printf("error : %s : %s\n", file_name_input, driver_getlasterror());
    return kFailure;
  }

  int copy_status = kSuccess;
  void *fileoutput = driver_fopen(file_name_output, 'w');
  if (fileoutput == NULL) {
    printf("error : %s : %s\n", file_name_input, driver_getlasterror());
    copy_status = kFailure;
  }

  if (copy_status == kSuccess) {
    // Reads the file by steps of nBufferSize and writes to the output file at
    // each step
    char *buffer = new char[nBufferSize + 1]();
    long long int sizeRead = nBufferSize;
    long long int sizeWrite;
    int cummulativeRead = 0;
    driver_fseek(fileinput, 0, SEEK_SET);
    while (sizeRead == nBufferSize && copy_status == kSuccess) {
      driver_fseek(fileinput, cummulativeRead, SEEK_SET);
      sizeRead = driver_fread(buffer, sizeof(char), nBufferSize, fileinput);
      cummulativeRead += sizeRead;
      if (sizeRead == -1) {
        copy_status = kFailure;
        printf("error while reading %s : %s\n", file_name_input,
               driver_getlasterror());
      } else {
        sizeWrite =
            driver_fwrite(buffer, sizeof(char), (size_t)sizeRead, fileoutput);
        if (sizeWrite == -1) {
          copy_status = kFailure;
          printf("error while writing %s : %s\n", file_name_output,
                 driver_getlasterror());
        }
      }
    }
    driver_fclose(fileoutput);
    delete[] (buffer);
  }
  driver_fclose(fileinput);
  return copy_status;
}

int removeFile(const char *filename) {
  int remove_status = driver_remove(filename);
  if (remove_status != kSuccess)
    printf("Error while removing : %s\n", driver_getlasterror());
  if (driver_fileExists(filename)) {
    printf("File %s should be removed !\n", filename);
    remove_status = kFailure;
  }
  return remove_status;
}

int compareSize(const char *file_name_output, long long int filesize) {
  int compare_status = kSuccess;
  long long int filesize_output = driver_getFileSize(file_name_output);
  printf("size of %s is %lld\n", file_name_output, filesize_output);
  if (filesize_output != filesize) {
    printf("Sizes of input and output are different\n");
    compare_status = kFailure;
  }
  if (driver_fileExists(file_name_output)) {
    printf("File %s exists\n", file_name_output);
  } else {
    printf("Something's wrong : %s is missing\n", file_name_output);
    compare_status = kFailure;
  }
  return compare_status;
}
