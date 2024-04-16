
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef __unix__
#include <unistd.h>
#include <dlfcn.h>
#endif

#ifndef __unix__
#include <windows.h>
#include "errhandlingapi.h"
#endif

/* API functions definition, that must be defined in the library */
const char* (*ptr_driver_getDriverName)();
const char* (*ptr_driver_getVersion)();
const char* (*ptr_driver_getScheme)();
int (*ptr_driver_isReadOnly)();
int (*ptr_driver_connect)();
int (*ptr_driver_disconnect)();
int (*ptr_driver_isConnected)();
int (*ptr_driver_exist)(const char* filename);
long long int (*ptr_driver_getFileSize)(const char* filename);
void* (*ptr_driver_fopen)(const char* filename, const char mode);
int (*ptr_driver_fclose)(void* stream);
long long int (*ptr_driver_fread)(void* ptr, size_t size, size_t count, void* stream);
int (*ptr_driver_fseek)(void* stream, long long int offset, int whence);
const char* (*ptr_driver_getlasterror)();

/* API functions definition, that must be defined in the library only if the driver is not read-only */
long long int (*ptr_driver_fwrite)(const void* ptr, size_t size, size_t count, void* stream);
int (*ptr_driver_fflush)(void* stream);
int (*ptr_driver_remove)(const char* filename);
int (*ptr_driver_mkdir)(const char* filename);
int (*ptr_driver_rmdir)(const char* filename);
long long int (*ptr_driver_diskFreeSpace)(const char* filename);

/* API functions definition, that can be defined optionally in the library only if the driver is not read-only */
int (*ptr_driver_copyToLocal)(const char* sourcefilename, const char* destfilename);
int (*ptr_driver_copyFromLocal)(const char* sourcefilename, const char* destfilename);

/* functions prototype */
void usage();
void* load_shared_library(const char* library_name);
int free_shared_library(void* library_handle);
void* get_shared_library_function(void* library_handle, const char* function_name, int mandatory);
void do_test(const char* file_name_input, const char* file_name_output, const char* file_name_local);

/* error indicator in case of error */
int global_error = 0;

int main(int argc, char* argv[])
{
	void* library_handle;
	if (argc != 5)
		usage();

	// Try to load the shared library
	library_handle = load_shared_library(argv[1]);
	if (!library_handle)
	{
		fprintf(stderr, "Error while loading library %s", argv[1]);
#ifdef __unix__
		fprintf(stderr, " (%s). ", dlerror());
#else
		fwprintf(stderr, L" (0x%x). ", GetLastError());
#endif
		fprintf(stderr, "Check LD_LIBRARY_PATH or set the library with its full path\n");
		exit(EXIT_FAILURE);
	}

	// Bind each mandatory function of the library (and set global_error to 1 in case of errors)
	*(void**)(&ptr_driver_getDriverName) = get_shared_library_function(library_handle, "driver_getDriverName", 1);
	*(void**)(&ptr_driver_getVersion) = get_shared_library_function(library_handle, "driver_getVersion", 1);
	*(void**)(&ptr_driver_getScheme) = get_shared_library_function(library_handle, "driver_getScheme", 1);
	*(void**)(&ptr_driver_isReadOnly) = get_shared_library_function(library_handle, "driver_isReadOnly", 1);
	*(void**)(&ptr_driver_connect) = get_shared_library_function(library_handle, "driver_connect", 1);
	*(void**)(&ptr_driver_disconnect) = get_shared_library_function(library_handle, "driver_disconnect", 1);
	*(void**)(&ptr_driver_isConnected) = get_shared_library_function(library_handle, "driver_isConnected", 1);
	*(void**)(&ptr_driver_exist) = get_shared_library_function(library_handle, "driver_exist", 1);
	*(void**)(&ptr_driver_getFileSize) = get_shared_library_function(library_handle, "driver_getFileSize", 1);
	*(void**)(&ptr_driver_fopen) = get_shared_library_function(library_handle, "driver_fopen", 1);
	*(void**)(&ptr_driver_fclose) = get_shared_library_function(library_handle, "driver_fclose", 1);
	*(void**)(&ptr_driver_fseek) = get_shared_library_function(library_handle, "driver_fseek", 1);
	*(void**)(&ptr_driver_fread) = get_shared_library_function(library_handle, "driver_fread", 1);
	*(void**)(&ptr_driver_getlasterror) = get_shared_library_function(library_handle, "driver_getlasterror", 1);

	// Bind each read-write function of the library if necessary
	if (!global_error && !ptr_driver_isReadOnly())
	{
		*(void**)(&ptr_driver_fwrite) = get_shared_library_function(library_handle, "driver_fwrite", 1);
		*(void**)(&ptr_driver_fflush) = get_shared_library_function(library_handle, "driver_fflush", 1);
		*(void**)(&ptr_driver_remove) = get_shared_library_function(library_handle, "driver_remove", 1);
		*(void**)(&ptr_driver_mkdir) = get_shared_library_function(library_handle, "driver_mkdir", 1);
		*(void**)(&ptr_driver_rmdir) = get_shared_library_function(library_handle, "driver_rmdir", 1);
		*(void**)(&ptr_driver_diskFreeSpace) = get_shared_library_function(library_handle, "driver_diskFreeSpace", 1);

		// Bin optional functions (without setting global_error to 1 in case of errors)
		*(void**)(&ptr_driver_copyToLocal) = get_shared_library_function(library_handle, "driver_copyToLocal", 0);
		*(void**)(&ptr_driver_copyFromLocal) = get_shared_library_function(library_handle, "driver_copyFromLocal", 0);
	}

	if (!global_error)
	{
		// Call 2 simple functions
		printf("Test of the driver library '%s' version %s handling scheme %s\n", ptr_driver_getDriverName(), ptr_driver_getVersion(), ptr_driver_getScheme());

		// Connection to the file system
		bool bIsconnected = ptr_driver_connect();
		if (bIsconnected)
		{
			if (!ptr_driver_isConnected())
			{
				global_error = 1;
				fprintf(stderr, "ERROR : connection is done but driver is not connected\n");
			}
			if (!ptr_driver_exist(argv[2]))
			{
				fprintf(stderr, "ERROR : %s is missing\n", argv[2]);
				global_error = 1;
			}
			// The real test begins here (global_error is set to 1 in case of errors)
			if (!global_error)
				printf("Test begins\n");
				do_test(argv[2], argv[3], argv[4]);
			ptr_driver_disconnect();
		}
		else
		{
			global_error = 1;
			fprintf(stderr, "ERROR : unable to connect to the file system\n");
		}
	}

	free_shared_library(library_handle);
	if (global_error)
	{
		printf("Test has failed\n");
		exit(EXIT_FAILURE);
	}
	printf("! Test is successful !\n");
	exit(EXIT_SUCCESS);
}

/* functions definitions */

void usage()
{
	fprintf(stderr, "Usage : drivertest libraryname input_filename output_filename local_filename\n");
	fprintf(stderr, "example : drivertest ./libkhiopsdriver_file_gcs.so gs:///input_filename gs:///output_filename /tmp/copy.txt\n");
	exit(EXIT_FAILURE);
}

void* load_shared_library(const char* library_name)
{
#if defined(_MSC_VER)
	wchar_t* wString = new wchar_t[4096];
	MultiByteToWideChar(CP_ACP, 0, library_name, -1, wString, 4096);
	void* handle = (void*)LoadLibrary(wString);
	delete[] wString;
	return handle;
#elif defined(_WIN32)
	void* handle = (void*)LoadLibrary(library_name);
	return handle;
#elif defined(__unix__)
	return dlopen(library_name, RTLD_NOW);
#endif
}

int free_shared_library(void* library_handle)
{
#if defined(_MSC_VER) | defined(_WIN32)
	return FreeLibrary((HINSTANCE)library_handle);
#elif defined(__unix__)
	return dlclose(library_handle);
#endif
}

void* get_shared_library_function(void* library_handle, const char* function_name, int mandatory)
{
	void* ptr;
#if defined(_MSC_VER) | defined(_WIN32)
	ptr = (void*)GetProcAddress((HINSTANCE)library_handle, function_name);
#elif defined(__unix__)
	ptr = dlsym(library_handle, function_name);
#endif
	if (ptr == NULL && mandatory)
	{
		global_error = 1;
#ifdef __unix__
		fprintf(stderr, "Unable to load %s (%s)\n", function_name, dlerror());
#else
		fprintf(stderr, "Unable to load %s", function_name);
		fwprintf(stderr, L"(0x%x)\n", GetLastError());
#endif
	}
	return ptr;
}

void test_read(const char* file_name_input, void* file, long long int maxBytes=3000)
{
	// Reads the file by steps of 1Kb and writes to the output file at each step
	const int nBufferSize = 1024;
	//const int nBufferSize = 5;
	char buffer[nBufferSize + 1];
	long long int sizeRead = nBufferSize;
	const long long int maxTotalRead = 3000;
	long long int totalRead = 0;
	while (sizeRead == nBufferSize && !global_error)
	{
		int toRead = maxBytes - totalRead;
		if (toRead > nBufferSize) toRead = nBufferSize;
		sizeRead = ptr_driver_fread(&buffer, sizeof(char), toRead, file);
		printf("Read = %lld, total = %lld, max = %lld\n", sizeRead, totalRead, maxBytes);

		// Message d'erreur si necessaire
		if (sizeRead == 0)
		{
			global_error = 1;
			printf("error while reading %s : %s\n", file_name_input, ptr_driver_getlasterror());
		}
		// Affichage du contenu dans la console sinon
		else
		{
			// Debug
			/*for (int i=0; i<10 && i<sizeRead; i++) {
				printf("%d ", buffer[i]);
			}
			printf("\n");*/
			buffer[sizeRead] = '\0';
			printf("Buffer = %s", buffer);
			totalRead += sizeRead;

			// Arret quand on a atteint la limite
			if (totalRead > maxTotalRead)
			{
				printf("\n");
				break;
			}
		}
	}
}

void do_test(const char* file_name_input, const char* file_name_output, const char* file_name_local)
{
	printf("Starting test\n");
	fflush(NULL); 
	// Basic information of the scheme
	printf("scheme: %s\n", ptr_driver_getScheme());
	fflush(NULL); 
	printf("is read-only: %d\n", ptr_driver_isReadOnly());
	fflush(NULL); 

/*
	// Check existence of directory
	int res = ptr_driver_exist("tmp/");
	if (res == 0) {
		printf("directory does not exit\n");
	} else {
		printf("directory exits\n");
	}
*/

	// Checks size of input file
	long long int filesize = ptr_driver_getFileSize(file_name_input);
	printf("size of %s is %lld\n", file_name_input, filesize);

	// Opens for read
	void* file = ptr_driver_fopen(file_name_input, 'r');
	if (file == NULL)
	{
		printf("error : %s : %s\n", file_name_input, ptr_driver_getlasterror());
		global_error = 1;
		return;
	}

	// Show small extract if the driver is not read-only
	printf("Test read from start\n\n");
	test_read(file_name_input, file, filesize);
	
	printf("Test read from offset 3\n\n");
	ptr_driver_fseek(file, 3, SEEK_SET);
	test_read(file_name_input, file, filesize-3);
	
	printf("Test read from end -20\n\n");
	ptr_driver_fseek(file, -20, SEEK_END);
	test_read(file_name_input, file, 20);
	
	printf("Test read from current offset -40\n\n");
	ptr_driver_fseek(file, -40, SEEK_CUR);
	test_read(file_name_input, file, 40);
	//ptr_driver_fclose(file);
	
	ptr_driver_fseek(file, 0, SEEK_SET);

	// Opens for write if the driver is not read-only
	if (!ptr_driver_isReadOnly())
	{
		printf("Writing test\n\n");

		void* fileoutput = ptr_driver_fopen(file_name_output, 'w');
		if (fileoutput == NULL)
		{
			printf("error : %s : %s\n", file_name_output, ptr_driver_getlasterror());
			global_error = 1;
		}
		if (!global_error)
		{
			// Reads the file by steps of ~1Mb and writes to the output file at each step
			//const int nBufferSize = 1000*1000;
			//const int nBufferSize = 1024*1024;
			const int nBufferSize = 1234*5678;
			//const int nBufferSize = 128;
			char buffer[nBufferSize];
			long long int sizeRead = nBufferSize;
			long long int sizeWrite;
			long long int totalRead = 0;
			while (sizeRead == nBufferSize && !global_error)
			{
				int toRead = filesize - totalRead;
				if (toRead > nBufferSize) toRead = nBufferSize;
				sizeRead = ptr_driver_fread(buffer, sizeof(char), toRead, file);
				printf("Read = %lld, total = %lld, max = %lld\n", sizeRead, totalRead, filesize);
				if (sizeRead == -1)
				{
					global_error = 1;
					printf("error while reading %s : %s\n", file_name_input, ptr_driver_getlasterror());
				}
				else
				{
					sizeWrite = ptr_driver_fwrite(buffer, sizeof(char), (size_t)sizeRead, fileoutput);
					printf("sizeWrite = %lld\n", sizeWrite);
					if (sizeWrite == -1)
					{
						global_error = 1;
						printf("error while writing %s : %s\n", file_name_output, ptr_driver_getlasterror());
					}
					totalRead += sizeRead;
				}
			}
		}
		ptr_driver_fclose(fileoutput);
		ptr_driver_fclose(file);

		// Checks that the two sizes are the same
		if (!global_error)
		{
			long long int filesize_output = ptr_driver_getFileSize(file_name_output);
			printf("size of %s is %lld\n", file_name_output, filesize_output);
			if (filesize_output != filesize)
			{
				printf("Sizes of input and output are different\n");
				global_error = 1;
			}
			if (ptr_driver_exist(file_name_output))
			{
				printf("%s exists\n", file_name_output);
			}
			else
			{
				printf("something's wrong : %s is missing\n", file_name_output);
				global_error = 1;
			}
		}

		// Copy to local if this optional function is available in the librairy
		if (!global_error && ptr_driver_copyToLocal != NULL)
		{
			printf("\n\ncopyToLocal %s to %s\n", file_name_input, file_name_local);
			global_error = ptr_driver_copyToLocal(file_name_input, file_name_local) == 0;
			if (global_error)
				printf("Error while copying : %s\n", ptr_driver_getlasterror());
			else
				printf("copy %s to local is done\n", file_name_input);
		}

		// Delete file
		if (!global_error)
		{
			global_error = ptr_driver_remove(file_name_output) == 0;
			if (global_error)
				printf("Error while removing : %s\n", ptr_driver_getlasterror());
			if (ptr_driver_exist(file_name_output))
			{
				printf("%s should be removed !\n", file_name_output);
				global_error = 1;
			}
		}

		// Copy from local if this optional function is available in the librairy
		if (!global_error && ptr_driver_copyFromLocal != NULL)
		{
			printf("\n\ncopyFromLocal %s to %s\n", file_name_local, file_name_output);
			global_error = ptr_driver_copyFromLocal(file_name_local, file_name_output) == 0;
			if (global_error)
				printf("Error while copying : %s\n", ptr_driver_getlasterror());
			else
				printf("copy %s from local is done\n", file_name_local);
			if (!ptr_driver_exist(file_name_output))
			{
				printf("%s is missing !\n", file_name_output);
				global_error = 1;
			}
		}
	}
}
