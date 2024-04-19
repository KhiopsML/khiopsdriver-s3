# CMakeLists.txt

# Ensure to pick up the default triplet from the environment if any. This helps 
# driving the vcpkg triplet in the same way either when starting vcpkg directly,
# or when letting CMake start vcpkg at configure/generate time.
# Note: this logic must happen before PROJECT command.
if(DEFINED ENV{VCPKG_DEFAULT_TRIPLET} AND NOT DEFINED VCPKG_TARGET_TRIPLET)
  set(VCPKG_TARGET_TRIPLET "$ENV{VCPKG_DEFAULT_TRIPLET}" CACHE STRING "The vcpkg triplet")
endif()

cmake_minimum_required(VERSION 3.20)
# Enforce c++11 standard.
set (CMAKE_CXX_STANDARD 17)

project(khiops-gcs)

# Find dependencies provided by vcpkg (via vcpkg.cmake)
find_package(google_cloud_cpp_storage CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)

add_library(khiopsdriver_file_gcs MODULE src/gcsplugin.cpp)
add_executable(KhiopsPluginTest src/khiopsplugintest.cpp)

target_link_libraries(khiopsdriver_file_gcs
PRIVATE CURL::libcurl google-cloud-cpp::storage google-cloud-cpp::storage_protos google-cloud-cpp::experimental-storage-grpc spdlog::spdlog_header_only)

# debug lib
add_library(khiopsdriver_file_gcs2 MODULE src/gcsplugin2.cpp)
target_link_libraries(khiopsdriver_file_gcs2
PRIVATE CURL::libcurl google-cloud-cpp::storage google-cloud-cpp::storage_protos google-cloud-cpp::experimental-storage-grpc spdlog::spdlog_header_only)


# Debugging WIN compilation
add_executable(MainTest src/maintest.cpp)

add_executable(Demo src/main.cpp src/version.h)
set(MODULE_FILES
	src/d-hello.cpp
	src/d-hello.h
	src/dlib_export.h
)
add_library(DLib SHARED ${MODULE_FILES})

# Demo application requires library, so add it
target_link_libraries(Demo
	DLib
)

add_executable(GCPTest src/gcptest.cpp)
target_link_libraries(GCPTest
PRIVATE CURL::libcurl google-cloud-cpp::storage google-cloud-cpp::storage_protos google-cloud-cpp::experimental-storage-grpc spdlog::spdlog_header_only)
