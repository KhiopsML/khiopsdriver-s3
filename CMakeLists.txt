# CMakeLists.txt

# Ensure to pick up the default triplet from the environment if any. This helps 
# driving the vcpkg triplet in the same way either when starting vcpkg directly,
# or when letting CMake start vcpkg at configure/generate time.
# Note: this logic must happen before PROJECT command.
if(DEFINED ENV{VCPKG_DEFAULT_TRIPLET} AND NOT DEFINED VCPKG_TARGET_TRIPLET)
  set(VCPKG_TARGET_TRIPLET "$ENV{VCPKG_DEFAULT_TRIPLET}" CACHE STRING "The vcpkg triplet")
endif()

cmake_minimum_required(VERSION 3.20)
# Enforce c++17 standard.
set (CMAKE_CXX_STANDARD 17)

project(khiops-gcs LANGUAGES CXX)

# Find dependencies provided by vcpkg (via vcpkg.cmake)
find_package(fmt CONFIG REQUIRED)
find_package(ZLIB REQUIRED)
find_package(AWSSDK CONFIG COMPONENTS core s3 REQUIRED)
find_package(spdlog CONFIG REQUIRED)

add_library(khiopsdriver_file_s3 MODULE src/s3plugin.h src/s3plugin.cpp)
set_target_properties(khiopsdriver_file_s3 PROPERTIES LINK_FLAGS_RELEASE -s)
target_include_directories(khiopsdriver_file_s3 PRIVATE ${AWSSDK_INCLUDE_DIRS})
target_link_libraries(khiopsdriver_file_s3 PRIVATE ${AWSSDK_LIBRARIES} z spdlog::spdlog)




add_executable(KhiopsPluginTest src/khiopsplugintest.cpp)
target_link_libraries(KhiopsPluginTest PRIVATE fmt::fmt)

add_executable(drivertest src/drivertest.cpp)
target_link_libraries(drivertest)

