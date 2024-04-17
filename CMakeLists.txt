# CMakeLists.txt

# Ensure to pick up the default triplet from the environment if any. This helps 
# driving the vcpkg triplet in the same way either when starting vcpkg directly,
# or when letting CMake start vcpkg at configure/generate time.
# Note: this logic must happen before PROJECT command.
if(DEFINED ENV{VCPKG_DEFAULT_TRIPLET} AND NOT DEFINED VCPKG_TARGET_TRIPLET)
  set(VCPKG_TARGET_TRIPLET "$ENV{VCPKG_DEFAULT_TRIPLET}" CACHE STRING "The vcpkg triplet")
endif()

cmake_minimum_required(VERSION 3.20)
project(khiops-gcs)

# Find dependencies provided by vcpkg (via vcpkg.cmake)
find_package(google_cloud_cpp_storage CONFIG REQUIRED)
find_package(spdlog REQUIRED)
find_package(Threads)

add_library(khiopsdriver_file_gcs MODULE src/gcsplugin.cpp)
add_executable(KhiopsPluginTest src/main.cpp)

target_link_libraries(khiopsdriver_file_gcs
    PRIVATE google-cloud-cpp::storage spdlog::spdlog Threads::Threads)
