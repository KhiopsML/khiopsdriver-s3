# CMakeLists.txt
cmake_minimum_required(VERSION 3.20)
project(khiops-gcs)

# Find dependencies provided by vcpkg (via vcpkg.cmake)
find_package(google_cloud_cpp_storage CONFIG REQUIRED)
find_package(spdlog REQUIRED)


add_library(khiopsdriver_file_gcs SHARED src/gcsplugin.cpp)
add_executable(KhiopsPluginTest src/main.cpp)

target_link_libraries(khiopsdriver_file_gcs PRIVATE google-cloud-cpp::storage PRIVATE spdlog::spdlog)
