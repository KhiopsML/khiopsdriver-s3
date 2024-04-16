# Go cmake configuration
cmake_minimum_required(VERSION 3.20)

#set(CMAKE_C_COMPILER clang)
#set(CMAKE_CXX_COMPILER clang++)
if (VCPKG_TARGET_WINDOWS)
   message(STATUS "Cross-compiling for Windows")
   #set(CMAKE_C_COMPILER_TARGET x86_64-pc-win32)
   #set(CMAKE_CXX_COMPILER_TARGET x86_64-pc-win32)
   #set(VCPKG_TARGET_TRIPLET x64-windows)
   #set(VCPKG_TARGET_TRIPLET x64-mingw-dynamic)

   #IMPORTANT: THIS ASSUMES THAT  -DCMAKE_C_COMPILER= -DCMAKE_CXX_COMPILER=

   #set( COMPILER_BIN /home/nmms4680/workspace/llvm-mingw-20240404-ucrt-ubuntu-20.04-x86_64/bin )
   #set( CMAKE_C_COMPILER ${COMPILER_BIN}/x86_64-w64-mingw32-gcc CACHE PATH "clang" FORCE )
   #set( CMAKE_CXX_COMPILER ${COMPILER_BIN}/x86_64-w64-mingw32-g++ CACHE PATH "clang++" FORCE )
   #set(CMAKE_FIND_ROOT_PATH ${HOME}/workspace/llvm-mingw-20240404-ucrt-ubuntu-20.04-x86_64/bin)

   #set(CMAKE_TOOLCHAIN_FILE /home/nmms4680/workspace/vcpkg/scripts/buildsystems/vcpkg.cmake)
   #set(VCPKG_CHAINLOAD_TOOLCHAIN_FILE /home/nmms4680/workspace/khiopsdrivers/gcs/mingwtest/mingw-w64-x86.cmake)
   set(VCPKG_OVERLAY_TRIPLETS custom-vcpkg-triplets/)
   
   set(VCPKG_TARGET_TRIPLET x64-llvm-mingw-static)
   #set(VCPKG_TARGET_TRIPLET x64-llvm-mingw-dynamic)
   #set(VCPKG_DEFAULT_HOST_TRIPLET x64-llvm-mingw-static)
   #set(VCPKG_HOST_TRIPLET x64-llvm-mingw-static)

   #set(CMAKE_TOOLCHAIN_FILE toolchains/mingw-w64-x86.cmake)


   #set(VCPKG_DEFAULT_TRIPLET x64-mingw-static)
   #set(VCPKG_DEFAULT_HOST_TRIPLET x64-mingw-static)
   #set(VCPKG_TARGET_TRIPLET x64-mingw-dynamic)
   #set(CMAKE_SYSTEM_NAME Windows)
   #set(CMAKE_SYSTEM_PROCESSOR "AMD64")
   #set(CMAKE_SYSTEM_VERSION "10")
   #set(VCPKG_TRACE_FIND_PACKAGE ON)

else()
    message(STATUS "Standard compilation")
endif()

set(CMAKE_TOOLCHAIN_FILE /home/nmms4680/workspace/vcpkg/scripts/buildsystems/vcpkg.cmake)
#set(VCPKG_CHAINLOAD_TOOLCHAIN_FILE /home/nmms4680/workspace/khiopsdrivers/gcs/mingwtest/mingw-w64-x86.cmake)

#include(/home/nmms4680/workspace/vcpkg/scripts/toolchains/mingw.cmake)
#include(/home/nmms4680/workspace/vcpkg/scripts/buildsystems/vcpkg.cmake)

#if(CMAKE_SYSTEM MATCHES Linux)
#   message(STATUS "Build host runs Linux")
set(LIB libkhiopsdriver_file_gcs)
#endif()

# Define CMAKE_TOOLCHAIN_FILE to <path/to/vcpkg>/scripts/buildsystems/vcpkg.cmake


project(khiops-gcs)

#list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/vcpkg_installed/x64-linux/share/google_cloud_cpp_common)

#find_library(golang REQUIRED)
#find_program(GO_EXECUTABLE golang)

find_package(google_cloud_cpp_storage CONFIG REQUIRED)


#find_package(google_cloud_cpp_storage REQUIRED)
find_package(spdlog REQUIRED)

# Determine version number that will be included in library
# Debian package version rules, unless building locally with "cpack ."
if (NOT DEFINED CPACK_DEBIAN_PACKAGE_VERSION)
  if (NOT DEFINED MAJOR_VERSION)
    # Allow building of a test package
    SET(MAJOR_VERSION 0)
    SET(MINOR_VERSION 0)
    SET(PATCH_VERSION 1)
  endif()
  SET(CPACK_DEBIAN_PACKAGE_VERSION ${MAJOR_VERSION}.${MINOR_VERSION}.${PATCH_VERSION})
endif()

add_library(khiopsdriver_file_gcs MODULE gcsplugin.cpp)
if (VCPKG_TARGET_WINDOWS)
#set_target_properties(khiopsdriver_file_gcs PROPERTIES COMPILE_FLAGS “-m32” LINK_FLAGS “-m32”)
set_target_properties(khiopsdriver_file_gcs PROPERTIES LINK_FLAGS “-m32”)
endif()

add_executable(KhiopsPluginTest
  test/main.cpp
)
#add_dependencies(KhiopsPluginTest khiopsdriver_file_gcs)
target_link_libraries(KhiopsPluginTest)

target_link_libraries(khiopsdriver_file_gcs PUBLIC google-cloud-cpp::storage PRIVATE spdlog::spdlog $<$<BOOL:${MINGW}>:ws2_32>)

#target_link_libraries(KhiopsPluginTest LINK_PUBLIC khiopsdriver_file_gcs)

# Test program is not part of the installation
#install(TARGETS KhiopsPluginTest)
install(FILES ${LIB} DESTINATION "/usr/lib/")

# Packaging
IF(EXISTS "${CMAKE_ROOT}/Modules/CPack.cmake")
INCLUDE(InstallRequiredSystemLibraries)

SET(CPACK_SET_DESTDIR "off")
SET(CPACK_PACKAGING_INSTALL_PREFIX "/usr/lib")
SET(CPACK_GENERATOR "RPM;DEB")

find_program(DPKG_CMD dpkg)
execute_process(COMMAND "${DPKG_CMD}" --print-architecture
 OUTPUT_VARIABLE CPACK_DEBIAN_PACKAGE_ARCHITECTURE
 OUTPUT_STRIP_TRAILING_WHITESPACE
)

SET(CPACK_PACKAGE_DESCRIPTION "Khiops GCS Driver")
SET(CPACK_PACKAGE_DESCRIPTION_SUMMARY "The Khiops GCS driver allows Khiops to read files on GCS.")
SET(CPACK_PACKAGE_VERSION_MAJOR "${MAJOR_VERSION}")
SET(CPACK_PACKAGE_VERSION_MINOR "${MINOR_VERSION}")
SET(CPACK_PACKAGE_VERSION_PATCH "${PATCH_VERSION}")

SET(CPACK_DEBIAN_PACKAGE_PRIORITY "optional")
SET(CPACK_DEBIAN_PACKAGE_SECTION "math")
SET(CPACK_DEBIAN_PACKAGE_DEPENDS "khiops-core (>= 9.5)")
SET(CPACK_DEBIAN_PACKAGE_MAINTAINER "Stéphane GOUACHE <stephane.gouache@orange.com>")

SET(CPACK_RPM_PACKAGE_GROUP "math")
SET(CPACK_RPM_PACKAGE_REQUIRES "khiops-core >= 9.5")
SET(CPACK_RPM_PACKAGE_ARCHITECTURE "x86_64")

SET(CPACK_PACKAGE_FILE_NAME "${CMAKE_PROJECT_NAME}_${MAJOR_VERSION}.${MINOR_VERSION}.${CPACK_PACKAGE_VERSION_PATCH}_${CPACK_DEBIAN_PACKAGE_ARCHITECTURE}")
SET(CPACK_RPM_FILE_NAME  "${CMAKE_PROJECT_NAME}_${MAJOR_VERSION}.${MINOR_VERSION}.${CPACK_PACKAGE_VERSION_PATCH}.${CPACK_RPM_PACKAGE_ARCHITECTURE}.rpm")
SET(CPACK_SOURCE_PACKAGE_FILE_NAME "${CMAKE_PROJECT_NAME}_${MAJOR_VERSION}.${MINOR_VERSION}.${CPACK_PACKAGE_VERSION_PATCH}")

SET(CPACK_COMPONENTS_ALL Libraries ApplicationData)
INCLUDE(CPack)
 
ENDIF(EXISTS "${CMAKE_ROOT}/Modules/CPack.cmake")
