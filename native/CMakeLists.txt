cmake_minimum_required(VERSION 3.16)
project(mariadb-scylla-migrator VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Build type
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Compiler flags
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -DDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")

# Options
option(BUILD_JNI "Build JNI interface for Java/Scala integration" ON)
option(BUILD_SHARED "Build shared library" ON)
option(BUILD_STATIC "Build static library" ON)
option(BUILD_TESTS "Build tests" OFF)

# Find required packages

# MariaDB Connector/C (version 3.4.8)
# Try pkg-config first, then fall back to manual search
find_package(PkgConfig)
if(PKG_CONFIG_FOUND)
    pkg_check_modules(MARIADB libmariadb)
endif()

if(NOT MARIADB_FOUND)
    # Manual search for MariaDB
    find_path(MARIADB_INCLUDE_DIR mysql.h
        HINTS
            /usr/include/mariadb
            /usr/local/include/mariadb
            /usr/include/mysql
            /usr/local/include/mysql
            $ENV{MARIADB_DIR}/include
    )
    
    find_library(MARIADB_LIBRARY
        NAMES mariadb mariadbclient mysql mysqlclient
        HINTS
            /usr/lib
            /usr/lib64
            /usr/local/lib
            /usr/local/lib64
            $ENV{MARIADB_DIR}/lib
    )
    
    if(MARIADB_INCLUDE_DIR AND MARIADB_LIBRARY)
        set(MARIADB_FOUND TRUE)
        set(MARIADB_INCLUDE_DIRS ${MARIADB_INCLUDE_DIR})
        set(MARIADB_LIBRARIES ${MARIADB_LIBRARY})
    endif()
endif()

if(NOT MARIADB_FOUND)
    message(FATAL_ERROR 
        "MariaDB Connector/C not found. Please install mariadb-connector-c "
        "or set MARIADB_DIR environment variable.")
endif()

message(STATUS "Found MariaDB: ${MARIADB_INCLUDE_DIRS}")

# ScyllaDB cpp-rs-driver (version 0.5.1)
# The driver is typically installed as libscylla-cpp-driver
find_path(SCYLLADB_INCLUDE_DIR cassandra.h
    HINTS
        /usr/include
        /usr/local/include
        $ENV{SCYLLADB_DRIVER_DIR}/include
)

find_library(SCYLLADB_LIBRARY
    NAMES scylla-cpp-driver cassandra
    HINTS
        /usr/lib
        /usr/lib64
        /usr/local/lib
        /usr/local/lib64
        $ENV{SCYLLADB_DRIVER_DIR}/lib
)

if(NOT SCYLLADB_INCLUDE_DIR OR NOT SCYLLADB_LIBRARY)
    message(FATAL_ERROR 
        "ScyllaDB cpp-rs-driver not found. Please install the driver "
        "or set SCYLLADB_DRIVER_DIR environment variable.")
endif()

message(STATUS "Found ScyllaDB driver: ${SCYLLADB_INCLUDE_DIR}")

# RapidJSON for JSON serialization in JNI bridge
find_path(RAPIDJSON_INCLUDE_DIR rapidjson/document.h
    HINTS
        /usr/include
        /usr/local/include
        $ENV{RAPIDJSON_DIR}/include
)

if(NOT RAPIDJSON_INCLUDE_DIR)
    message(STATUS "RapidJSON not found, will use bundled headers or simple JSON")
    # Download RapidJSON header-only library if not found
    include(FetchContent)
    FetchContent_Declare(
        rapidjson
        GIT_REPOSITORY https://github.com/Tencent/rapidjson.git
        GIT_TAG v1.1.0
    )
    FetchContent_MakeAvailable(rapidjson)
    set(RAPIDJSON_INCLUDE_DIR ${rapidjson_SOURCE_DIR}/include)
else()
    message(STATUS "Found RapidJSON: ${RAPIDJSON_INCLUDE_DIR}")
endif()

# JNI (for Java/Scala integration)
if(BUILD_JNI)
    find_package(JNI)
    if(NOT JNI_FOUND)
        message(WARNING "JNI not found. JNI interface will not be built.")
        set(BUILD_JNI OFF)
    else()
        message(STATUS "Found JNI: ${JNI_INCLUDE_DIRS}")
    endif()
endif()

# Source files
set(CORE_SOURCES
    src/mariadb_connection.cpp
    src/scylladb_connection.cpp
    src/migrator.cpp
)

set(JNI_SOURCES
    src/jni_bridge.cpp
)

# Include directories
include_directories(
    ${CMAKE_SOURCE_DIR}/include
    ${MARIADB_INCLUDE_DIRS}
    ${SCYLLADB_INCLUDE_DIR}
    ${RAPIDJSON_INCLUDE_DIR}
)

# Core library (shared)
if(BUILD_SHARED)
    add_library(mariadb-scylla-migrator SHARED ${CORE_SOURCES})
    target_link_libraries(mariadb-scylla-migrator
        ${MARIADB_LIBRARIES}
        ${SCYLLADB_LIBRARY}
        pthread
    )
    set_target_properties(mariadb-scylla-migrator PROPERTIES
        VERSION ${PROJECT_VERSION}
        SOVERSION 1
    )
    install(TARGETS mariadb-scylla-migrator
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
    )
endif()

# Core library (static)
if(BUILD_STATIC)
    add_library(mariadb-scylla-migrator-static STATIC ${CORE_SOURCES})
    target_link_libraries(mariadb-scylla-migrator-static
        ${MARIADB_LIBRARIES}
        ${SCYLLADB_LIBRARY}
        pthread
    )
    set_target_properties(mariadb-scylla-migrator-static PROPERTIES
        OUTPUT_NAME mariadb-scylla-migrator
    )
    install(TARGETS mariadb-scylla-migrator-static
        ARCHIVE DESTINATION lib
    )
endif()

# JNI library for Scala integration
if(BUILD_JNI AND JNI_FOUND)
    add_library(mariadb-scylla-migrator-jni SHARED ${CORE_SOURCES} ${JNI_SOURCES})
    target_include_directories(mariadb-scylla-migrator-jni PRIVATE ${JNI_INCLUDE_DIRS})
    target_link_libraries(mariadb-scylla-migrator-jni
        ${MARIADB_LIBRARIES}
        ${SCYLLADB_LIBRARY}
        pthread
    )
    set_target_properties(mariadb-scylla-migrator-jni PROPERTIES
        VERSION ${PROJECT_VERSION}
        SOVERSION 1
    )
    
    # Platform-specific JNI library naming
    if(APPLE)
        set_target_properties(mariadb-scylla-migrator-jni PROPERTIES
            PREFIX "lib"
            SUFFIX ".jnilib"
        )
    elseif(WIN32)
        set_target_properties(mariadb-scylla-migrator-jni PROPERTIES
            PREFIX ""
            SUFFIX ".dll"
        )
    endif()
    
    install(TARGETS mariadb-scylla-migrator-jni
        LIBRARY DESTINATION lib
    )
endif()

# Install headers
install(FILES
    include/mariadb_scylla_migrator.h
    include/mariadb_scylla_migrator_jni.h
    DESTINATION include
)

# Tests
if(BUILD_TESTS)
    enable_testing()
    
    add_executable(migrator_test tests/migrator_test.cpp)
    target_link_libraries(migrator_test
        mariadb-scylla-migrator
        ${MARIADB_LIBRARIES}
        ${SCYLLADB_LIBRARY}
    )
    add_test(NAME migrator_test COMMAND migrator_test)
endif()

# Print configuration summary
message(STATUS "")
message(STATUS "Configuration Summary:")
message(STATUS "  Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "  Build shared: ${BUILD_SHARED}")
message(STATUS "  Build static: ${BUILD_STATIC}")
message(STATUS "  Build JNI: ${BUILD_JNI}")
message(STATUS "  Build tests: ${BUILD_TESTS}")
message(STATUS "")
