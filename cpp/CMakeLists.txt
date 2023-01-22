project(rosetta-db)
cmake_minimum_required(VERSION 3.20)

# Set RANGES_BUILD_TESTS to OFF to disable tests for tl::ranges library
set(RANGES_BUILD_TESTS OFF CACHE BOOL "Build tests for tl::ranges" FORCE)
set(SPDLOG_USE_STD_FORMAT OFF CACHE BOOL "Use std::format for fmt" FORCE)

Include(FetchContent)

FetchContent_Declare(
    Catch2
    GIT_REPOSITORY https://github.com/catchorg/Catch2
    GIT_TAG        devel # For GCC 13 support, due to missing #include for <cstdint>
)
FetchContent_MakeAvailable(Catch2)

FetchContent_Declare(
    spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog
    GIT_TAG        v1.11.0
)
FetchContent_MakeAvailable(spdlog)

FetchContent_Declare(
    backward
    GIT_REPOSITORY https://github.com/damageboy/backward-cpp
    GIT_TAG master # Fork which has proper CMake ALIAS target   
)
FetchContent_MakeAvailable(backward)

FetchContent_Declare(
    tl-ranges
    GIT_REPOSITORY https://github.com/TartanLlama/ranges
    GIT_TAG        main
)
FetchContent_MakeAvailable(tl-ranges)

FetchContent_Declare(
    libassert
    GIT_REPOSITORY https://github.com/jeremy-rifkin/libassert
    GIT_TAG v1.1
)
FetchContent_MakeAvailable(libassert)

FetchContent_Declare(
    ring_span_lite
    GIT_REPOSITORY https://github.com/martinmoene/ring-span-lite
    GIT_TAG master
)
FetchContent_MakeAvailable(ring_span_lite)



# Add source files
file(GLOB_RECURSE SOURCES "src/*.cpp")
add_executable(rosetta-db ${SOURCES})
target_include_directories(rosetta-db
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_compile_options(rosetta-db
    PRIVATE
        $ENV{CXX_FLAGS}
)
target_link_libraries(rosetta-db
    PRIVATE
        spdlog::spdlog
        Backward::Backward bfd dl
        tl::ranges
        assert dl
)

# Configure Catch2 test discovery
# list(APPEND CMAKE_MODULE_PATH ${catch2_SOURCE_DIR}/extras)
# include(CTest)
# include(Catch)
# catch_discover_tests(rosetta-db)

# Copy compile_commands.json to root directory if changed
add_custom_command(TARGET rosetta-db POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        ${CMAKE_CURRENT_BINARY_DIR}/compile_commands.json
        ${CMAKE_CURRENT_SOURCE_DIR}/compile_commands.json
)