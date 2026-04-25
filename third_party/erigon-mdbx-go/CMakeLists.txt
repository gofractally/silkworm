# Copyright 2025 The Silkworm Authors
# SPDX-License-Identifier: Apache-2.0

option(USE_PSITRI "Use PsiTri (psitrimdbx) instead of native libmdbx" OFF)

if(USE_PSITRI)
  # ── PsiTri backend ─────────────────────────────────────────────────
  # Build the full psitri library tree (sal, hash, ucc, art, psitri core),
  # then add psitrimdbx on top. The top-level CMakeLists auto-detects
  # PSITRI_AS_SUBDIRECTORY and skips project()/wrapper libs.

  set(PSITRI_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../psitri")

  # Build core psitri (sets up sal, hash, ucc, art, psitri targets)
  add_subdirectory("${PSITRI_DIR}" "${CMAKE_CURRENT_BINARY_DIR}/psitri")

  # Build psitrimdbx (the MDBX-compatible shim)
  add_subdirectory("${PSITRI_DIR}/libraries/psitrimdbx" "${CMAKE_CURRENT_BINARY_DIR}/psitrimdbx")

  # Silkworm expects a target named "mdbx-static"
  add_library(mdbx-static ALIAS psitrimdbx)

  target_compile_definitions(psitrimdbx PUBLIC CONSTEXPR_ASSERT=assert)

else()
  # ── Native libmdbx backend (default) ───────────────────────────────

  set(MDBX_ENABLE_TESTS OFF)

  if(NOT CMAKE_MESSAGE_LOG_LEVEL)
    set(CMAKE_MESSAGE_LOG_LEVEL_EMPTY YES)
    set(CMAKE_MESSAGE_LOG_LEVEL NOTICE)
  endif()

  add_subdirectory(mdbx-go/mdbxdist)

  if(CMAKE_MESSAGE_LOG_LEVEL_EMPTY)
    unset(CMAKE_MESSAGE_LOG_LEVEL)
  endif()

  target_compile_definitions(mdbx-static PUBLIC CONSTEXPR_ASSERT=assert)

  if(SILKWORM_SANITIZE STREQUAL "thread" AND CMAKE_CXX_COMPILER_ID MATCHES "GNU")
    target_compile_options(mdbx-static PRIVATE -Wno-tsan)
  endif()

  if(MDBX_BUILD_TOOLS)
    foreach(
      TOOL
      mdbx_chk mdbx_copy mdbx_stat mdbx_dump mdbx_load mdbx_drop
    )
      if((CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang") AND (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL 15))
        target_link_options(${TOOL} PRIVATE -Wl,-no_warn_duplicate_libraries)
      endif()
    endforeach()
  endif()

endif()
