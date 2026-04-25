# Copyright 2025 The Silkworm Authors
# SPDX-License-Identifier: Apache-2.0

add_subdirectory(intx)

# Treat intx headers as system includes to suppress C++23 deprecation warnings
if(TARGET intx)
  get_target_property(_intx_dirs intx INTERFACE_INCLUDE_DIRECTORIES)
  if(_intx_dirs)
    set_target_properties(intx PROPERTIES INTERFACE_INCLUDE_DIRECTORIES "")
    target_include_directories(intx SYSTEM INTERFACE ${_intx_dirs})
  endif()
endif()
