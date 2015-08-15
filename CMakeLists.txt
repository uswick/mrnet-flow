#
# This is project ======Flow======
# 

project (flow)
cmake_minimum_required(VERSION 2.8)

# Version information
set(FLOW_MAJOR_VERSION 1)
set(FLOW_MINOR_VERSION 0)
set(FLOW_PATCH_VERSION 0)
set(FLOW_VERSION "${FLOW_MAJOR_VERSION}.${FLOW_MINOR_VERSION}.${FLOW_PATCH_VERSION}")


set(CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmakeFind;${CMAKE_MODULE_PATH}")


#mrnet library
add_library(mrnet STATIC IMPORTED)
set_property(TARGET mrnet PROPERTY IMPORTED_LOCATION  ${MRNET_INSTALL_DIR}/lib/libmrnet.a.4.0.0 )
add_library(xplat STATIC IMPORTED)
set_property(TARGET xplat PROPERTY IMPORTED_LOCATION  ${MRNET_INSTALL_DIR}/lib/libxplat.a.4.0.0 )

set(LIBMRNET_INCLUDE_DIRS  ${MRNET_INSTALL_DIR}/include ${MRNET_INSTALL_DIR}/include/mrnet ${MRNET_INSTALL_DIR}/build/x86_64-unknown-linux-gnu ${MRNET_INSTALL_DIR}/include/xplat  ${MRNET_INSTALL_DIR}/include/xplat/include)

#Boost library
set(Boost_USE_STATIC_LIBS OFF) 
set(Boost_USE_MULTITHREADED ON)  
set(Boost_USE_STATIC_RUNTIME OFF) 
find_package(Boost 1.55.0 REQUIRED COMPONENTS system timer thread chrono) 

if(Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS}) 
endif()

#pthread library
find_package (Threads)

#find libm and libdl
find_library(M_LIB m)
find_library(DL_LIB dl)

#==============full rpath hadling===================
# use, i.e. don't skip the full RPATH for the build tree
SET(CMAKE_SKIP_BUILD_RPATH  FALSE)

# when building, don't use the install RPATH already
# (but later on when installing)
SET(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE) 

SET(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")

# add the automatically determined parts of the RPATH
# which point to directories outside the build tree to the install RPATH
SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# the RPATH to be used when installing, but only if it's not a system directory
LIST(FIND CMAKE_PLATFORM_IMPLICIT_LINK_DIRECTORIES "${CMAKE_INSTALL_PREFIX}/lib" isSystemDir)
IF("${isSystemDir}" STREQUAL "-1")
   SET(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
ENDIF("${isSystemDir}" STREQUAL "-1")
#==============End===================


#set main sources for main flow-library 
set(FLOW_HEADERS
    data.h
    schema.h
    operator.h
    process.h
    utils.h
    sight_common_internal.h
    filter_init.h
    mrnet_flow.h
    mrnet_operator.h)

set(FLOW_SOURCES
    data.C
    schema.C
    operator.C
    process.C
    sight_common.C
    utils.C
    filter.C
    mrnet_flow.C
    mrnet_operator.C
    dataTest.C)
  

set(CMAKE_C_FLAGS  "${CMAKE_C_FLAGS}  -std=c99 -g ")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -g -fPIC -D__STDC_LIMIT_MACROS -D__STDC_CONSTANT_MACROS -D__STDC_FORMAT_MACROS -Dos_linux")

include_directories(${LIBMRNET_INCLUDE_DIRS})

#build main flow library
add_library(flow ${FLOW_SOURCES})

target_link_libraries(flow mrnet)
target_link_libraries(flow xplat)
target_link_libraries(flow ${M_LIB})
target_link_libraries(flow ${DL_LIB})
target_link_libraries(flow ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(flow ${Boost_LIBRARIES})

#build core-test
add_executable(dataTest dataTest.C)
target_link_libraries(dataTest flow)

#build mrnet test
add_executable(demo_front front.C)
add_executable(demo_backend backend.C)
add_library(demo_filter SHARED filter_init.C)
target_link_libraries(demo_front flow)
target_link_libraries(demo_backend flow)
target_link_libraries(demo_filter flow)

#build topology generator
add_executable(simple_topgen simple_topgen.C)


install(FILES ${FLOW_HEADERS} DESTINATION include)
install(TARGETS flow DESTINATION lib)
install(TARGETS dataTest DESTINATION bin)
install(TARGETS demo_front DESTINATION bin)
install(TARGETS demo_backend DESTINATION bin)
install(TARGETS demo_filter DESTINATION lib)

install(TARGETS simple_topgen DESTINATION bin)

#build applications
add_subdirectory(apps)
