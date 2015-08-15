include_directories("../../")

#build mrnet histogram multilevel app
add_executable(front front.C)
add_executable(backend backend.C)
add_library(filter SHARED filter_init.C)
target_link_libraries(front flow)
target_link_libraries(backend flow)
target_link_libraries(filter flow)


# the RPATH to be used when installing, but only if it's not a system directory
LIST(FIND CMAKE_PLATFORM_IMPLICIT_LINK_DIRECTORIES "${CMAKE_INSTALL_PREFIX}/bin/histogram-multilevel" isSystemDir)
IF("${isSystemDir}" STREQUAL "-1")
   SET(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/bin/histogram-multilevel")
ENDIF("${isSystemDir}" STREQUAL "-1")

install(TARGETS front DESTINATION bin/histogram-multilevel)
install(TARGETS backend DESTINATION bin/histogram-multilevel)
install(TARGETS filter DESTINATION bin/histogram-multilevel)
