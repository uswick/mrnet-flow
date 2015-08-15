include_directories("../../")

#build mrnet histogram multilevel app
add_executable(hist_front front.C)
add_executable(hist_backend backend.C)
add_library(hist_filter SHARED filter_init.C)
target_link_libraries(hist_front flow)
target_link_libraries(hist_backend flow)
target_link_libraries(hist_filter flow)


# the RPATH to be used when installing, but only if it's not a system directory
LIST(FIND CMAKE_PLATFORM_IMPLICIT_LINK_DIRECTORIES "${CMAKE_INSTALL_PREFIX}/bin/histogram" isSystemDir)
IF("${isSystemDir}" STREQUAL "-1")
   SET(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/bin/histogram")
ENDIF("${isSystemDir}" STREQUAL "-1")

set_target_properties(hist_front PROPERTIES OUTPUT_NAME front)
set_target_properties(hist_filter PROPERTIES OUTPUT_NAME filter)
set_target_properties(hist_backend PROPERTIES OUTPUT_NAME backend)

install(TARGETS hist_front DESTINATION bin/histogram)
install(TARGETS hist_backend DESTINATION bin/histogram)
install(TARGETS hist_filter DESTINATION bin/histogram)
