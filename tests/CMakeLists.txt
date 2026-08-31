function(add_ucp_test target source)
    add_executable(${target} ${source})
    target_link_libraries(${target} PRIVATE proactor_static pthread)
    target_include_directories(${target} PRIVATE ${PROJECT_SOURCE_DIR}/tests)
    add_test(NAME ${target} COMMAND ${target})
endfunction()

add_ucp_test(ucp_result_test result_test.cpp)
