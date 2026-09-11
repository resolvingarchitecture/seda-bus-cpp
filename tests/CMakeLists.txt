add_executable(seda_bus_tests
    test_main.cpp
    test_envelope.cpp
    test_bus.cpp
)
target_include_directories(seda_bus_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../third_party)
target_link_libraries(seda_bus_tests PRIVATE seda_bus)

find_package(Threads REQUIRED)
target_link_libraries(seda_bus_tests PRIVATE Threads::Threads)

include(CTest)
add_test(NAME seda_bus_tests COMMAND seda_bus_tests)
