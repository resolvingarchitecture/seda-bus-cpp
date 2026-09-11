add_executable(pipeline pipeline.cpp)
target_link_libraries(pipeline PRIVATE seda_bus)

find_package(Threads REQUIRED)
target_link_libraries(pipeline PRIVATE Threads::Threads)
