cmake_minimum_required(VERSION 3.16)

# 设置项目名称和使用的 C++ 标准
project(Plin)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_BUILD_TYPE Debug)

# 添加编译器标志
set(CMAKE_CXX_FLAGS "-Iinclude -mclflushopt -mclwb -fopenmp -march=native -O3")

# 添加服务器端可执行文件
add_executable(test_server test_server.cpp)

# 添加客户端可执行文件
add_executable(test_client test_client.cpp)

# 链接库
# 如果服务器和客户端都需要链接到相同的库
target_link_libraries(test_server -lpmemobj -pthread)
target_link_libraries(test_client -lpmemobj -pthread)
