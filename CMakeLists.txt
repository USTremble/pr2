cmake_minimum_required(VERSION 3.28)
project(pr1main)

set(CMAKE_CXX_STANDARD 20)

add_executable(server main.cpp)

add_executable(client client.cpp)
