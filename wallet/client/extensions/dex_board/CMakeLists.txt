cmake_minimum_required(VERSION 3.13)

add_library(dex_board STATIC)
target_sources(dex_board
    PUBLIC
        dex_board.h
        dex_order.h
    PRIVATE
        dex_board.cpp
        dex_order.cpp
)

target_link_libraries(dex_board 
    PUBLIC
        broadcast_gateway
        wallet
)

target_compile_features(dex_board PUBLIC ${BEAM_INTERFACE_STANDARD})

