# boost - header-only + minimal compiled libs via bootstrap/b2
set(BOOST_SRC ${TP_SOURCE_DIR}/boost_1_81_0)
set(BOOST_BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR}/boost)

# Boost 1.81 does NOT have a root CMakeLists.txt.
# Use header-only approach and build required libs via b2 during build phase.
set(BOOST_DATE_TIME_A "${BOOST_BUILD_DIR}/lib/libboost_date_time.a")
set(BOOST_SYSTEM_A "${BOOST_BUILD_DIR}/lib/libboost_system.a")

include(ProcessorCount)
ProcessorCount(NPROC)

add_custom_command(
    OUTPUT ${BOOST_DATE_TIME_A} ${BOOST_SYSTEM_A}
    COMMAND ${CMAKE_COMMAND} -E make_directory ${BOOST_BUILD_DIR}
    COMMAND ${BOOST_SRC}/bootstrap.sh --prefix=${BOOST_BUILD_DIR} --with-libraries=date_time,system,container
    COMMAND ${BOOST_SRC}/b2 install --prefix=${BOOST_BUILD_DIR} --with-date_time --with-system link=static variant=release cxxflags=-fPIC -j${NPROC}
    WORKING_DIRECTORY ${BOOST_SRC}
    COMMENT "Building boost from source (b2)..."
)

add_custom_target(boost_builder DEPENDS ${BOOST_DATE_TIME_A} ${BOOST_SYSTEM_A})

# Header-only interface for all of Boost
add_library(boost_headers INTERFACE)
target_include_directories(boost_headers INTERFACE ${BOOST_SRC})

# Compiled boost libs
foreach(_lib boost_date_time boost_system)
    add_library(${_lib} STATIC IMPORTED GLOBAL)
    set_target_properties(${_lib} PROPERTIES IMPORTED_LOCATION "${BOOST_BUILD_DIR}/lib/lib${_lib}.a")
    add_dependencies(${_lib} boost_builder)
endforeach()

# boost_container is header-only in most usages
add_library(boost_container INTERFACE)
target_include_directories(boost_container INTERFACE ${BOOST_SRC})

# Create IMPORTED targets instead of ALIAS because Arrow modifies them via target_link_libraries
if(NOT TARGET Boost::headers)
    add_library(Boost::headers INTERFACE IMPORTED GLOBAL)
    target_link_libraries(Boost::headers INTERFACE boost_headers)
endif()
if(TARGET boost_date_time AND NOT TARGET Boost::date_time)
    add_library(Boost::date_time INTERFACE IMPORTED GLOBAL)
    target_link_libraries(Boost::date_time INTERFACE boost_date_time)
endif()
if(NOT TARGET Boost::container)
    add_library(Boost::container INTERFACE IMPORTED GLOBAL)
    target_link_libraries(Boost::container INTERFACE boost_container)
endif()
if(TARGET boost_system AND NOT TARGET Boost::system)
    add_library(Boost::system INTERFACE IMPORTED GLOBAL)
    target_link_libraries(Boost::system INTERFACE boost_system)
endif()
