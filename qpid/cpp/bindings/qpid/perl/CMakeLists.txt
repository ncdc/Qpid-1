#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

##--------------------------------------------------------
## Properites used for generating the Perl source tarball.
##--------------------------------------------------------
set(PERL_SOURCES_DIR ${CMAKE_BINARY_DIR}/perl-qpid-${qpidc_version})
set(PERL_EXAMPLES_DIR ${CMAKE_SOURCE_DIR}/bindings/qpid/examples/perl)
set(PERL_SWIG_FILE ${CMAKE_CURRENT_SOURCE_DIR}/perl.i)
set(PERL_SWIG_TYPES ${CMAKE_CURRENT_SOURCE_DIR}/../../swig_perl_typemaps.i)
set(PERL_MAKEFILE ${CMAKE_CURRENT_SOURCE_DIR}/Makefile.PL)
set(PERL_SOURCES_TGZ ${PERL_SOURCES_DIR}.tar.gz)
set(PERL_BINDINGS_LICENSE  ${CMAKE_CURRENT_SOURCE_DIR}/LICENSE)

##------------------------------------------------------
## Use Swig to generate a literal binding to the C++ API
##------------------------------------------------------
set_source_files_properties(${PERL_SWIG_FILE} PROPERTIES CPLUSPLUS ON)
set_source_files_properties(${CMAKE_CURRENT_SOURCE_DIR}/perl.i PROPERTIES SWIG_FLAGS "-I${qpid-cpp_SOURCE_DIR}/include")

swig_add_module(cqpid_perl perl ${PERL_SWIG_FILE})
swig_link_libraries(cqpid_perl qpidmessaging qpidtypes qmf2 ${PERL_LIBRARY})

set_source_files_properties(${swig_generated_file_fullname} PROPERTIES COMPILE_FLAGS "-fno-strict-aliasing -I${PERL_INCLUDE_PATH} -I${qpid-cpp_SOURCE_DIR}/include")

##----------------------------------
## Install the complete Perl binding
##----------------------------------
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/libcqpid_perl.so
              ${CMAKE_CURRENT_BINARY_DIR}/cqpid_perl.pm
        DESTINATION ${PERL_ARCHLIB}
        COMPONENT ${QPID_COMPONENT_CLIENT}
        )

add_custom_command(OUTPUT ${PERL_SOURCES_DIR}
  COMMAND cmake -E make_directory ${PERL_SOURCES_DIR}
  COMMAND cmake -E make_directory ${PERL_SOURCES_DIR}/examples
  COMMAND cmake -E copy ${PERL_MAKEFILE} ${PERL_SOURCES_DIR}
  COMMAND cmake -E copy ${PERL_SWIG_FILE} ${PERL_SOURCES_DIR}
  COMMAND cmake -E copy ${PERL_SWIG_TYPES} ${PERL_SOURCES_DIR}
  COMMAND cmake -E copy_directory ${PERL_EXAMPLES_DIR} ${PERL_SOURCES_DIR}/examples
  COMMAND cmake -E copy ${PERL_BINDINGS_LICENSE} ${PERL_SOURCES_DIR})

add_custom_command(OUTPUT ${PERL_SOURCES_TGZ}
                   COMMAND cmake -E tar zcvf ${PERL_SOURCES_TGZ} ${PERL_SOURCES_DIR}
                   DEPENDS ${PERL_SOURCES_DIR}
                   )

add_custom_target(perl_sources
                  DEPENDS ${PERL_SOURCES_TGZ}
                 )
