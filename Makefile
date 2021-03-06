REPO_PATH=/N/u/uswickra/Karst/MRNet/mrnet_4.1.0
BOOST_INSTALL_DIR=/N/u/uswickra/Karst/boost/boost_1_52_0/install

#REPO_PATH=/home/udayanga/Software/Flow/MRNet/mrnet_4.1.0/build/x86_64-unknown-linux-gnu
#BOOST_INSTALL_DIR=/home/udayanga/Software/Flow/Boost/boost_1_52_0/install

MRNET_CXXFLAGS = -g -fPIC -D__STDC_LIMIT_MACROS -D__STDC_CONSTANT_MACROS -D__STDC_FORMAT_MACROS  \
				-I${REPO_PATH}/include/mrnet  \
				-I${BOOST_INSTALL_DIR}/include  \
				-I${REPO_PATH}/include  \
				-I${REPO_PATH}/build/x86_64-unknown-linux-gnu/  \
        		-I${REPO_PATH}/include/xplat \
        		-I${REPO_PATH}/xplat/include \
                -I${ROOT_PATH}/ \
                    -Dos_linux -std=c++11

TEST_CXXFLAGS= -g  -Iapps/histogram/tests/ -std=c++11

CXX = g++
#CXX = clang++
CXXFLAGS = -fPIC -g  -I${BOOST_INSTALL_DIR}/include -std=c++11

LDFLAGS = -L${BOOST_INSTALL_DIR}/lib -lboost_thread -lboost_system

#MRNET_SOFLAGS =
MRNET_SOFLAGS = -fPIC -shared -rdynamic

#MRNET_LIBS = -L${REPO_PATH}/mrnet/lib -lmrnet -lxplat -lm -lpthread -ldl
MRNET_LIBS = ${REPO_PATH}/lib/libmrnet.a.4.0.0  ${REPO_PATH}/lib/libxplat.a.4.0.0 -L${BOOST_INSTALL_DIR}/lib -lm -lpthread -ldl -lboost_system -lboost_timer -lboost_thread -lboost_chrono

all: dataTest

schema.o: schema.C data.h schema.h
	${CXX} ${CXXFLAGS} -I/usr/include schema.C -c -o schema.o

data.o: data.C data.h schema.h
	${CXX} ${CXXFLAGS} -I/usr/include data.C -c -o data.o

operator.o: operator.C operator.h data.h schema.h
	${CXX} ${CXXFLAGS} -I/usr/include operator.C -c -o operator.o

process.o: process.C process.h sight_common_internal.h
	${CXX} ${CXXFLAGS} -I/usr/include process.C -c -o process.o

sight_common.o: sight_common.C process.h sight_common_internal.h
	${CXX} ${CXXFLAGS} -I/usr/include sight_common.C -c -o sight_common.o

utils.o: utils.C utils.h
	${CXX} ${CXXFLAGS} -I/usr/include utils.C -c -o utils.o

dataTest: dataTest.C *.h schema.o data.o operator.o process.o sight_common.o utils.o
	${CXX} ${CXXFLAGS} -I/usr/include dataTest.C schema.o data.o operator.o process.o sight_common.o utils.o -o dataTest ${LDFLAGS}

#MRNet integration specific targets
.PHONY: mrnop
mrnop: dataTest front backend filter.so simple_topgen

simple_topgen: simple_topgen.C
	${CXX} -g  simple_topgen.C -o simple_topgen

mrnet_operator.o: mrnet_operator.C mrnet_operator.h mrnet_flow.h
	${CXX} ${MRNET_CXXFLAGS} -I/usr/include mrnet_operator.C -c -o mrnet_operator.o

mrnet_flow.o: mrnet_flow.C mrnet_flow.h
	${CXX} ${MRNET_CXXFLAGS} -I/usr/include mrnet_flow.C -c -o mrnet_flow.o

filter_init.o: mrnet_operator.h mrnet_flow.h filter_init.h
	${CXX} ${MRNET_CXXFLAGS} -I/usr/include filter_init.C -c -o filter_init.o

front: front.C mrnet_operator.o *.h schema.o data.o operator.o process.o sight_common.o utils.o mrnet_flow.o
	${CXX} ${MRNET_CXXFLAGS} -I/usr/include front.C mrnet_operator.o schema.o data.o operator.o process.o sight_common.o utils.o mrnet_flow.o -o front ${MRNET_LIBS}

backend: backend.C mrnet_operator.o *.h schema.o data.o operator.o process.o sight_common.o utils.o mrnet_flow.o
	${CXX} ${MRNET_CXXFLAGS} -I/usr/include backend.C mrnet_operator.o schema.o data.o operator.o process.o sight_common.o utils.o mrnet_flow.o -o backend ${MRNET_LIBS}

filter.so: filter.C mrnet_operator.o filter_init.o *.h schema.o data.o operator.o process.o sight_common.o utils.o mrnet_flow.o
	${CXX} ${MRNET_CXXFLAGS} ${MRNET_SOFLAGS} -I/usr/include filter.C mrnet_operator.o filter_init.o schema.o data.o operator.o process.o sight_common.o utils.o mrnet_flow.o -o filter.so ${MRNET_LIBS}

#############################################################
#
#build apps
#currently source for histogram app
#############################################################

.PHONY: histogram
histogram: mrnop apps/histogram/front apps/histogram/backend apps/histogram/filter.so

apps/histogram/filter_init.o: mrnet_operator.h mrnet_flow.h filter_init.h
	${CXX} ${MRNET_CXXFLAGS} -I./ apps/histogram/filter_init.C -c -o apps/histogram/filter_init.o

apps/histogram/front: apps/histogram/front.C mrnet_operator.o *.h schema.o data.o operator.o process.o sight_common.o utils.o mrnet_flow.o
	${CXX} ${MRNET_CXXFLAGS} -I./ apps/histogram/front.C mrnet_operator.o schema.o data.o operator.o process.o sight_common.o utils.o mrnet_flow.o -o apps/histogram/front ${MRNET_LIBS}

apps/histogram/backend: apps/histogram/backend.C mrnet_operator.o *.h schema.o data.o operator.o process.o sight_common.o utils.o mrnet_flow.o
	${CXX} ${MRNET_CXXFLAGS} -I./ apps/histogram/backend.C mrnet_operator.o schema.o data.o operator.o process.o sight_common.o utils.o mrnet_flow.o -o apps/histogram/backend ${MRNET_LIBS}

apps/histogram/filter.so: filter.C mrnet_operator.o apps/histogram/filter_init.o *.h schema.o data.o operator.o process.o sight_common.o utils.o mrnet_flow.o
	${CXX} ${MRNET_CXXFLAGS} ${MRNET_SOFLAGS} -I./ filter.C mrnet_operator.o apps/histogram/filter_init.o schema.o data.o operator.o process.o sight_common.o utils.o mrnet_flow.o -o apps/histogram/filter.so ${MRNET_LIBS}


#############################################################
#
#build app tests
#currently tests are only for histogram app
#############################################################
TESTS= apps/histogram/tests/histogram_aggregate_test apps/histogram/tests/histogram_properties_test apps/histogram/tests/histogram_coloumn_properties_test apps/histogram/tests/record_join_operator_test apps/histogram/tests/histogram_serialization_test \
apps/histogram/tests/histogram_coloumn_serialization_test apps/histogram/tests/app_common_test
TEST_OBJS = apps/histogram/tests/flow_test.o schema.o data.o operator.o process.o sight_common.o utils.o mrnet_flow.o

.PHONY: tests
#tests: apps/histogram/tests/flow_test.o apps/histogram/tests/histogram_aggregate_test apps/histogram/tests/histogram_properties_test apps/histogram/tests/histogram_coloumn_properties_test
tests: apps/histogram/tests/flow_test.o ${TESTS}
	@echo "\n\n************************************\n***            TESTS             ***\n************************************\n"
	for T in ${TESTS}; do  $$T ; done

apps/histogram/tests/flow_test.o: apps/histogram/tests/flow_test.C mrnet_operator.h mrnet_flow.h data.h schema.h operator.h apps/histogram/tests/flow_test.h
	${CXX} ${TEST_CXXFLAGS} ${MRNET_CXXFLAGS} -I./ apps/histogram/tests/flow_test.C -c -o apps/histogram/tests/flow_test.o


apps/histogram/tests/histogram_aggregate_test: apps/histogram/tests/histogram_aggregate_test.C *.h schema.o ${TEST_OBJS}
	${CXX} ${TEST_CXXFLAGS} ${MRNET_CXXFLAGS} -I./ apps/histogram/tests/histogram_aggregate_test.C ${TEST_OBJS} -o apps/histogram/tests/histogram_aggregate_test ${MRNET_LIBS}


apps/histogram/tests/histogram_properties_test: apps/histogram/tests/histogram_properties_test.C *.h schema.o ${TEST_OBJS}
	${CXX} ${TEST_CXXFLAGS} ${MRNET_CXXFLAGS} -I./ apps/histogram/tests/histogram_properties_test.C ${TEST_OBJS} -o apps/histogram/tests/histogram_properties_test ${MRNET_LIBS}

apps/histogram/tests/histogram_coloumn_properties_test: apps/histogram/tests/histogram_coloumn_properties_test.C *.h schema.o ${TEST_OBJS}
	${CXX} ${TEST_CXXFLAGS} ${MRNET_CXXFLAGS} -I./ apps/histogram/tests/histogram_coloumn_properties_test.C ${TEST_OBJS} -o apps/histogram/tests/histogram_coloumn_properties_test ${MRNET_LIBS}

apps/histogram/tests/record_join_operator_test: apps/histogram/tests/record_join_operator_test.C *.h schema.o ${TEST_OBJS}
	${CXX} ${TEST_CXXFLAGS} ${MRNET_CXXFLAGS} -I./ apps/histogram/tests/record_join_operator_test.C ${TEST_OBJS} -o apps/histogram/tests/record_join_operator_test ${MRNET_LIBS}

apps/histogram/tests/histogram_serialization_test: apps/histogram/tests/histogram_serialization_test.C *.h schema.o ${TEST_OBJS}
	${CXX} ${TEST_CXXFLAGS} ${MRNET_CXXFLAGS} -I./ apps/histogram/tests/histogram_serialization_test.C ${TEST_OBJS} -o apps/histogram/tests/histogram_serialization_test ${MRNET_LIBS}

apps/histogram/tests/histogram_coloumn_serialization_test: apps/histogram/tests/histogram_coloumn_serialization_test.C *.h schema.o ${TEST_OBJS}
	${CXX} ${TEST_CXXFLAGS} ${MRNET_CXXFLAGS} -I./ apps/histogram/tests/histogram_coloumn_serialization_test.C ${TEST_OBJS} -o apps/histogram/tests/histogram_coloumn_serialization_test ${MRNET_LIBS}

apps/histogram/tests/app_common_test: apps/histogram/tests/app_common_test.C  *.h schema.o ${TEST_OBJS}
	${CXX} ${TEST_CXXFLAGS} ${MRNET_CXXFLAGS} -I./  apps/histogram/tests/app_common_test.C ${TEST_OBJS} -o apps/histogram/tests/app_common_test ${MRNET_LIBS}


#############################################################
# end of tests
# ############################################################

clean:
	rm -f *.o dataTest front backend filter.so simple_topgen apps/histogram/*.o apps/histogram/front apps/histogram/filter.so apps/histogram/backend apps/histogram/tests/*.o ${TESTS}
