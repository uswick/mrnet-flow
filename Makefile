REPO_PATH=/media/sf_Appcode/Flow/Flow/flow/mrnet

MRNET_CXXFLAGS = -g -fPIC -D__STDC_LIMIT_MACROS -D__STDC_CONSTANT_MACROS -D__STDC_FORMAT_MACROS  \
				-I${REPO_PATH}/mrnet/include/mrnet  \
				-I${REPO_PATH}/mrnet/include  \
				-I${REPO_PATH}/mrnet/build/x86_64-unknown-linux-gnu/  \
        		-I${REPO_PATH}/mrnet/include/xplat \
        		-I${REPO_PATH}/mrnet/xplat/include \
                -I${ROOT_PATH}/mrnet \
                    -Dos_linux

CXX = g++
#CXX = clang++
CXXFLAGS = -fPIC

LDFLAGS = -Wl,-E

#MRNET_SOFLAGS =
MRNET_SOFLAGS = -fPIC -shared -rdynamic

#MRNET_LIBS = -L${REPO_PATH}/mrnet/lib -lmrnet -lxplat -lm -lpthread -ldl
MRNET_LIBS = ${REPO_PATH}/mrnet/lib/libmrnet.a.4.0.0  ${REPO_PATH}/mrnet/lib/libxplat.a.4.0.0 -lm -lpthread -ldl -lboost_system -lboost_timer -lboost_thread

all: dataTest

schema.o: schema.C data.h schema.h
	${CXX} -g ${CXXFLAGS} -I/usr/include schema.C -c -o schema.o

data.o: data.C data.h schema.h
	${CXX} -g ${CXXFLAGS} -I/usr/include data.C -c -o data.o

operator.o: operator.C operator.h data.h schema.h
	${CXX} -g ${CXXFLAGS} -I/usr/include operator.C -c -o operator.o

process.o: process.C process.h sight_common_internal.h
	${CXX} -g ${CXXFLAGS} -I/usr/include process.C -c -o process.o

sight_common.o: sight_common.C process.h sight_common_internal.h
	${CXX} -g ${CXXFLAGS} -I/usr/include sight_common.C -c -o sight_common.o

utils.o: utils.C utils.h
	${CXX} -g ${CXXFLAGS} -I/usr/include utils.C -c -o utils.o

dataTest: dataTest.C *.h schema.o data.o operator.o process.o sight_common.o utils.o
	${CXX} -g ${CXXFLAGS} -I/usr/include dataTest.C schema.o data.o operator.o process.o sight_common.o utils.o -o dataTest -lboost_thread

#MRNet integration specific targets
.PHONY: mrnop
mrnop: dataTest front backend filter.so

mrnet_operator.o: mrnet_operator.C mrnet_operator.h mrnet_flow.h
	${CXX} -g ${MRNET_CXXFLAGS} -I/usr/include mrnet_operator.C -c -o mrnet_operator.o

filter_init.o: mrnet_operator.h mrnet_flow.h filter_init.h
	${CXX} -g  ${MRNET_CXXFLAGS} -I/usr/include filter_init.C -c -o filter_init.o

front: front.C mrnet_operator.o *.h schema.o data.o operator.o process.o sight_common.o utils.o
	${CXX} -g ${MRNET_CXXFLAGS} -I/usr/include front.C mrnet_operator.o schema.o data.o operator.o process.o sight_common.o utils.o -o front ${MRNET_LIBS}

backend: backend.C mrnet_operator.o *.h schema.o data.o operator.o process.o sight_common.o utils.o
	${CXX} -g ${MRNET_CXXFLAGS} -I/usr/include backend.C mrnet_operator.o schema.o data.o operator.o process.o sight_common.o utils.o -o backend ${MRNET_LIBS}

filter.so: filter.C mrnet_operator.o filter_init.o *.h schema.o data.o operator.o process.o sight_common.o utils.o
	${CXX} -g ${MRNET_CXXFLAGS} ${MRNET_SOFLAGS} -I/usr/include filter.C mrnet_operator.o filter_init.o schema.o data.o operator.o process.o sight_common.o utils.o -o filter.so ${MRNET_LIBS}

.PHONY: testmrnet
testmrnet: ifront iback ifilter.so

ifront: IntegerAddition_FE.C IntegerAddition.h
	${CXX} -g ${MRNET_CXXFLAGS} -I/usr/include IntegerAddition_FE.C -o intfront ${MRNET_LIBS}

iback: IntegerAddition_BE.C IntegerAddition.h
	${CXX} -g ${MRNET_CXXFLAGS} -I/usr/include IntegerAddition_BE.C -o intback ${MRNET_LIBS}

ifilter.so: IntegerAdditionFilter.C IntegerAddition.h
	${CXX} -g ${MRNET_CXXFLAGS} ${MRNET_SOFLAGS} -I/usr/include IntegerAdditionFilter.C -o intfilter.so ${MRNET_LIBS}

clean:
	rm -f *.o dataTest front backend filter.so
