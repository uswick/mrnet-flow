#include "data.h"
#include "schema.h"
#include "operator.h"
#include "mrnet_flow.h"
#include <iostream>
#include <stdio.h>

using namespace std;
typedef bool (*test_func)();


void testSuccess(string label);
void testSuccess();
void testFailure(string label);
void testFailure();

void registerTest(string test_name, test_func t);
void runTests(string label = "Default Flow Tests");
void runIntegrationTests(string label = "Default MRNet Flow Integration Tests");

// Operator that writes received Data objects to a given FILE* using the Schema of its single input stream
template <class keyType, class valType>
class TestOutOperator : public AsynchOperator {
private:
    typedef bool (*callback_func)(int inStreamIdx, DataPtr data, map<keyType, valType> validator);
    callback_func callback;
    map<keyType, valType>* validatr;

public:

    TestOutOperator(unsigned int numInputs, unsigned int numOutputs, unsigned int ID, callback_func cbk, map<keyType, valType>& validator):
            AsynchOperator(numInputs , numOutputs, ID){
        callback = cbk ;
        validatr = &validator;
//        this->validat = validator;
    }

    TestOutOperator(unsigned int numInputs, unsigned int numOutputs, unsigned int ID):
            AsynchOperator(numInputs , numOutputs, ID){
//        callback = cbk ;
//        validatr = &validator;
//        this->validat = validator;
    }

    ~TestOutOperator(){}

    // Called to signal that all the incoming streams have been connected. Returns the schemas
    // of the outgoing streams based on the schemas of the incoming streams.
    std::vector<SchemaPtr> inConnectionsComplete(){
        vector<SchemaPtr> schemas;
        return schemas;
    }

    // Called when a tuple arrives on single incoming stream.
    // inStreamIdx: the index of the stream on which the object arrived
    // inData: holds the single Data object from the single stream
    // This function may send Data objects on some of the outgoing streams.
    void work(unsigned int inStreamIdx, DataPtr inData){
        assert(inStreamIdx >= 0);
        (*callback)(inStreamIdx, inData, *validatr);
    }

};




