#include "flow_test.h"

map<string, test_func> testRegistry ;
string current_test ;

void testSuccess(string label){
    cout << "[Test : " << label << " success !! ]"  << endl;
}

void testFailure(string label){
    cerr << "[Test : " << label << " failed !! ]"  << endl;
    assert(0);
}

void testSuccess(){
    testSuccess(current_test);
}

void testFailure(){
    testFailure(current_test);
}

void registerTest(string test_name, test_func t){
    if(t && test_name != ""){
        //insert to map
        testRegistry[test_name] = t;
    }
}

void runTests(string lbl){
    cout << "[Start of Test Suite : " << lbl << " number of tests : " << testRegistry.size()  << " ]" << endl;
    map<string, test_func>::iterator testIt = testRegistry.begin();
    for(; testIt != testRegistry.end(); testIt++){
        current_test = testIt->first;
        //run a test
        (testIt->second)();
        testSuccess();
    }
    cout << "[End of Test Suite : " << lbl << "  success !! ]" << endl;

}
