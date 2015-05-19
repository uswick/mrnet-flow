#include "flow_test.h"

map<string, test_func> testRegistry ;
string current_test ;

void testSuccess(string label){
    cout << "[Test : " << label << " --> success !! ]"  << endl;
}

void testFailure(string label){
    cerr << "[Test : " << label << " --> failed !! ]"  << endl;
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


void printTestSummary(int passed, int failed){
    cout << endl;
    cout << endl;
    cout << "[******************************** Test Summary ********************************]" << endl;
    cout << "[Total number of tests : " << passed + failed  << " ]" << endl;
    cout << "[Total passed : " << passed  << " ]" << endl;
    cout << "[Total failed : " << failed  << " ]" << endl;
    cout << "[******************************** End  Summary ********************************]" << endl;
    cout << endl;
}

void runIntegrationTests(string label){

}


void runTests(string lbl){
    cout << "[Start of Test Suite : { " << lbl << " } {number of tests : " << testRegistry.size()  << "} ]" << endl;
    map<string, test_func>::iterator testIt = testRegistry.begin();
    int failed_tests = 0 ;
    for(; testIt != testRegistry.end(); testIt++){
        current_test = testIt->first;
        //run a test
        bool ret = (testIt->second)();
        if(ret){
            testSuccess();
        } else{
            failed_tests++;
        }
    }

    printTestSummary(testRegistry.size() - failed_tests, failed_tests);
    cout << "[End of Test Suite : { " << lbl << " }  success !! ]" << endl << endl ;

}
