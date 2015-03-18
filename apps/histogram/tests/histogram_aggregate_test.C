#include "flow_test.h"


static bool test1(){

    return true;
}

int main(int argc, char** argv) {
    string test_suite = "histogram::aggregate";

    //register each inidividual test
    registerTest(test_suite + "::test1", &test1);

    //run Tests which has been registered above
    runTests(test_suite);

    return 0;
}
