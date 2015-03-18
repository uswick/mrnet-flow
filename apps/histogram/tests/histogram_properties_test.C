#include "flow_test.h"

using namespace std;

bool test_initialized(){
    HistogramPtr histo = makePtr<Histogram>();
    bool isinit = histo->isInitialized() ;
    if(isinit != false){
        testFailure();
    }
    DataPtr min = makePtr<Scalar<double> >(100.0);
    DataPtr max = makePtr<Scalar<double> >(2000.0);

    histo->setMin(min);
    isinit = histo->isInitialized();
    if(isinit != false){
        testFailure();
    }

    histo->setMax(max);
    isinit = histo->isInitialized();
    if(isinit != true){
        testFailure();
    }


    return true;
}

int main(int argc, char** argv) {
    string test_suite = "histogram::properties";

    //register each inidividual test
    registerTest(test_suite + "::test_initialized", &test_initialized);

    //run Tests which has been registered above
    runTests(test_suite);

    return 0;
}
