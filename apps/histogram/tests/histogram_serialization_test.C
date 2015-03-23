#include "flow_test.h"

using namespace std;


bool test_serialization(){
    HistogramPtr histo = makePtr<Histogram>();

    DataPtr min = makePtr<Scalar<double> >(100.0);
    DataPtr max = makePtr<Scalar<double> >(500.0);

    histo->setMin(min);
    histo->setMax(max);

    double bin_width = 100.0 ;
    for(int i = 100 ; i < 500 ; i+=bin_width){
        DataPtr key = makePtr<Scalar<double> >(i);
        DataPtr end = makePtr<Scalar<double> >(i + bin_width);
        DataPtr count = makePtr<Scalar<int> >(100);
        HistogramBinPtr value = makePtr<HistogramBin>(key, end, count);
        histo->aggregateBin(key, value);
    }

    HistogramSchemaPtr schema = makePtr<HistogramSchema>() ;
    char internal[1000];
    StreamBuffer buf(internal, 1000);
    schema->serialize(histo, &buf);

    DataPtr des_histogram = schema->deserialize(&buf);
    if(des_histogram != histo){
        testFailure();
    }

    cout << "original histogram --> " << endl;
    histo->str(cout, schema);
    cout << "\n deserialized histogram --> " << endl;
    des_histogram->str(cout, schema);
    return true;
}


int main(int argc, char** argv) {
    string test_suite = "histogram::serialization";

    //register each inidividual test
    registerTest(test_suite + "::test_serialization", &test_serialization);

    //run Tests which has been registered above
    runTests(test_suite);

    return 0;
}
