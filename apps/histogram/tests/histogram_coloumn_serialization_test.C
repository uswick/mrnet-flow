#include "flow_test.h"

using namespace std;


bool test_serialization(){
    DataPtr key = makePtr<Scalar<double> >(150);
    DataPtr end = makePtr<Scalar<double> >(250);
    DataPtr count = makePtr<Scalar<int> >(100);
    HistogramBinPtr histo_coloumn = makePtr<HistogramBin>(key, end, count);


    HistogramBinSchemaPtr schema = makePtr<HistogramBinSchema>() ;
    char internal[1000];
    StreamBuffer buf(internal, 1000);
    schema->serialize(histo_coloumn, &buf);

    DataPtr des_histogram_col = schema->deserialize(&buf);
    if(des_histogram_col != histo_coloumn){
        testFailure();
    }

    cout << "original histogram col --> " << endl;
    histo_coloumn->str(cout, schema);
    cout << "\n deserialized histogram --> " << endl;
    des_histogram_col->str(cout, schema);
    return true;
}

bool test_serialization_with_update(){
    DataPtr key = makePtr<Scalar<double> >(150);
    DataPtr end = makePtr<Scalar<double> >(250);
    DataPtr count = makePtr<Scalar<int> >(100);
    HistogramBinPtr histo_coloumn = makePtr<HistogramBin>(key, end, count);


    HistogramBinSchemaPtr schema = makePtr<HistogramBinSchema>() ;
    char internal[1000];
    StreamBuffer buf(internal, 1000);
    schema->serialize(histo_coloumn, &buf);

    DataPtr des_histogram_col = schema->deserialize(&buf);
    if(des_histogram_col != histo_coloumn){
        testFailure();
    }

    histo_coloumn->update(300);
    DataPtr count_new = makePtr<Scalar<int> >(100+300);
    HistogramBinPtr new_histo_coloumn = makePtr<HistogramBin>(key, end, count_new);
    StreamBuffer buf2(internal, 1000);
    schema->serialize(histo_coloumn, &buf2);
    DataPtr des_histogram_col_updated = schema->deserialize(&buf2);

    if(des_histogram_col_updated != new_histo_coloumn){
        testFailure();
    }

//    cout << "original histogram col --> " << endl;
//    histo_coloumn->str(cout, schema);
//    cout << "\n deserialized histogram --> " << endl;
//    des_histogram_col->str(cout, schema);
    return true;
}



int main(int argc, char** argv) {
    string test_suite = "histogram::col_serialization";

    //register each inidividual test
    registerTest(test_suite + "::test_serialization", &test_serialization);
    registerTest(test_suite + "::test_serialization_with_update", &test_serialization_with_update);

    //run Tests which has been registered above
    runTests(test_suite);

    return 0;
}
