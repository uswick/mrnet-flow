#include "flow_test.h"

using namespace std;

bool test_initialized(){
    HistogramBinPtr histobin = makePtr<HistogramBin>();
    bool isinit = histobin->isInitialized() ;
    if(isinit != false){
        testFailure();
    }
    DataPtr start = makePtr<Scalar<double> >(100.0);
    DataPtr end = makePtr<Scalar<double> >(200.0);
    DataPtr count = makePtr<Scalar<int> >(0);

    HistogramBinPtr histobin2 = makePtr<HistogramBin>(start, end, count);
    isinit = histobin2->isInitialized() ;
    if(isinit != true){
        testFailure();
    }

    HistogramBinPtr histobin3 = makePtr<HistogramBin>();
    HistogramBinSchemaPtr schema = makePtr<HistogramBinSchema>();

    histobin3->add(schema->field_start, start, schema);
    histobin3->add(schema->field_end, end, schema);
    isinit = histobin3->isInitialized() ;
    if(isinit != false){
        testFailure();
    }
    histobin3->add(schema->field_count, count, schema);
    isinit = histobin3->isInitialized() ;
    if(isinit != true){
        testFailure();
    }


    return true;
}

bool test_update(){
    DataPtr start = makePtr<Scalar<double> >(100.0);
    DataPtr end = makePtr<Scalar<double> >(200.0);
    DataPtr count = makePtr<Scalar<int> >(2);

    HistogramBinPtr histobin = makePtr<HistogramBin>();
    HistogramBinSchemaPtr schema = makePtr<HistogramBinSchema>();
    histobin->add(schema->field_start, start, schema);
    histobin->add(schema->field_end, end, schema);
    histobin->add(schema->field_count, count, schema);

    int cnt = dynamicPtrCast<Scalar<int> >(histobin->getCount())->get();
//    cout << "count is : " << cnt << endl;
    if(cnt != 2){
        testFailure();
    }
    //increment count
    histobin->update(3);
    cnt = dynamicPtrCast<Scalar<int> >(histobin->getCount())->get();
    if(cnt != 5){
        testFailure();
    }

    return true;
}

//merging gets successful if a similar range column is found
bool test_merge_success(){
    DataPtr start = makePtr<Scalar<double> >(100.0);
    DataPtr end = makePtr<Scalar<double> >(200.0);
    DataPtr count = makePtr<Scalar<int> >(2);
    DataPtr count2 = makePtr<Scalar<int> >(3);

    HistogramBinPtr histobin = makePtr<HistogramBin>(start, end, count);
    HistogramBinPtr histobin2 = makePtr<HistogramBin>(start, end, count2);

    //do successful merge
    histobin->merge(histobin2);

    int cnt = dynamicPtrCast<Scalar<int> >(histobin->getCount())->get();
//    cout << "count is : " << cnt << endl;
    if(cnt != 5){
        testFailure();
    }

    return true;
}

//merging gets successful only if a similar range column is found
//unless counts are not updated
bool test_merge_fail(){
    DataPtr start = makePtr<Scalar<double> >(100.0);
    DataPtr end = makePtr<Scalar<double> >(200.0);
    DataPtr count = makePtr<Scalar<int> >(2);

    DataPtr start2 = makePtr<Scalar<double> >(300.0);
    DataPtr end2 = makePtr<Scalar<double> >(400.0);
    DataPtr count2 = makePtr<Scalar<int> >(3);

    HistogramBinPtr histobin = makePtr<HistogramBin>(start, end, count);
    HistogramBinPtr histobin2 = makePtr<HistogramBin>(start2, end2, count2);

    //do successful merge
    histobin->merge(histobin2);

    int cnt = dynamicPtrCast<Scalar<int> >(histobin->getCount())->get();
//    cout << "count is : " << cnt << endl;
    if(cnt != 5){
        testFailure();
    }

    return true;
}

bool test_histogram_col_tostr(){
    DataPtr start = makePtr<Scalar<double> >(100.0);
    DataPtr end = makePtr<Scalar<double> >(200.0);
    DataPtr count = makePtr<Scalar<int> >(0);

    HistogramBinPtr histobin = makePtr<HistogramBin>();
    HistogramBinSchemaPtr schema = makePtr<HistogramBinSchema>();
    histobin->add(schema->field_start, start, schema);
    histobin->add(schema->field_end, end, schema);
    histobin->add(schema->field_count, count, schema);

    histobin->str(cout, schema);
    return true;
}


int main(int argc, char** argv) {
    string test_suite = "histogram::coloumn_properties";

    //register each inidividual test
    registerTest(test_suite + "::test_initialized", &test_initialized);
    registerTest(test_suite + "::test_update", &test_update);
    registerTest(test_suite + "::test_histogram_col_tostr", &test_histogram_col_tostr);
    registerTest(test_suite + "::test_merge_success", &test_merge_success);
//    registerTest(test_suite + "::test_merge_fail", &test_merge_fail);

    //run Tests which has been registered above
    runTests(test_suite);

    return 0;
}
