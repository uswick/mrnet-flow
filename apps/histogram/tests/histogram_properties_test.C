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

bool test_setvalues(){
    HistogramPtr histo = makePtr<Histogram>();
    DataPtr min = makePtr<Scalar<double> >(100.0);
    DataPtr max = makePtr<Scalar<double> >(2000.0);

    histo->setMin(min);
    histo->setMax(max);

    DataPtr maxret = histo->getMax();
    DataPtr minret = histo->getMin();

    SharedPtr<Scalar<double> > maxretVal = dynamicPtrCast<Scalar<double> >(maxret);
    SharedPtr<Scalar<double> > minretVal = dynamicPtrCast<Scalar<double> >(minret);
    double maxDouble =  maxretVal.get()->get();
    double minDouble =  minretVal.get()->get();

    if(maxDouble != 2000.0){
        testFailure();
    }

    if(minDouble != 100.0){
        testFailure();
    }
    return true;
}

bool test_coloumn_data(){
    HistogramPtr histo = makePtr<Histogram>();

    DataPtr min = makePtr<Scalar<double> >(100.0);
    DataPtr max = makePtr<Scalar<double> >(500.0);

    histo->setMin(min);
    histo->setMax(max);

    double bin_width = 100.0 ;
    for(int i = 100 ; i < 500 ; i+=bin_width){
        DataPtr key = makePtr<Scalar<double> >(i);
        DataPtr end = makePtr<Scalar<double> >(i + bin_width);
        DataPtr count = makePtr<Scalar<int> >(0);
        HistogramBinPtr value = makePtr<HistogramBin>(key, end, count);
        histo->aggregateBin(key, value);
    }

    std::map<DataPtr, std::list<DataPtr> > data = histo->getData();
    std::map<DataPtr, std::list<DataPtr> >::iterator dataIt = data.begin();

    for(int i = 100; dataIt != data.end(); dataIt++, i+=bin_width){
        DataPtr key = dataIt->first;
        DataPtr value = *dataIt->second.begin();

        //check keys
        double keyValue = dynamicPtrCast<Scalar<double> >(key)->get();
        if(keyValue != i){
            testFailure();
        }
        //check values
        HistogramBinPtr colData = dynamicPtrCast<HistogramBin>(value);
        int count = dynamicPtrCast<Scalar<int> >(colData->getCount())->get();
        double start = dynamicPtrCast<Scalar<double> >(colData->getStartPos())->get();
        double end = dynamicPtrCast<Scalar<double> >(colData->getEndPos())->get();
        if(count != 0 || start != i || end != (i + bin_width)){
            testFailure();
        }


    }

    return true;
}

bool test_histogram_tostr(){
    HistogramPtr histo = makePtr<Histogram>();

    DataPtr min = makePtr<Scalar<double> >(100.0);
    DataPtr max = makePtr<Scalar<double> >(500.0);

    histo->setMin(min);
    histo->setMax(max);

    double bin_width = 100.0 ;
    for(int i = 100 ; i < 500 ; i+=bin_width){
        DataPtr key = makePtr<Scalar<double> >(i);
        DataPtr end = makePtr<Scalar<double> >(i + bin_width);
        DataPtr count = makePtr<Scalar<int> >(0);
        HistogramBinPtr value = makePtr<HistogramBin>(key, end, count);
        histo->aggregateBin(key, value);
    }

    HistogramSchemaPtr schema = makePtr<HistogramSchema>() ;
    histo->str(cout, schema);
    return true;
}


int main(int argc, char** argv) {
    string test_suite = "histogram::properties";

    //register each inidividual test
    registerTest(test_suite + "::test_initialized", &test_initialized);
    registerTest(test_suite + "::test_setvalues", &test_setvalues);
    registerTest(test_suite + "::test_coloumn_data", &test_coloumn_data);
    registerTest(test_suite + "::test_histogram_tostr", &test_histogram_tostr);

    //run Tests which has been registered above
    runTests(test_suite);

    return 0;
}
