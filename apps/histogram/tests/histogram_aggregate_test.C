#include "flow_test.h"


bool test_aggregate1(){
    HistogramPtr histo = makePtr<Histogram>();
    HistogramPtr histo2 = makePtr<Histogram>();

    DataPtr min = makePtr<Scalar<double> >(100.0);
    DataPtr max = makePtr<Scalar<double> >(500.0);

    histo->setMin(min);
    histo2->setMin(min);
    histo->setMax(max);
    histo2->setMax(max);

    double bin_width = 100.0 ;
    for(int i = 100 ; i < 500 ; i+=bin_width){
        DataPtr key = makePtr<Scalar<double> >(i);
        DataPtr end = makePtr<Scalar<double> >(i + bin_width);
        //seperate counts for 2 histograms
        DataPtr count = makePtr<Scalar<int> >(2);
        DataPtr count2 = makePtr<Scalar<int> >(1);

        HistogramBinPtr value = makePtr<HistogramBin>(key, end, count);
        HistogramBinPtr value2 = makePtr<HistogramBin>(key, end, count2);
        histo->aggregateBin(key, value);
        histo2->aggregateBin(key, value2);
    }

    HistogramSchemaPtr schema = makePtr<HistogramSchema>() ;
    histo->join(histo2);
    histo->str(cout, schema);

    //validate counts
    std::map<DataPtr, std::list<DataPtr> > data = histo->getData();
    std::map<DataPtr, std::list<DataPtr> >::iterator dataIt = data.begin();

    for(; dataIt != data.end(); dataIt++){
        DataPtr value = *dataIt->second.begin();

        //check values
        HistogramBinPtr colData = dynamicPtrCast<HistogramBin>(value);
        int count = dynamicPtrCast<Scalar<int> >(colData->getCount())->get();

        if(count != 3){
            testFailure();
        }
    }


    return true;
}

bool test_aggregate2(){
    HistogramPtr histo = makePtr<Histogram>();
    HistogramPtr histo2 = makePtr<Histogram>();

    DataPtr min = makePtr<Scalar<double> >(100.0);
    DataPtr max = makePtr<Scalar<double> >(500.0);
    DataPtr min2 = makePtr<Scalar<double> >(200.0);
    DataPtr max2 = makePtr<Scalar<double> >(400.0);

    histo->setMin(min);
    histo->setMax(max);

    //different range but same bin width
    histo2->setMin(min2);
    histo2->setMax(max2);

    double bin_width = 100.0 ;
    for(int i = 100 ; i < 500 ; i+=bin_width){
        DataPtr key = makePtr<Scalar<double> >(i);
        DataPtr end = makePtr<Scalar<double> >(i + bin_width);
        DataPtr count = makePtr<Scalar<int> >(2);
        HistogramBinPtr value = makePtr<HistogramBin>(key, end, count);
        histo->aggregateBin(key, value);
    }

    for(int i = 200 ; i < 400 ; i+=bin_width){
        DataPtr key = makePtr<Scalar<double> >(i);
        DataPtr end = makePtr<Scalar<double> >(i + bin_width);
        //seperate counts for 2 histograms
        DataPtr count = makePtr<Scalar<int> >(1);
        HistogramBinPtr value = makePtr<HistogramBin>(key, end, count);
        histo2->aggregateBin(key, value);
    }

    HistogramSchemaPtr schema = makePtr<HistogramSchema>() ;
    histo->join(histo2);
    histo->str(cout, schema);

    //validate counts
    std::map<DataPtr, std::list<DataPtr> > data = histo->getData();
    std::map<DataPtr, std::list<DataPtr> >::iterator dataIt = data.begin();

    for(; dataIt != data.end(); dataIt++){
        DataPtr value = *dataIt->second.begin();

        //check values
        HistogramBinPtr colData = dynamicPtrCast<HistogramBin>(value);
        int count = dynamicPtrCast<Scalar<int> >(colData->getCount())->get();

        if (dataIt == data.begin() && count != 2){
            //first coloumn count should be 2
            testFailure();
        } else if ( dataIt != data.end()  && count != 3){
            //everything else except last coloumn count should be 3
            testFailure();
        }
    }


    return true;
}


int main(int argc, char** argv) {
    string test_suite = "histogram::aggregate";

    //register each inidividual test
    registerTest(test_suite + "::test_aggregate1", &test_aggregate1);
    registerTest(test_suite + "::test_aggregate2", &test_aggregate2);

    //run Tests which has been registered above
    runTests(test_suite);

    return 0;
}
