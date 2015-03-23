#include "flow_test.h"
#include "math.h"

using namespace std;


SchemaPtr getInputSchemaFilterNode(int numFields) {
// The value will be a record
    RecordSchemaPtr recSchema = makePtr<RecordSchema>();
    for(int i = 0 ; i < numFields ; i++) {
        SharedPtr<ScalarSchema> recScalarSchema = makePtr<ScalarSchema>(ScalarSchema::doubleT);
        recSchema->add(txt() << "Rec_" << i,  recScalarSchema);
    }
    recSchema->finalize();
//    cout << " entries per rec : " << dynamicPtrCast<Record>(*inData.begin())->getFields().size() << endl ;
//    cout << "schema entries per rec : "  << endl ;
//    int a = recSchema->rFields.size();
    return recSchema;
}

//track number of records

bool test_record_join_callback(int inStreamIdx, DataPtr inData, map<double , int> rec_counts) {
    //once when records are joined this function works
    //check return joined Histogram inData.size == rec_Counts.size
    // individual rec counts for range
    HistogramPtr hist = dynamicPtrCast<Histogram>(inData);

    map<DataPtr, std::list<DataPtr> >::const_iterator bin_It = hist->getData().begin();
    cout << "hist join OP !!! hist data Size : " << hist->getData().size() << endl ;
    for ( ; bin_It != hist->getData().end() ; bin_It++) {
        double key = dynamicPtrCast<Scalar<double> >(bin_It->first)->get();
        DataPtr value = *bin_It->second.begin();
        int count = dynamicPtrCast<Scalar<int> >(dynamicPtrCast<HistogramBin>(value)->getCount())->get();
        if(count != rec_counts[key]){
            testFailure();
        }
        cout << " OP !!! hist[ " << key << " ] = " << count << endl ;
    }

//    map<double , int>::iterator recc_It = rec_counts.begin();
//    cout << "join OP !!! rec Size : " << rec_counts.size() << endl ;
//    for ( ; recc_It != rec_counts.end() ; recc_It++) {
//        cout << " OP !!! rec[ " << recc_It->first << " ] = " << recc_It->second << endl ;
//    }

    return true;
}

bool test_record_join(){
    const double MIN = 100.0 ;
    const double MAX = 500.0 ;
    const double WIDTH = 100.0 ;
    int num_recs = 100 ;
    map<double , int> rec_counts;

    //init join operator
    SynchedRecordJoinOperator* op = new SynchedRecordJoinOperator(3, 0, MIN, MAX, WIDTH);
    OperatorPtr opPtr(op);
    SharedPtr<SynchedRecordJoinOperator> joinOpPtr = dynamicPtrCast<SynchedRecordJoinOperator>(opPtr);

    RecordSchemaPtr schema = dynamicPtrCast<RecordSchema>(getInputSchemaFilterNode(10));
    joinOpPtr->setInSchema(schema);
    joinOpPtr->setOutSchema(makePtr<HistogramSchema>());

    TestOutOperator<double ,int>* outputOp = new TestOutOperator<double ,int>(1, 0, 1, &test_record_join_callback, rec_counts);
//    OperatorPtr outputOpPtr = makePtr<TestOutOperator<double ,int> >(1, 0, 1, &test_record_join_callback, rec_counts);
//    OperatorPtr outputOpPtr = makePtr<TestOutOperator<double ,int> >(1, 0, 1);

    StreamPtr out_stream = makePtr<Stream>(schema);
    //connect operators with streams
    joinOpPtr->outConnect(0, out_stream);
    //bind operator with stream
    OperatorPtr outputOpPtr(outputOp);
    outputOpPtr->inConnect(0, out_stream);
//    out_stream->connectToOperatorInput(outputOpPtr, 0);

    //create records (each record has 10 data fields)
    //lets have  vector of 100 records(ideally 100 incoming streams) with each 10 data fields
    vector<DataPtr> inData;
    inData.reserve(num_recs);

    //rand seed
    srand(time(NULL));
    for (int i = 0; i < num_recs; i++) {
        RecordPtr rec = makePtr<Record>(dynamicPtrCast<RecordSchema const>(schema));

        //for each rec add 10 entries
        for (int j = 0; j < 10; j++) {
            double rand_num = MIN + static_cast <double> (rand()) /( static_cast <double> (RAND_MAX/(MAX - MIN)));
            //add record to data ptr
            SharedPtr<Scalar<double> > rand_num_data = makePtr<Scalar<double> >(rand_num);
            string strkey = txt() << "Rec_" << j ;
            rec->add(strkey, rand_num_data, dynamicPtrCast<RecordSchema const>(schema));

            //keep track of number of records in range if MIN-MAX with WIDTH size bins
            double tracking_key = MIN + WIDTH*floor((rand_num - MIN)/WIDTH);
            if(rec_counts.find(tracking_key) == rec_counts.end()){
                rec_counts[tracking_key] = 1 ;
            }else{
                rec_counts[tracking_key] += 1 ;
            }
        }
//        cout << "test rec fields : " << rec->getFields().size() << endl ;
        inData.push_back(rec);
    }
//        cout << "entries per rec : " << dynamicPtrCast<Record>(*inData.begin())->getFields().size() << endl ;

    joinOpPtr->work(dynamic_cast<vector<DataPtr> const &>(inData));

    return true;
}


int main(int argc, char** argv) {
    string test_suite = "operators::recordjoin";

    //register each inidividual test
    registerTest(test_suite + "::test_record_join", &test_record_join);


    //run Tests which has been registered above
    runTests(test_suite);

    return 0;
}
