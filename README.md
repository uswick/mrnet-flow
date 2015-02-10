flow-mrnet
==========
MRNet integration of Flow engine

prerequsites
---------------------------------------
MRNet v4.1.0
Boost 1.54/1.55 (Boost thread/chrono/timer/system)


How to build/run with MRNet->
---------------------------------------
`$ make clean mrnop`


How to build/run without MRNet->
---------------------------------------
`$ make clean dataTest`



How to run->
---------------------------------------

a) build the project, modify $REPO_PATH and $BOOST_INSTALL_DIR 
   to include your MRNet and Boost installation directories
    
   --Notes-- 
   
    1. if boost/mrnet is in your System Path then REPO_PATH or
       BOOST_INSTALL_DIR shall be /usr/local
       
    2. you may want to add $BOOST_INSTALL_DIR/lib and/or MRNet
       $REPO_PATH to linkers path variable ie:- LD_LIBRARY_PATH


b) modify MRNet topology file to suite your configuration, an
   3 leaf node configuration is the default setting. Topology 
   file ${Flow_Dir}/top_file
           
c) once project is built simply execute

   `$ ./front`

d) A successfull execution shows similar output to following..

`[BE]: initialization complete PID : 22432 thread ID : 139664805431072`
`[BE]: initialization complete PID : 22433 thread ID : 140198648768288`  
`[BE]: initialization complete PID : 22431 thread ID : 139848547247904`  
`[FE]: initialization complete PID : 22429 thread ID : 139837675480864` 
`[Filter]: initialization complete PID : 22429 thread ID : 139837654247168` 
`[FE]: Recieved EXIT tag from MRNet Stream, moving into final phase..`
`[FE]: Flow data transfer is successfull... !! PID : 22429` 
`Sink data:`
`0: [ExplicitKeyValMap:` 
`    [Tuple: tFields=`
`    0: [Scalar: val={0}, type=int]`
`    1: [Scalar: val={0}, type=int]`
`]: `
`        [Tuple: tFields=`
`    0: [Record: rFields=`
`    name: [Scalar: val={subStream 0, iter 0, 0}, type=string]`
`    valSq: [Scalar: val={0}, type=double]`
`]`

....
...
..





