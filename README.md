flow-mrnet
==========
MRNet integration of Flow engine

prerequsites
---------------------------------------
MRNet v4.1.0
Boost 1.54/1.55 (Boost thread/chrono/timer/system)


How to build/run with MRNet->
---------------------------------------
$ make clean mrnop


How to build/run without MRNet->
$ make clean dataTest

-----------------------------------------

How to run ->

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
           
c) once buuild to run simply execute
   $ ./front






