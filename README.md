# ExaQUte-API

This repository contains the ExaQUte API implementation for local and distributed environments.

## Installation instructions
To install the ExaQUte API clone the repository

'''
$ git clone https://github.com/ExaQUte-project/exaqute-api.git
'''

and add the folder in the PYTHONPATH environment variable
'''
$ cd exaque-api/
$ export PYTHONPATH=$PWD:$PYTHONPATH

If you want to run with some of the supported backend, they must be installed in the distributed computing infrastructure. 

## Execution
Include the API calls in your python script and set-up the environment variable EXAQUTE_BACKEND to define the used backend. By default, It is set to the local backend. This backend execute task sequentially in the localhost. The propose of this backend is to debug, check the syntax and application correctnes before submitting to a distributed infrastructure. 

The execution of the application with the local backend is executed as a normal python application (python commnand). If you want to run with a distributed computing backend, it must be executed with the corresponding backend executor (e.g runcompss,...)   

## Acknowledgement
This work has been supported by the European Union's Horizon 2020 research and innovation program under grant agreement 800898 ([ExaQUte](http://exaqute.eu/) project).
