#!/bin/bash
export PYTHONPATH=../../ramcloud/bindings/python
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:../../ramcloud/obj.master
../ramcloud/obj.master/coordinator > coordinator.out 2>&1 &
../ramcloud/obj.master/server -M -r 0 > server.out 2>&1 &
