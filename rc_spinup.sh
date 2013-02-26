#!/bin/bash
../ramcloud/obj.master/coordinator > coordinator.out 2>&1 &
../ramcloud/obj.master/server -M -r 0 > server.out 2>&1 &
