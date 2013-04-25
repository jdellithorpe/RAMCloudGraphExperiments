#!/bin/bash
~/ramcloud/obj.master/coordinator -C "infrc:host=192.168.1.101,port=12247" > coordinator.out 2>&1 &
~/ramcloud/obj.master/server -L "infrc:host=192.168.1.101,port=12242" -C "infrc:host=192.168.1.101,port=12247" -M -r 0 -t 50% --masterServiceThreads 4 > server.out 2>&1 &
