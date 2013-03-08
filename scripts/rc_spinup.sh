#!/bin/bash
~/ramcloud/obj.master/coordinator -C "infrc:host=192.168.1.101,port=12246" > coordinator.out 2>&1 &
~/ramcloud/obj.master/server -L "infrc:host=192.168.1.101,port=12242" -C "infrc:host=192.168.1.101,port=12246" -M -r 0 > server.out 2>&1 &
