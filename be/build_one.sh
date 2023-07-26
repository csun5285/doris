#!/usr/bin/env bash

target=$(grep $1.o: build_Release/build.ninja | awk -F'[ :]' '{print $2}')
ninja -C build_Release "${target}"
