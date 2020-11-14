#!/bin/bash

clojure -M:dev:test -m kaocha.runner "$@"
