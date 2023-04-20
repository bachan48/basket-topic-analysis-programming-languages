#!/usr/bin/env bash
sbt --error 'set showSuccess := false' 'runMain stackoverflow.Baskets data/baskets.txt'

